/*
 * Copyright 2026 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package keycloak reads the group tree of a realm from the Keycloak admin API.
//
// This is the only part of the service that holds real credentials: the
// internal admin token the other clients authenticate with is not accepted by
// Keycloak, so this package runs a client credentials grant of its own and
// caches the access token. Neither the client secret nor the token ever reaches
// an error, a log line or a formatted struct - see credential.
//
// The configured URL is used verbatim. Deployments that serve Keycloak under
// /auth carry that prefix in the configured URL; this package neither adds nor
// removes one. Only a trailing slash is trimmed, because it would otherwise
// turn every path into a double slash.
//
// Group changes are polled rather than evented (SPEC.md: the realm has no admin
// event listener), so Groups runs on an interval: it has to be cheap to repeat,
// safe to call from more than one goroutine, and it must not hang on a
// malformed answer.
package keycloak

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/SENERGY-Platform/graph-provider/pkg/config"
	"github.com/SENERGY-Platform/graph-provider/pkg/model"
)

const (
	// maxDepth bounds the descent through the group tree. A cycle is already
	// caught by the id set below, so this only fires on a tree that is
	// pathologically deep or on a server inventing a new group per level.
	// Erroring out is deliberate: silently truncating would hand the caller a
	// group tree that looks complete and is not.
	maxDepth = 32

	// maxPages bounds one paginated listing. Without it a server that keeps
	// answering full pages would keep the poll running forever.
	maxPages = 10000

	// defaultPageSize is used when the configuration carries no usable page
	// size. max=0 would make Keycloak answer an empty page, which reads exactly
	// like a realm without groups.
	defaultPageSize = 100
)

// Client talks to the admin API of one realm.
//
// Safe for concurrent use: the http client is, and the cached token is guarded.
type Client struct {
	baseUrl  string
	realm    string
	clientId string
	secret   credential
	pageSize int

	http *http.Client

	// now is the clock the token expiry is judged against, injectable for the
	// tests.
	now func() time.Time

	// tokenMux guards the cached token and the grant in flight. It is never
	// held across a request: a waiting caller has to be able to give up on its
	// own context, and a mutex cannot be waited on with one.
	tokenMux    sync.Mutex
	token       credential
	tokenExpiry time.Time
	grant       *tokenGrant
}

// New builds a client from the configuration.
//
// timeout is the per-request timeout of the underlying http client; ctx still
// governs every call on top of it.
func New(cfg config.KeycloakConfig, timeout time.Duration) *Client {
	pageSize := cfg.PageSize
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	return &Client{
		baseUrl:  strings.TrimRight(cfg.Url, "/"),
		realm:    cfg.Realm,
		clientId: cfg.ClientId,
		secret:   hold(cfg.ClientSecret.Value()),
		pageSize: pageSize,
		http: &http.Client{
			Timeout: timeout,
			// Redirects are not followed, and this is the credential defence
			// rather than a preference. A 307 or 308 replays the request body,
			// and the body of the token request is the client secret - the one
			// route by which it would leave this process. The bearer token
			// leaks less loudly: net/http copies the Authorization header onto
			// a redirect whose hostname matches, and it compares hostnames
			// only, so an https to http downgrade keeps it. The admin API has
			// no legitimate redirect, so a redirect stays the status code it is
			// and getJson reports it like any other non-2xx.
			CheckRedirect: func(request *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		now: time.Now,
	}
}

// groupRepresentation is the part of Keycloak's GroupRepresentation this
// service reads.
//
// SubGroups is only populated by older versions, which shipped the whole tree
// in one answer. Newer ones load children lazily and report SubGroupCount
// instead. SubGroupCount is a pointer because absent and zero mean different
// things: absent says nothing about the children, zero says there are none.
type groupRepresentation struct {
	Id            string                `json:"id"`
	Path          string                `json:"path"`
	SubGroups     []groupRepresentation `json:"subGroups"`
	SubGroupCount *int                  `json:"subGroupCount"`
}

// Groups returns every group of the realm, at every depth, flattened and
// sorted by path.
//
// Path is Keycloak's own path, not one this package assembles: it is the key
// permissions-v2 stores group rights under and the value the groups claim
// carries, so it is the identity. Name is derived from it via model.GroupName
// rather than taken from Keycloak's name field, so the naming rule lives in one
// place.
//
// A group that repeats within the tree is emitted once. That is both the cycle
// guard and the answer to a server that lists a group twice.
func (this *Client) Groups(ctx context.Context) ([]model.Group, error) {
	roots, err := this.listGroups(ctx, this.adminPath("groups"))
	if err != nil {
		return nil, err
	}
	// An empty listing is the one answer that must not be taken at face value.
	// Keycloak filters the group listing by what the caller may view, and a
	// service account holding only query-groups may view nothing - so it gets
	// an empty array and HTTP 200, indistinguishable from a realm with no
	// groups. The count endpoint is not filtered the same way, so the two
	// disagreeing is a reliable signature of a missing role.
	//
	// This has to be an error rather than an empty result: everything
	// downstream replaces its view of the world with what this returns, and a
	// false "no groups" would look like every company having disappeared.
	if len(roots) == 0 {
		count, err := this.groupCount(ctx)
		if err == nil && count > 0 {
			return nil, fmt.Errorf("keycloak reports %v group(s) in realm %v but"+
				" showed none: the service account may query groups but not view"+
				" them. Grant view-users on the client that administers this realm"+
				" (\"%v-realm\") - view-groups is a role of the account client and"+
				" is not what governs the admin API",
				count, this.realm, this.realm)
		}
	}
	result, err := this.collect(ctx, roots, 0, map[string]bool{}, []model.Group{})
	if err != nil {
		return nil, err
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Path < result[j].Path
	})
	return result, nil
}

// collect appends one level of groups and descends into their children.
//
// A group without an id or without a path is skipped: the id is the only key
// the children endpoint and the cycle guard have, and the path is the identity
// the rest of the service works with. Neither can be invented, and failing the
// whole poll over one unusable entry would cost every other group too.
func (this *Client) collect(ctx context.Context, groups []groupRepresentation, depth int, seen map[string]bool, into []model.Group) ([]model.Group, error) {
	if len(groups) == 0 {
		return into, nil
	}
	if depth >= maxDepth {
		return nil, fmt.Errorf("keycloak group tree exceeds the depth limit of %v", maxDepth)
	}
	for _, group := range groups {
		if group.Id == "" || group.Path == "" {
			continue
		}
		if seen[group.Id] {
			continue
		}
		seen[group.Id] = true
		into = append(into, model.Group{
			Id:   group.Id,
			Path: group.Path,
			Name: model.GroupName(group.Path),
		})

		children := group.SubGroups
		if len(children) == 0 && (group.SubGroupCount == nil || *group.SubGroupCount > 0) {
			var err error
			children, err = this.children(ctx, group.Id)
			if err != nil {
				return nil, err
			}
		}
		var err error
		into, err = this.collect(ctx, children, depth+1, seen, into)
		if err != nil {
			return nil, err
		}
	}
	return into, nil
}

// children fetches the subgroups of one group.
//
// A 404 is answered with no children rather than an error. Two deployments end
// up here: one whose Keycloak predates the children endpoint - it delivered the
// whole tree inline, so an empty subGroups really is a leaf - and one where the
// group was deleted between the listing and this call, which the next poll
// picks up anyway. Every other status stays an error.
func (this *Client) children(ctx context.Context, id string) ([]groupRepresentation, error) {
	path := this.adminPath("groups/" + url.PathEscape(id) + "/children")
	children, err := this.listGroups(ctx, path)
	var status statusError
	if errors.As(err, &status) && status.Status == http.StatusNotFound {
		return nil, nil
	}
	return children, err
}

// listGroups reads a paginated group listing whole.
//
// Keycloak's group endpoints default to a small page, so max and first are
// always sent. Only an empty page ends the listing. A short page does not:
// max bounds the window the server reads, not the number of groups it returns,
// so a listing that is filtered server side - by the client's own view of the
// realm, by a search - answers fewer than max with more behind it.
//
// That costs one extra request per listing, and it is worth it. Stopping on a
// short page hands back a tree that looks complete: cache.RefreshGroups
// replaces the tree wholesale, so every group behind the short page counts as
// removed, reconcile.Full unshares its graph and clears the group attribute
// that identifies it - and the next successful poll then creates a second graph
// while the edited one is invisible to the company. maxPages bounds the loop,
// so the extra request is the only price. Do not optimise it away.
func (this *Client) listGroups(ctx context.Context, path string) ([]groupRepresentation, error) {
	result := []groupRepresentation{}
	for pageNumber := 0; ; pageNumber++ {
		if pageNumber >= maxPages {
			return nil, fmt.Errorf("keycloak %v did not stop paginating after %v pages", path, maxPages)
		}
		query := url.Values{}
		query.Set("max", strconv.Itoa(this.pageSize))
		query.Set("first", strconv.Itoa(pageNumber*this.pageSize))

		page := []groupRepresentation{}
		err := this.getJson(ctx, path, query, &page)
		if err != nil {
			return nil, err
		}
		result = append(result, page...)
		if len(page) == 0 {
			return result, nil
		}
	}
}

// groupCount is how many groups Keycloak says the realm has.
//
// Only used to interpret an empty listing. Keycloak answers this from the same
// permission as the listing but without filtering the result, which is what
// makes the two comparable at all.
func (this *Client) groupCount(ctx context.Context) (int64, error) {
	var answer struct {
		Count int64 `json:"count"`
	}
	if err := this.getJson(ctx, this.adminPath("groups/count"), nil, &answer); err != nil {
		return 0, err
	}
	return answer.Count, nil
}

// adminPath is the admin API path of the configured realm.
func (this *Client) adminPath(suffix string) string {
	return "/admin/realms/" + url.PathEscape(this.realm) + "/" + suffix
}

// getJson performs one authenticated GET and decodes the answer.
func (this *Client) getJson(ctx context.Context, path string, query url.Values, target any) error {
	token, err := this.accessToken(ctx)
	if err != nil {
		return err
	}
	endpoint := this.baseUrl + path
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return fmt.Errorf("unable to build keycloak request for %v", path)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Accept", "application/json")

	response, err := this.http.Do(request)
	if err != nil {
		return transportError(path, err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return statusError{Status: response.StatusCode, Path: path}
	}
	err = json.NewDecoder(response.Body).Decode(target)
	if err != nil {
		return fmt.Errorf("unable to decode keycloak answer of %v: %w", path, err)
	}
	return nil
}

// statusError is a non-2xx answer.
//
// It carries the status and the path and never the body: an error body from the
// token endpoint can echo the request, and this error is meant to be logged.
type statusError struct {
	Status int
	Path   string
}

func (this statusError) Error() string {
	// A 403 on an admin path is the one status worth explaining. It means the
	// grant worked and the service account simply lacks the role, and the place
	// to assign it is not where the name suggests: a realm is administered by a
	// client, and for the realm "master" that client is "master-realm", not
	// "realm-management". Saying so here saves the next person the hour it cost
	// to work out from a bare 403.
	if this.Status == http.StatusForbidden && strings.Contains(this.Path, "/admin/realms/") {
		return fmt.Sprintf("keycloak answered %v for %v"+
			" - the token is valid but the service account lacks the role;"+
			" assign view-groups and query-groups from the client that administers"+
			" this realm (\"<realm>-realm\", e.g. \"master-realm\" for the master realm),"+
			" not from realm-management or account",
			this.Status, this.Path)
	}
	return fmt.Sprintf("keycloak answered %v for %v", this.Status, this.Path)
}

// transportError reports a request that never got an answer.
//
// The url.Error wrapper is unwrapped on purpose: it repeats the full request
// URL, and the configured URL is the one part of this client's input that could
// carry credentials. The cause is kept, so errors.Is against context.Canceled
// still works.
func transportError(path string, err error) error {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		err = urlErr.Err
	}
	return fmt.Errorf("keycloak request %v failed: %w", path, err)
}
