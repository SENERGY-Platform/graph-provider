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

package keycloak

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	sb_config_types "github.com/SENERGY-Platform/go-service-base/config-hdl/types"
	"github.com/SENERGY-Platform/graph-provider/pkg/config"
	"github.com/SENERGY-Platform/graph-provider/pkg/model"
)

const (
	testRealm    = "test-realm"
	testClientId = "graph-provider"

	// testSecret is asserted against as a whole string: no error this package
	// returns may contain it.
	testSecret = "very-secret-client-secret-do-not-log"
)

// recordedRequest is one request the fake answered, minus its body.
type recordedRequest struct {
	Method string
	Path   string
	Query  url.Values
	Auth   string
}

// fakeKeycloak is a Keycloak that only knows the three endpoints this client
// uses. Anything else is a test failure: a client that invents a path - an
// /auth prefix, say - has to be caught rather than silently 404ed.
type fakeKeycloak struct {
	server *httptest.Server

	mutex     sync.Mutex
	tokens    []recordedRequest
	forms     []url.Values
	groups    []recordedRequest
	expiresIn int64
	tokenBody func(w http.ResponseWriter, issued int)
}

// newFake starts a fake Keycloak. groupsHandler answers the realm's group
// listing, childrenHandler the children endpoint; a nil childrenHandler is a
// Keycloak that does not have that endpoint yet.
func newFake(t *testing.T, groupsHandler http.HandlerFunc, childrenHandler http.HandlerFunc) *fakeKeycloak {
	t.Helper()
	fake := &fakeKeycloak{expiresIn: 300}

	mux := http.NewServeMux()
	mux.HandleFunc("/realms/"+testRealm+"/protocol/openid-connect/token", func(writer http.ResponseWriter, request *http.Request) {
		fake.handleToken(t, writer, request)
	})
	mux.HandleFunc("/admin/realms/"+testRealm+"/groups", func(writer http.ResponseWriter, request *http.Request) {
		fake.record(request)
		groupsHandler(writer, request)
	})
	mux.HandleFunc("/admin/realms/"+testRealm+"/groups/", func(writer http.ResponseWriter, request *http.Request) {
		fake.record(request)
		if childrenHandler == nil {
			http.NotFound(writer, request)
			return
		}
		childrenHandler(writer, request)
	})
	mux.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		t.Errorf("unexpected request to %v %v", request.Method, request.URL.String())
		http.NotFound(writer, request)
	})

	fake.server = httptest.NewServer(mux)
	t.Cleanup(fake.server.Close)
	return fake
}

func (this *fakeKeycloak) handleToken(t *testing.T, writer http.ResponseWriter, request *http.Request) {
	t.Helper()
	if request.Method != http.MethodPost {
		t.Errorf("token request method is %v, want POST", request.Method)
	}
	if contentType := request.Header.Get("Content-Type"); contentType != "application/x-www-form-urlencoded" {
		t.Errorf("token request content type is %q, want form encoding", contentType)
	}
	if err := request.ParseForm(); err != nil {
		t.Errorf("token request body is not form encoded: %v", err)
	}

	this.mutex.Lock()
	this.tokens = append(this.tokens, recordedRequest{Method: request.Method, Path: request.URL.Path})
	this.forms = append(this.forms, request.PostForm)
	issued := len(this.tokens)
	expiresIn := this.expiresIn
	body := this.tokenBody
	this.mutex.Unlock()

	writer.Header().Set("Content-Type", "application/json")
	if body != nil {
		body(writer, issued)
		return
	}
	fmt.Fprintf(writer, `{"access_token":%q,"expires_in":%d,"token_type":"Bearer"}`, accessToken(issued), expiresIn)
}

func (this *fakeKeycloak) record(request *http.Request) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.groups = append(this.groups, recordedRequest{
		Method: request.Method,
		Path:   request.URL.Path,
		Query:  request.URL.Query(),
		Auth:   request.Header.Get("Authorization"),
	})
}

func (this *fakeKeycloak) tokenRequests() []recordedRequest {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	return append([]recordedRequest{}, this.tokens...)
}

func (this *fakeKeycloak) tokenForms() []url.Values {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	return append([]url.Values{}, this.forms...)
}

func (this *fakeKeycloak) groupRequests() []recordedRequest {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	return append([]recordedRequest{}, this.groups...)
}

// client builds the client under test against the fake.
func (this *fakeKeycloak) client(pageSize int) *Client {
	return New(config.KeycloakConfig{
		Url:          this.server.URL,
		Realm:        testRealm,
		ClientId:     testClientId,
		ClientSecret: sb_config_types.Secret(testSecret),
		PageSize:     pageSize,
	}, 5*time.Second)
}

// accessToken is the token the fake hands out on its nth grant. Distinct per
// grant, so a test can tell a reused token from a fresh one.
func accessToken(issued int) string {
	return fmt.Sprintf("access-token-%d", issued)
}

// fakeClock is the injected clock of the expiry tests.
type fakeClock struct {
	mutex sync.Mutex
	now   time.Time
}

func (this *fakeClock) Now() time.Time {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	return this.now
}

func (this *fakeClock) advance(d time.Duration) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.now = this.now.Add(d)
}

// modern is a group as Keycloak answers it since it loads children lazily:
// subGroups empty, subGroupCount saying how many there are.
//
// The name field deliberately does not match the path's leaf - the client has
// to derive the name from the path, not read this.
func modern(id string, path string, subGroupCount int) string {
	return fmt.Sprintf(`{"id":%q,"name":%q,"path":%q,"subGroups":[],"subGroupCount":%d}`,
		id, "keycloak-name-of-"+id, path, subGroupCount)
}

// legacy is a group as an older Keycloak answers it: the whole subtree inline
// and no subGroupCount at all.
func legacy(id string, path string, subGroups ...string) string {
	return fmt.Sprintf(`{"id":%q,"name":%q,"path":%q,"subGroups":[%v]}`,
		id, "keycloak-name-of-"+id, path, strings.Join(subGroups, ","))
}

// serve answers a listing request out of groups, honouring max and first, so
// the pagination under test is the real thing rather than a scripted sequence.
func serve(t *testing.T, groups []string) http.HandlerFunc {
	t.Helper()
	return func(writer http.ResponseWriter, request *http.Request) {
		first, err := strconv.Atoi(request.URL.Query().Get("first"))
		if err != nil {
			t.Errorf("listing request without a usable first: %v", request.URL.String())
			http.Error(writer, "bad first", http.StatusBadRequest)
			return
		}
		max, err := strconv.Atoi(request.URL.Query().Get("max"))
		if err != nil || max <= 0 {
			t.Errorf("listing request without a usable max: %v", request.URL.String())
			http.Error(writer, "bad max", http.StatusBadRequest)
			return
		}
		page := []string{}
		for index := first; index < first+max && index < len(groups); index++ {
			page = append(page, groups[index])
		}
		writer.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(writer, "[%v]", strings.Join(page, ","))
	}
}

// childrenOf answers the children endpoint out of a map of parent id to
// subgroups.
func childrenOf(t *testing.T, children map[string][]string) http.HandlerFunc {
	t.Helper()
	return func(writer http.ResponseWriter, request *http.Request) {
		parent, ok := parentOfChildrenPath(request.URL.Path)
		if !ok {
			t.Errorf("unexpected group request %v", request.URL.Path)
			http.NotFound(writer, request)
			return
		}
		serve(t, children[parent])(writer, request)
	}
}

// parentOfChildrenPath pulls the group id out of a children path.
func parentOfChildrenPath(path string) (string, bool) {
	prefix := "/admin/realms/" + testRealm + "/groups/"
	if !strings.HasPrefix(path, prefix) || !strings.HasSuffix(path, "/children") {
		return "", false
	}
	return strings.TrimSuffix(strings.TrimPrefix(path, prefix), "/children"), true
}

// paths is the group paths of a result, in result order.
func paths(groups []model.Group) []string {
	result := []string{}
	for _, group := range groups {
		result = append(result, group.Path)
	}
	return result
}

func equal(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for index := range a {
		if a[index] != b[index] {
			return false
		}
	}
	return true
}

func TestTokenRequestAndBearer(t *testing.T) {
	fake := newFake(t, serve(t, []string{modern("id-acme", "/acme", 0)}), nil)
	client := fake.client(10)

	groups, err := client.Groups(context.Background())
	if err != nil {
		t.Fatalf("Groups: %v", err)
	}
	if len(groups) != 1 {
		t.Fatalf("got %v groups, want 1", len(groups))
	}

	tokenRequests := fake.tokenRequests()
	if len(tokenRequests) != 1 {
		t.Fatalf("got %v token requests, want 1", len(tokenRequests))
	}
	wantPath := "/realms/" + testRealm + "/protocol/openid-connect/token"
	if tokenRequests[0].Path != wantPath {
		t.Errorf("token path is %v, want %v", tokenRequests[0].Path, wantPath)
	}

	form := fake.tokenForms()[0]
	if got := form.Get("grant_type"); got != "client_credentials" {
		t.Errorf("grant_type is %q, want client_credentials", got)
	}
	if got := form.Get("client_id"); got != testClientId {
		t.Errorf("client_id is %q, want %q", got, testClientId)
	}
	if got := form.Get("client_secret"); got != testSecret {
		t.Errorf("client_secret is not the configured one")
	}

	// Two: the page carrying /acme and the empty page that confirms it was the
	// last one. A listing does not end on a short page - see listGroups.
	groupRequests := fake.groupRequests()
	if len(groupRequests) != 2 {
		t.Fatalf("got %v group requests, want 2", len(groupRequests))
	}
	for index, request := range groupRequests {
		if want := "Bearer " + accessToken(1); request.Auth != want {
			t.Errorf("authorization of request %v is %q, want %q", index, request.Auth, want)
		}
	}
}

func TestCachedTokenIsReused(t *testing.T) {
	fake := newFake(t, serve(t, []string{modern("id-acme", "/acme", 0)}), nil)
	client := fake.client(10)

	for round := 0; round < 3; round++ {
		if _, err := client.Groups(context.Background()); err != nil {
			t.Fatalf("Groups in round %v: %v", round, err)
		}
	}
	if got := len(fake.tokenRequests()); got != 1 {
		t.Errorf("got %v token requests, want 1 - the token is cached", got)
	}
	for _, request := range fake.groupRequests() {
		if want := "Bearer " + accessToken(1); request.Auth != want {
			t.Errorf("authorization is %q, want %q", request.Auth, want)
		}
	}
}

func TestExpiredTokenIsRequestedAgain(t *testing.T) {
	fake := newFake(t, serve(t, []string{modern("id-acme", "/acme", 0)}), nil)
	fake.expiresIn = 60
	client := fake.client(10)

	clock := &fakeClock{now: time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)}
	client.now = clock.Now

	if _, err := client.Groups(context.Background()); err != nil {
		t.Fatalf("first Groups: %v", err)
	}

	// 60 s lifetime less the 30 s margin: still valid here.
	clock.advance(29 * time.Second)
	if _, err := client.Groups(context.Background()); err != nil {
		t.Fatalf("second Groups: %v", err)
	}
	if got := len(fake.tokenRequests()); got != 1 {
		t.Fatalf("got %v token requests inside the lifetime, want 1", got)
	}

	// Past the margin: the token counts as gone even though Keycloak would
	// still accept it for another 29 s.
	clock.advance(2 * time.Second)
	if _, err := client.Groups(context.Background()); err != nil {
		t.Fatalf("third Groups: %v", err)
	}
	if got := len(fake.tokenRequests()); got != 2 {
		t.Fatalf("got %v token requests after expiry, want 2", got)
	}

	requests := fake.groupRequests()
	if want := "Bearer " + accessToken(2); requests[len(requests)-1].Auth != want {
		t.Errorf("last authorization is %q, want the fresh token %q", requests[len(requests)-1].Auth, want)
	}
}

func TestTokenWithoutExpiresInIsNotCached(t *testing.T) {
	fake := newFake(t, serve(t, []string{modern("id-acme", "/acme", 0)}), nil)
	fake.tokenBody = func(writer http.ResponseWriter, issued int) {
		fmt.Fprintf(writer, `{"access_token":%q}`, accessToken(issued))
	}
	client := fake.client(10)

	for round := 0; round < 2; round++ {
		if _, err := client.Groups(context.Background()); err != nil {
			t.Fatalf("Groups in round %v: %v", round, err)
		}
	}
	// Not cached means one grant per authenticated request, so the two counts
	// have to match; pinning an absolute number would pin the page count too.
	tokens, requests := len(fake.tokenRequests()), len(fake.groupRequests())
	if tokens < 2 || tokens != requests {
		t.Errorf("got %v token requests for %v group requests, want one grant each - a token of unknown lifetime is not cached", tokens, requests)
	}
}

func TestTokenAnswerWithoutTokenIsAnError(t *testing.T) {
	fake := newFake(t, serve(t, []string{}), nil)
	fake.tokenBody = func(writer http.ResponseWriter, issued int) {
		fmt.Fprint(writer, `{"expires_in":300}`)
	}
	client := fake.client(10)

	_, err := client.Groups(context.Background())
	if err == nil {
		t.Fatal("want an error for a token answer without a token")
	}
	if !strings.Contains(err.Error(), "no access token") {
		t.Errorf("error is %q, want it to name the missing token", err)
	}
}

func TestPagination(t *testing.T) {
	all := []string{
		modern("id-1", "/g1", 0),
		modern("id-2", "/g2", 0),
		modern("id-3", "/g3", 0),
		modern("id-4", "/g4", 0),
		modern("id-5", "/g5", 0),
	}
	fake := newFake(t, serve(t, all), nil)
	client := fake.client(2)

	groups, err := client.Groups(context.Background())
	if err != nil {
		t.Fatalf("Groups: %v", err)
	}
	want := []string{"/g1", "/g2", "/g3", "/g4", "/g5"}
	if !equal(paths(groups), want) {
		t.Errorf("got %v, want %v", paths(groups), want)
	}

	// Four requests for five groups: 2, 2, 1 and the empty page that ends the
	// listing. The short third page does not end it - see listGroups.
	requests := fake.groupRequests()
	if len(requests) != 4 {
		t.Fatalf("got %v listing requests, want 4 pages of 2, 2, 1 and 0", len(requests))
	}
	for index, wantFirst := range []string{"0", "2", "4", "6"} {
		if got := requests[index].Query.Get("first"); got != wantFirst {
			t.Errorf("page %v asked for first=%v, want %v", index, got, wantFirst)
		}
		if got := requests[index].Query.Get("max"); got != "2" {
			t.Errorf("page %v asked for max=%v, want 2", index, got)
		}
	}
}

func TestPageSizeFallsBackWhenUnconfigured(t *testing.T) {
	fake := newFake(t, serve(t, []string{modern("id-acme", "/acme", 0)}), nil)
	client := fake.client(0)

	if _, err := client.Groups(context.Background()); err != nil {
		t.Fatalf("Groups: %v", err)
	}
	requests := fake.groupRequests()
	if len(requests) == 0 {
		t.Fatal("got no listing request")
	}
	for index, request := range requests {
		if got := request.Query.Get("max"); got != strconv.Itoa(defaultPageSize) {
			t.Errorf("max of request %v is %v, want the fallback %v - max=0 would read as an empty realm", index, got, defaultPageSize)
		}
	}
}

func TestNestedGroupsInline(t *testing.T) {
	tree := legacy("id-acme", "/acme",
		legacy("id-plant", "/acme/plant",
			legacy("id-hall", "/acme/plant/hall-1"),
		),
		legacy("id-office", "/acme/office"),
	)
	fake := newFake(t, serve(t, []string{tree}), nil)
	client := fake.client(10)

	groups, err := client.Groups(context.Background())
	if err != nil {
		t.Fatalf("Groups: %v", err)
	}
	want := []string{"/acme", "/acme/office", "/acme/plant", "/acme/plant/hall-1"}
	if !equal(paths(groups), want) {
		t.Errorf("got %v, want %v", paths(groups), want)
	}
	// A group whose children came inline is not asked for them again. The
	// leaves are: an inline answer carries no subGroupCount, so "no subGroups"
	// and "children not delivered" look the same from here, and one 404 probe
	// per leaf is the price of not silently losing a subtree on a Keycloak that
	// does load children lazily.
	probed := map[string]bool{}
	for _, request := range fake.groupRequests() {
		if parent, isChildren := parentOfChildrenPath(request.Path); isChildren {
			probed[parent] = true
		}
	}
	if probed["id-acme"] || probed["id-plant"] {
		t.Errorf("children were fetched although the answer carried them inline: %v", probed)
	}
}

func TestNestedGroupsViaChildrenEndpoint(t *testing.T) {
	roots := []string{modern("id-acme", "/acme", 2)}
	children := map[string][]string{
		"id-acme": {
			modern("id-office", "/acme/office", 0),
			modern("id-plant", "/acme/plant", 1),
		},
		"id-plant": {modern("id-hall", "/acme/plant/hall-1", 0)},
	}
	fake := newFake(t, serve(t, roots), childrenOf(t, children))
	client := fake.client(10)

	groups, err := client.Groups(context.Background())
	if err != nil {
		t.Fatalf("Groups: %v", err)
	}
	want := []string{"/acme", "/acme/office", "/acme/plant", "/acme/plant/hall-1"}
	if !equal(paths(groups), want) {
		t.Errorf("got %v, want %v", paths(groups), want)
	}

	fetched := map[string]bool{}
	for _, request := range fake.groupRequests() {
		if parent, isChildren := parentOfChildrenPath(request.Path); isChildren {
			fetched[parent] = true
			// first advances per page, so only its presence is pinned here.
			if request.Query.Get("max") != "10" || request.Query.Get("first") == "" {
				t.Errorf("children of %v were fetched unpaginated: %v", parent, request.Query)
			}
		}
	}
	if !fetched["id-acme"] || !fetched["id-plant"] {
		t.Errorf("children were fetched for %v, want id-acme and id-plant", fetched)
	}
	// subGroupCount 0 says there is nothing to fetch.
	if fetched["id-office"] || fetched["id-hall"] {
		t.Errorf("children were fetched for a group that reported none: %v", fetched)
	}
}

func TestLeafWithoutSubGroupCountToleratesMissingChildrenEndpoint(t *testing.T) {
	// An older Keycloak: the whole tree inline, no subGroupCount, and no
	// children endpoint. Asking for the children of a leaf answers 404, and
	// that must not fail the poll.
	tree := legacy("id-acme", "/acme", legacy("id-plant", "/acme/plant"))
	fake := newFake(t, serve(t, []string{tree}), nil)
	client := fake.client(10)

	groups, err := client.Groups(context.Background())
	if err != nil {
		t.Fatalf("Groups: %v", err)
	}
	want := []string{"/acme", "/acme/plant"}
	if !equal(paths(groups), want) {
		t.Errorf("got %v, want %v", paths(groups), want)
	}
}

func TestChildrenErrorOtherThanNotFoundFails(t *testing.T) {
	roots := []string{modern("id-acme", "/acme", 1)}
	fake := newFake(t, serve(t, roots), func(writer http.ResponseWriter, request *http.Request) {
		http.Error(writer, "boom", http.StatusInternalServerError)
	})
	client := fake.client(10)

	_, err := client.Groups(context.Background())
	if err == nil {
		t.Fatal("want an error for a failing children request")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("error is %q, want the status in it", err)
	}
}

func TestNameIsTheLeafOfThePath(t *testing.T) {
	roots := []string{legacy("id-acme", "/acme",
		legacy("id-plant", "/acme/plant",
			legacy("id-hall", "/acme/plant/hall-1"),
		),
	)}
	fake := newFake(t, serve(t, roots), nil)
	client := fake.client(10)

	groups, err := client.Groups(context.Background())
	if err != nil {
		t.Fatalf("Groups: %v", err)
	}
	want := map[string]string{
		"/acme":              "acme",
		"/acme/plant":        "plant",
		"/acme/plant/hall-1": "hall-1",
	}
	if len(groups) != len(want) {
		t.Fatalf("got %v groups, want %v", len(groups), len(want))
	}
	for _, group := range groups {
		if group.Name != want[group.Path] {
			t.Errorf("%v is named %q, want %q", group.Path, group.Name, want[group.Path])
		}
		if strings.Contains(group.Name, "keycloak-name-of-") {
			t.Errorf("%v took its name from keycloak's name field", group.Path)
		}
	}
}

func TestUnauthorizedGroupCall(t *testing.T) {
	fake := newFake(t, func(writer http.ResponseWriter, request *http.Request) {
		http.Error(writer, "not allowed", http.StatusUnauthorized)
	}, nil)
	client := fake.client(10)

	groups, err := client.Groups(context.Background())
	if err == nil {
		t.Fatal("want an error for a 401")
	}
	if groups != nil {
		t.Errorf("got %v groups next to the error, want none", len(groups))
	}
	if !strings.Contains(err.Error(), "401") {
		t.Errorf("error is %q, want the status in it", err)
	}
	if !strings.Contains(err.Error(), "/admin/realms/"+testRealm+"/groups") {
		t.Errorf("error is %q, want the path in it", err)
	}
	if strings.Contains(err.Error(), "not allowed") {
		t.Errorf("error is %q, want it to leave the answer body out", err)
	}
}

func TestCredentialsNeverReachAnError(t *testing.T) {
	// Every answer echoes the secret and a token, which is exactly what an
	// error body from a token endpoint can do.
	echo := func(writer http.ResponseWriter, status int) {
		writer.WriteHeader(status)
		fmt.Fprintf(writer, `{"error":"invalid_client","secret":%q,"token":%q}`, testSecret, accessToken(1))
	}

	tests := []struct {
		name    string
		fake    func(t *testing.T) *fakeKeycloak
		wantErr bool
	}{
		{
			name: "token endpoint rejects the credentials",
			fake: func(t *testing.T) *fakeKeycloak {
				fake := newFake(t, serve(t, []string{}), nil)
				fake.tokenBody = func(writer http.ResponseWriter, issued int) {
					echo(writer, http.StatusUnauthorized)
				}
				return fake
			},
		},
		{
			name: "token answer is not json",
			fake: func(t *testing.T) *fakeKeycloak {
				fake := newFake(t, serve(t, []string{}), nil)
				fake.tokenBody = func(writer http.ResponseWriter, issued int) {
					fmt.Fprintf(writer, "not json, and here is the secret %v", testSecret)
				}
				return fake
			},
		},
		{
			name: "group listing rejects the token",
			fake: func(t *testing.T) *fakeKeycloak {
				return newFake(t, func(writer http.ResponseWriter, request *http.Request) {
					echo(writer, http.StatusForbidden)
				}, nil)
			},
		},
		{
			name: "group listing is not json",
			fake: func(t *testing.T) *fakeKeycloak {
				return newFake(t, func(writer http.ResponseWriter, request *http.Request) {
					fmt.Fprintf(writer, "not json, and here is the secret %v", testSecret)
				}, nil)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fake := test.fake(t)
			client := fake.client(10)

			_, err := client.Groups(context.Background())
			if err == nil {
				t.Fatal("want an error")
			}
			if strings.Contains(err.Error(), testSecret) {
				t.Errorf("the client secret reached the error: %q", err)
			}
			if strings.Contains(err.Error(), accessToken(1)) {
				t.Errorf("the access token reached the error: %q", err)
			}
			// The same for the value a caller might dump while handling it.
			for _, formatted := range []string{fmt.Sprintf("%v", client), fmt.Sprintf("%+v", client), fmt.Sprintf("%#v", client)} {
				if strings.Contains(formatted, testSecret) {
					t.Errorf("the client secret reached a formatted client: %v", formatted)
				}
			}
		})
	}
}

func TestFormattedClientHidesTheToken(t *testing.T) {
	fake := newFake(t, serve(t, []string{modern("id-acme", "/acme", 0)}), nil)
	client := fake.client(10)
	if _, err := client.Groups(context.Background()); err != nil {
		t.Fatalf("Groups: %v", err)
	}
	for _, formatted := range []string{fmt.Sprintf("%v", client), fmt.Sprintf("%+v", client), fmt.Sprintf("%#v", client)} {
		if strings.Contains(formatted, accessToken(1)) {
			t.Errorf("the cached access token reached a formatted client: %v", formatted)
		}
		if strings.Contains(formatted, testSecret) {
			t.Errorf("the client secret reached a formatted client: %v", formatted)
		}
	}
}

func TestContextCancellationAborts(t *testing.T) {
	entered := make(chan struct{})
	fake := newFake(t, func(writer http.ResponseWriter, request *http.Request) {
		close(entered)
		<-request.Context().Done()
	}, nil)
	client := fake.client(10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-entered
		cancel()
	}()

	done := make(chan error, 1)
	go func() {
		_, err := client.Groups(ctx)
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("want an error for a cancelled context")
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("error is %q, want it to be context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Groups did not return after the context was cancelled")
	}
}

func TestCancelledContextIsNotEvenSent(t *testing.T) {
	fake := newFake(t, serve(t, []string{modern("id-acme", "/acme", 0)}), nil)
	client := fake.client(10)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := client.Groups(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error is %v, want context.Canceled", err)
	}
	if got := len(fake.tokenRequests()); got != 0 {
		t.Errorf("got %v token requests for a cancelled context, want none", got)
	}
}

func TestCycleIsNotFollowed(t *testing.T) {
	roots := []string{modern("id-acme", "/acme", 1)}
	// The children endpoint answers with the parent itself.
	fake := newFake(t, serve(t, roots), childrenOf(t, map[string][]string{
		"id-acme": {modern("id-acme", "/acme", 1)},
	}))
	client := fake.client(10)

	done := make(chan struct{})
	var groups []model.Group
	var err error
	go func() {
		groups, err = client.Groups(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Groups did not return on a cyclic group tree")
	}
	if err != nil {
		t.Fatalf("Groups: %v", err)
	}
	if !equal(paths(groups), []string{"/acme"}) {
		t.Errorf("got %v, want /acme exactly once", paths(groups))
	}
}

func TestDepthLimitIsAnError(t *testing.T) {
	// A server that invents one new group per level, so the id set never
	// catches it and only the depth limit can.
	roots := []string{modern("id-0", "/level-0", 1)}
	fake := newFake(t, serve(t, roots), func(writer http.ResponseWriter, request *http.Request) {
		parent, ok := parentOfChildrenPath(request.URL.Path)
		if !ok {
			t.Errorf("unexpected group request %v", request.URL.Path)
			http.NotFound(writer, request)
			return
		}
		level, err := strconv.Atoi(strings.TrimPrefix(parent, "id-"))
		if err != nil {
			t.Errorf("unexpected parent id %v", parent)
			http.NotFound(writer, request)
			return
		}
		next := level + 1
		serve(t, []string{modern(fmt.Sprintf("id-%d", next), fmt.Sprintf("/level-%d", next), 1)})(writer, request)
	})
	client := fake.client(10)

	_, err := client.Groups(context.Background())
	if err == nil {
		t.Fatal("want an error for an endless group tree")
	}
	if !strings.Contains(err.Error(), "depth limit") {
		t.Errorf("error is %q, want it to name the depth limit", err)
	}
}

func TestUnusableGroupIsSkipped(t *testing.T) {
	// A group without a path has no identity for permissions-v2 and one
	// without an id cannot be asked for children. Both are dropped, the rest
	// of the listing survives.
	roots := []string{
		`{"id":"id-1","name":"no path","subGroups":[],"subGroupCount":0}`,
		`{"id":"","name":"no id","path":"/nameless","subGroups":[],"subGroupCount":0}`,
		modern("id-acme", "/acme", 0),
	}
	fake := newFake(t, serve(t, roots), nil)
	client := fake.client(10)

	groups, err := client.Groups(context.Background())
	if err != nil {
		t.Fatalf("Groups: %v", err)
	}
	if !equal(paths(groups), []string{"/acme"}) {
		t.Errorf("got %v, want /acme only", paths(groups))
	}
}

func TestConcurrentGroups(t *testing.T) {
	all := []string{
		modern("id-1", "/g1", 0),
		modern("id-2", "/g2", 0),
		modern("id-3", "/g3", 0),
	}
	fake := newFake(t, serve(t, all), nil)
	client := fake.client(2)

	wait := sync.WaitGroup{}
	errs := make(chan error, 8)
	for worker := 0; worker < 8; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			groups, err := client.Groups(context.Background())
			if err != nil {
				errs <- err
				return
			}
			if !equal(paths(groups), []string{"/g1", "/g2", "/g3"}) {
				errs <- fmt.Errorf("got %v, want /g1 /g2 /g3", paths(groups))
			}
		}()
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if got := len(fake.tokenRequests()); got != 1 {
		t.Errorf("got %v token requests from 8 concurrent callers, want 1", got)
	}
}

func TestShortPageDoesNotTruncateTheListing(t *testing.T) {
	// Keycloak applies max to the window it reads before it filters, so a page
	// can be shorter than max and still have groups behind it. Stopping there
	// hands back a tree that looks complete: cache.RefreshGroups replaces the
	// tree wholesale, so every group after the short page counts as removed and
	// reconcile.Full unshares its graph.
	all := []string{}
	for index := 1; index <= 10; index++ {
		all = append(all, modern(fmt.Sprintf("id-%02d", index), fmt.Sprintf("/g%02d", index), 0))
	}
	// The fourth group is filtered out server side, so the first page of four
	// carries three.
	const hidden = 3
	fake := newFake(t, func(writer http.ResponseWriter, request *http.Request) {
		first, err := strconv.Atoi(request.URL.Query().Get("first"))
		if err != nil {
			t.Errorf("listing request without a usable first: %v", request.URL.String())
			http.Error(writer, "bad first", http.StatusBadRequest)
			return
		}
		max, err := strconv.Atoi(request.URL.Query().Get("max"))
		if err != nil || max <= 0 {
			t.Errorf("listing request without a usable max: %v", request.URL.String())
			http.Error(writer, "bad max", http.StatusBadRequest)
			return
		}
		page := []string{}
		for index := first; index < first+max && index < len(all); index++ {
			if index == hidden {
				continue
			}
			page = append(page, all[index])
		}
		writer.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(writer, "[%v]", strings.Join(page, ","))
	}, nil)
	client := fake.client(4)

	groups, err := client.Groups(context.Background())
	if err != nil {
		t.Fatalf("Groups: %v", err)
	}
	want := []string{"/g01", "/g02", "/g03", "/g05", "/g06", "/g07", "/g08", "/g09", "/g10"}
	if !equal(paths(groups), want) {
		t.Errorf("got %v groups %v, want %v %v", len(groups), paths(groups), len(want), want)
	}
}

func TestRedirectDoesNotCarryCredentials(t *testing.T) {
	// A 307 replays the request body, and the body of the token request is the
	// client secret. Following it would hand the secret to the redirect target,
	// including a foreign host.
	t.Run("token endpoint", func(t *testing.T) {
		delivered := make(chan url.Values, 4)
		target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			body, _ := io.ReadAll(request.Body)
			form, _ := url.ParseQuery(string(body))
			delivered <- form
			writer.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(writer, `{"access_token":%q,"expires_in":300}`, accessToken(99))
		}))
		t.Cleanup(target.Close)

		fake := newFake(t, serve(t, []string{modern("id-acme", "/acme", 0)}), nil)
		fake.tokenBody = func(writer http.ResponseWriter, issued int) {
			writer.Header().Set("Location", target.URL+"/elsewhere")
			writer.WriteHeader(http.StatusTemporaryRedirect)
		}
		client := fake.client(10)

		_, err := client.Groups(context.Background())
		if err == nil {
			t.Fatal("want an error for a redirected token request")
		}
		if !strings.Contains(err.Error(), strconv.Itoa(http.StatusTemporaryRedirect)) {
			t.Errorf("error is %q, want the redirect status in it", err)
		}
		if strings.Contains(err.Error(), testSecret) {
			t.Errorf("the client secret reached the error: %q", err)
		}
		close(delivered)
		for form := range delivered {
			t.Errorf("the redirect target was called, with client_secret=%q", form.Get("client_secret"))
		}
	})

	t.Run("admin endpoint", func(t *testing.T) {
		// Go copies the Authorization header onto a redirect whose hostname
		// matches, and it compares hostnames only - an https to http downgrade
		// keeps the header.
		delivered := make(chan string, 4)
		target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			delivered <- request.Header.Get("Authorization")
			writer.Header().Set("Content-Type", "application/json")
			fmt.Fprint(writer, "[]")
		}))
		t.Cleanup(target.Close)

		fake := newFake(t, func(writer http.ResponseWriter, request *http.Request) {
			writer.Header().Set("Location", target.URL+"/elsewhere")
			writer.WriteHeader(http.StatusTemporaryRedirect)
		}, nil)
		client := fake.client(10)

		_, err := client.Groups(context.Background())
		if err == nil {
			t.Fatal("want an error for a redirected group listing")
		}
		if !strings.Contains(err.Error(), strconv.Itoa(http.StatusTemporaryRedirect)) {
			t.Errorf("error is %q, want the redirect status in it", err)
		}
		close(delivered)
		for authorization := range delivered {
			t.Errorf("the redirect target was called, with authorization %q", authorization)
		}
	})
}

func TestExpiresInIsNotRequiredToBeAJsonInteger(t *testing.T) {
	// Keycloak answers with an integer, but a proxy that re-serialises the
	// answer can turn it into a float or a string. Failing there costs every
	// poll, and the decoder's own message may not be shown.
	tests := []struct {
		name      string
		expiresIn string
	}{
		{name: "float", expiresIn: `300.5`},
		{name: "numeric string", expiresIn: `"300"`},
		{name: "exponent", expiresIn: `3e2`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fake := newFake(t, serve(t, []string{modern("id-acme", "/acme", 0)}), nil)
			fake.tokenBody = func(writer http.ResponseWriter, issued int) {
				fmt.Fprintf(writer, `{"access_token":%q,"expires_in":%v}`, accessToken(issued), test.expiresIn)
			}
			client := fake.client(10)

			for round := 0; round < 2; round++ {
				if _, err := client.Groups(context.Background()); err != nil {
					t.Fatalf("Groups in round %v: %v", round, err)
				}
			}
			if got := len(fake.tokenRequests()); got != 1 {
				t.Errorf("got %v token requests, want 1 - %v is a usable lifetime", got, test.expiresIn)
			}
		})
	}
}

func TestUnusableExpiresInNamesTheField(t *testing.T) {
	tests := []struct {
		name      string
		expiresIn string
		wantType  string
	}{
		{name: "object", expiresIn: `{}`, wantType: "object"},
		{name: "boolean", expiresIn: `true`, wantType: "boolean"},
		{name: "non numeric string", expiresIn: `"not-a-number"`, wantType: "string"},
		{name: "out of range", expiresIn: `1e400`, wantType: "number"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fake := newFake(t, serve(t, []string{modern("id-acme", "/acme", 0)}), nil)
			fake.tokenBody = func(writer http.ResponseWriter, issued int) {
				fmt.Fprintf(writer, `{"access_token":%q,"expires_in":%v}`, accessToken(issued), test.expiresIn)
			}
			client := fake.client(10)

			_, err := client.Groups(context.Background())
			if err == nil {
				t.Fatal("want an error for an unusable expires_in")
			}
			if !strings.Contains(err.Error(), "expires_in") {
				t.Errorf("error is %q, want it to name the field", err)
			}
			if !strings.Contains(err.Error(), test.wantType) {
				t.Errorf("error is %q, want it to name the type %v", err, test.wantType)
			}
			// The field's content is not in the error: the same body carries
			// the token, and a message that quotes what it choked on has
			// quoted a token before.
			if strings.Contains(err.Error(), strings.Trim(test.expiresIn, `"`)) {
				t.Errorf("error is %q, want it to leave the value out", err)
			}
			if strings.Contains(err.Error(), accessToken(1)) {
				t.Errorf("the access token reached the error: %q", err)
			}
		})
	}
}

func TestNullExpiresInIsNotCached(t *testing.T) {
	// An explicit null says as little as an absent field, and absent is not an
	// error - the token is used once and not kept.
	fake := newFake(t, serve(t, []string{modern("id-acme", "/acme", 0)}), nil)
	fake.tokenBody = func(writer http.ResponseWriter, issued int) {
		fmt.Fprintf(writer, `{"access_token":%q,"expires_in":null}`, accessToken(issued))
	}
	client := fake.client(10)

	for round := 0; round < 2; round++ {
		if _, err := client.Groups(context.Background()); err != nil {
			t.Fatalf("Groups in round %v: %v", round, err)
		}
	}
	tokens, requests := len(fake.tokenRequests()), len(fake.groupRequests())
	if tokens < 2 || tokens != requests {
		t.Errorf("got %v token requests for %v group requests, want one grant each - a token of unknown lifetime is not cached", tokens, requests)
	}
}

func TestWaitingCallerHonoursItsOwnDeadline(t *testing.T) {
	// A grant in flight must not make a second caller wait past its own
	// deadline: a mutex held across the request ignores the waiter's context
	// entirely, which delays a shutdown by up to one http timeout per caller.
	release := make(chan struct{})
	entered := make(chan struct{})
	once := sync.Once{}
	fake := newFake(t, serve(t, []string{modern("id-acme", "/acme", 0)}), nil)
	fake.tokenBody = func(writer http.ResponseWriter, issued int) {
		once.Do(func() { close(entered) })
		<-release
		fmt.Fprintf(writer, `{"access_token":%q,"expires_in":300}`, accessToken(issued))
	}
	client := fake.client(10)

	first := make(chan error, 1)
	go func() {
		_, err := client.Groups(context.Background())
		first <- err
	}()
	<-entered

	second := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_, err := client.Groups(ctx)
		second <- err
	}()

	var err error
	select {
	case err = <-second:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("the waiting caller did not return on its own deadline")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error is %v, want context.DeadlineExceeded", err)
	}

	close(release)
	select {
	case err := <-first:
		if err != nil {
			t.Fatalf("first Groups: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the in-flight grant did not finish")
	}
	if got := len(fake.tokenRequests()); got != 1 {
		t.Errorf("got %v token requests, want 1 - the waiter must not start one of its own", got)
	}
}

func TestFormattedClientHidesTheGrantsToken(t *testing.T) {
	// The grant is the one field the client gained for the single-flight, and
	// it carries a token of its own. Set by hand rather than by racing a real
	// grant: formatting a client from another goroutine while a call is in
	// flight is a data race whatever the fields hold.
	fake := newFake(t, serve(t, []string{}), nil)
	client := fake.client(10)
	client.grant = &tokenGrant{done: make(chan struct{}), token: hold(accessToken(1))}

	for _, formatted := range []string{fmt.Sprintf("%v", client), fmt.Sprintf("%+v", client), fmt.Sprintf("%#v", client)} {
		if strings.Contains(formatted, accessToken(1)) {
			t.Errorf("the grant's access token reached a formatted client: %v", formatted)
		}
		if strings.Contains(formatted, testSecret) {
			t.Errorf("the client secret reached a formatted client: %v", formatted)
		}
	}
}

// Keycloak filters the group listing by what the caller may view. A service
// account holding query-groups but no view permission gets an empty array and
// HTTP 200 - indistinguishable from a realm with no groups, and taken at face
// value it would look to the rest of the service like every company having
// disappeared. The count endpoint is not filtered the same way, so the two
// disagreeing is what gives it away. Observed against a real realm: /groups
// answered [] while /groups/count answered {"count":2}.
func TestAnEmptyListingWithANonZeroCountIsAPermissionProblem(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case strings.HasSuffix(request.URL.Path, "/protocol/openid-connect/token"):
			_, _ = writer.Write([]byte(`{"access_token":"tok","expires_in":300}`))
		case strings.HasSuffix(request.URL.Path, "/groups/count"):
			_, _ = writer.Write([]byte(`{"count":2}`))
		case strings.HasSuffix(request.URL.Path, "/groups"):
			_, _ = writer.Write([]byte(`[]`))
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	_, err := clientFor(t, server.URL).Groups(context.Background())
	if err == nil {
		t.Fatal("an empty listing against a non-zero count must be an error, not an empty result")
	}
	for _, want := range []string{"view-users", "2 group"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error should name %q, got: %v", want, err)
		}
	}
}

// A realm that genuinely has no groups answers both consistently, and that is
// a valid answer rather than a fault.
func TestAnEmptyRealmIsNotAnError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case strings.HasSuffix(request.URL.Path, "/protocol/openid-connect/token"):
			_, _ = writer.Write([]byte(`{"access_token":"tok","expires_in":300}`))
		case strings.HasSuffix(request.URL.Path, "/groups/count"):
			_, _ = writer.Write([]byte(`{"count":0}`))
		case strings.HasSuffix(request.URL.Path, "/groups"):
			_, _ = writer.Write([]byte(`[]`))
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	groups, err := clientFor(t, server.URL).Groups(context.Background())
	if err != nil {
		t.Fatalf("an empty realm is an answer, not a fault: %v", err)
	}
	if len(groups) != 0 {
		t.Errorf("expected no groups, got %+v", groups)
	}
}

// A count endpoint that itself fails must not turn a legitimately empty
// listing into an error - the listing is what was asked for.
func TestAFailingCountDoesNotBreakAnEmptyListing(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case strings.HasSuffix(request.URL.Path, "/protocol/openid-connect/token"):
			_, _ = writer.Write([]byte(`{"access_token":"tok","expires_in":300}`))
		case strings.HasSuffix(request.URL.Path, "/groups/count"):
			writer.WriteHeader(http.StatusForbidden)
		case strings.HasSuffix(request.URL.Path, "/groups"):
			_, _ = writer.Write([]byte(`[]`))
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	if _, err := clientFor(t, server.URL).Groups(context.Background()); err != nil {
		t.Errorf("expected the empty listing to stand: %v", err)
	}
}

// clientFor is a client pointed at a bare httptest server, for the three tests
// above that need to script the count endpoint as well as the listing - which
// fakeKeycloak does not model.
func clientFor(t *testing.T, url string) *Client {
	t.Helper()
	return New(config.KeycloakConfig{
		Url:          url,
		Realm:        testRealm,
		ClientId:     testClientId,
		ClientSecret: sb_config_types.Secret(testSecret),
		PageSize:     10,
	}, 5*time.Second)
}
