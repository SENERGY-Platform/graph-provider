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

// Package graphs is the service's access to stored graphs and their sharing.
//
// It is the only writer in this service, which is also where the kill switch
// lives: with writing disabled everything else still runs, reads and computes,
// and nothing leaves the process.
package graphs

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"

	devicerepomodel "github.com/SENERGY-Platform/device-repository/lib/model"
	"github.com/SENERGY-Platform/graph-provider/pkg/model"
	platform "github.com/SENERGY-Platform/models/go/models"
	permissions "github.com/SENERGY-Platform/permissions-v2/pkg/client"
)

// pageSize is how many graphs are asked for per request.
const pageSize = 500

// Repository is the part of the device repository client this package uses.
//
// ReadGraph and DeleteGraph are absent because neither is needed, not because
// they could not be called: a single graph is read through ListGraphs with an
// id filter, and a graph is never deleted - a group that disappears has its
// sharing withdrawn, not its user's work removed. (An earlier version of this
// comment claimed those two lacked an admin bypass. They do not:
// permissions-v2 returns early for a token with the admin role in
// CheckTopicDefaultPermissionContext, so every check passes for this service's
// token. The conclusion stood for the wrong reason.)
type Repository interface {
	ListGraphs(token string, options devicerepomodel.GraphListOptions) (result []platform.Graph, total int64, err error, errCode int)
	SetGraph(token string, graph platform.Graph) (result platform.Graph, err error, code int)
}

// Permissions is the part of the permissions-v2 client this package uses.
type Permissions interface {
	GetResource(token string, topicId string, id string) (result permissions.Resource, err error, code int)
	SetPermission(token string, topicId string, id string, perms permissions.ResourcePermissions) (result permissions.ResourcePermissions, err error, code int)
}

type Store struct {
	repository  Repository
	permissions Permissions

	token         string
	graphTopic    string
	serviceUserId string

	// writable false is the kill switch. Reads still happen, writes are
	// reported as skipped.
	writable bool
}

func New(repository Repository, perms Permissions, token string, graphTopic string, serviceUserId string, writable bool) *Store {
	return &Store{
		repository:    repository,
		permissions:   perms,
		token:         token,
		graphTopic:    graphTopic,
		serviceUserId: serviceUserId,
		writable:      writable,
	}
}

// ErrReadOnly is returned instead of writing when the kill switch is set.
var ErrReadOnly = errors.New("writing is disabled by configuration")

func (this *Store) Writable() bool {
	return this.writable
}

// Defaults is this service's own graphs, indexed the two ways they can be
// recognised.
//
// ById is the normal case. ByPath holds only graphs that carry the path
// attribute without an id - graphs written before the id was recorded, or a
// write that was interrupted between the two. The caller adopts those and
// writes the id, so the second map empties itself over time.
type Defaults struct {
	ById   map[string]platform.Graph
	ByPath map[string]platform.Graph
}

// Defaults returns this service's own graphs.
//
// Filtered by the presence of either provenance attribute rather than by their
// values. A value would have to travel through the client's attributes_json
// parameter, which double-escapes it - the client percent-encodes the JSON and
// the query encoder escapes it again, so the repository is handed something
// that is no longer JSON and answers 400. Confirmed against the real
// repository in pkg/tests. The number of default graphs is the number of
// groups, so matching the values here costs nothing.
//
// One request per key, not one request naming both. The repository's plain
// attributes filter is documented as matching an element that carries an
// attribute "that is in the given list", but it builds one $elemMatch per
// listed attribute and ANDs them: a two-key filter returns only graphs
// carrying BOTH keys, and the graph that carries the path alone - written
// before the id was recorded, or a write interrupted between the two - would
// be invisible, so the service would create a second graph beside the one the
// user had built. pkg/tests pins this.
//
// Leaving Ids nil is what earns the admin bypass in the repository's graph
// controller: an id filter would send the call through a per-id permission
// check instead.
func (this *Store) Defaults(ctx context.Context) (Defaults, error) {
	result := Defaults{
		ById:   map[string]platform.Graph{},
		ByPath: map[string]platform.Graph{},
	}
	for _, key := range []string{model.AttrKeycloakGroupId, model.AttrKeycloakGroup} {
		if err := this.collect(ctx, key, result); err != nil {
			return result, err
		}
	}
	return result, nil
}

// collect adds every graph carrying one attribute key to result.
//
// A graph carrying both keys arrives twice, once per call. Indexing it is
// idempotent: keepLower is handed the same id both times.
func (this *Store) collect(ctx context.Context, attributeKey string, result Defaults) error {
	for offset := int64(0); ; offset += pageSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, _, err, _ := this.repository.ListGraphs(this.token, devicerepomodel.GraphListOptions{
			Limit:      pageSize,
			Offset:     offset,
			SortBy:     "id.asc",
			Attributes: []platform.Attribute{{Key: attributeKey}},
		})
		if err != nil {
			return fmt.Errorf("unable to list graphs by %v: %w", attributeKey, err)
		}
		for _, graph := range page {
			if groupId := GroupIdOf(graph); groupId != "" {
				keepLower(result.ById, groupId, graph)
				continue
			}
			if path := GroupPathOf(graph); path != "" {
				keepLower(result.ByPath, path, graph)
			}
		}
		if int64(len(page)) < pageSize {
			return nil
		}
	}
}

// keepLower stores graph under key, keeping the lower id when two graphs claim
// the same group.
//
// Two graphs claiming one group is a state this service cannot have produced.
// Choosing the lower id makes the outcome stable instead of dependent on paging
// order, and leaves the duplicate untouched for someone to look at.
func keepLower(into map[string]platform.Graph, key string, graph platform.Graph) {
	if previous, taken := into[key]; taken && previous.Id < graph.Id {
		return
	}
	into[key] = graph
}

// Get reads one graph back by id.
func (this *Store) Get(ctx context.Context, id string) (platform.Graph, bool, error) {
	if err := ctx.Err(); err != nil {
		return platform.Graph{}, false, err
	}
	page, _, err, _ := this.repository.ListGraphs(this.token, devicerepomodel.GraphListOptions{
		Ids: []string{id},
	})
	if err != nil {
		return platform.Graph{}, false, fmt.Errorf("unable to read graph %v: %w", id, err)
	}
	if len(page) == 0 {
		return platform.Graph{}, false, nil
	}
	return page[0], true, nil
}

// Save creates or updates a graph.
//
// Validated locally first: the repository would answer an invalid graph with a
// 400, and failing here separates a heuristic bug from a transport problem.
func (this *Store) Save(ctx context.Context, graph platform.Graph) (platform.Graph, error) {
	if err := graph.Valid(); err != nil {
		return graph, fmt.Errorf("refusing to write an invalid graph: %w", err)
	}
	if !this.writable {
		return graph, ErrReadOnly
	}
	if err := ctx.Err(); err != nil {
		return graph, err
	}
	result, err, code := this.repository.SetGraph(this.token, graph)
	if err != nil {
		return graph, fmt.Errorf("unable to write graph %v (%v): %w", graph.Id, code, err)
	}
	return result, nil
}

// Share gives a group read, write and execute on a graph, and nothing more.
//
// Not administrate: members are meant to edit the company graph, not to delete
// it or hand it on. Administrate stays with the owner.
//
// The current state is read first and the write skipped when it already
// agrees. Every write here produces a rights command on the graph topic that
// this service consumes itself; without the comparison that echo would ask for
// another write and the loop would not end.
func (this *Store) Share(ctx context.Context, graphId string, groupPath string) (changed bool, err error) {
	return this.setGroupPermission(ctx, graphId, groupPath, &permissions.PermissionsMap{
		Read:    true,
		Write:   true,
		Execute: true,
	})
}

// Unshare withdraws a group's access and leaves everything else alone.
//
// This is what a disappeared group gets. The graph is not deleted: removing a
// user's work in response to a directory event is the harsher of the two
// actions, and the graph stays usable to its owner.
func (this *Store) Unshare(ctx context.Context, graphId string, groupPath string) (changed bool, err error) {
	return this.setGroupPermission(ctx, graphId, groupPath, nil)
}

// setGroupPermission writes one group's entry, or removes it when want is nil.
func (this *Store) setGroupPermission(ctx context.Context, graphId string, groupPath string, want *permissions.PermissionsMap) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	resource, err, code := this.permissions.GetResource(this.token, this.graphTopic, graphId)
	if err != nil && code != http.StatusNotFound {
		return false, fmt.Errorf("unable to read permissions of graph %v: %w", graphId, err)
	}
	if code == http.StatusNotFound {
		// The graph has no permission resource, so there is nothing to share
		// and no safe way to invent one: writing permissions requires at least
		// one administrating user, and guessing who that is would be worse
		// than reporting that the graph is not ready yet.
		return false, fmt.Errorf("graph %v has no permission resource", graphId)
	}

	next := clonePermissions(resource.ResourcePermissions)
	current, held := next.GroupPermissions[groupPath]
	switch {
	case want == nil && !held:
		return false, nil
	case want != nil && held && current == *want:
		return false, nil
	case want == nil:
		delete(next.GroupPermissions, groupPath)
	default:
		next.GroupPermissions[groupPath] = *want
	}

	// The owner has to keep administrate, or permissions-v2 rejects the whole
	// write: a resource with no administrating user is invalid by its model.
	owner := next.UserPermissions[this.serviceUserId]
	if !owner.Administrate {
		next.UserPermissions[this.serviceUserId] = permissions.PermissionsMap{
			Read: true, Write: true, Execute: true, Administrate: true,
		}
	}
	if !next.Valid() {
		return false, fmt.Errorf("graph %v would be left without an administrating user", graphId)
	}

	if !this.writable {
		return false, ErrReadOnly
	}
	if _, err, code = this.permissions.SetPermission(this.token, this.graphTopic, graphId, next); err != nil {
		return false, fmt.Errorf("unable to write permissions of graph %v (%v): %w", graphId, code, err)
	}
	return true, nil
}

func clonePermissions(source permissions.ResourcePermissions) permissions.ResourcePermissions {
	result := permissions.ResourcePermissions{
		UserPermissions:  map[string]permissions.PermissionsMap{},
		GroupPermissions: map[string]permissions.PermissionsMap{},
		RolePermissions:  map[string]permissions.PermissionsMap{},
	}
	for key, value := range source.UserPermissions {
		result.UserPermissions[key] = value
	}
	for key, value := range source.GroupPermissions {
		result.GroupPermissions[key] = value
	}
	for key, value := range source.RolePermissions {
		result.RolePermissions[key] = value
	}
	return result
}

// GroupIdOf is the Keycloak group id a graph is the default graph of, or "".
//
// The identity, because it survives a rename. See model.AttrKeycloakGroupId.
func GroupIdOf(graph platform.Graph) string {
	return AttributeOf(graph.Attributes, model.AttrKeycloakGroupId)
}

// GroupPathOf is the group path a graph is currently shared with, or "".
func GroupPathOf(graph platform.Graph) string {
	return AttributeOf(graph.Attributes, model.AttrKeycloakGroup)
}

// GeneratedNameOf is the root name this service last wrote onto a graph.
//
// The one piece of history the service needs about its own work: comparing it
// to the root's current name is how a group rename is distinguished from a
// name a user chose. Provenance travels with the object rather than sitting in
// a store this service would have to keep in step.
func GeneratedNameOf(graph platform.Graph) string {
	return AttributeOf(graph.Attributes, model.AttrName)
}

// AttributeOf returns the value of the named attribute, or "".
func AttributeOf(attributes []platform.Attribute, key string) string {
	for _, attribute := range attributes {
		if attribute.Key == key {
			return attribute.Value
		}
	}
	return ""
}

// SetAttribute writes an attribute, replacing any existing one with that key.
func SetAttribute(attributes []platform.Attribute, key string, value string) []platform.Attribute {
	result := slices.DeleteFunc(slices.Clone(attributes), func(attribute platform.Attribute) bool {
		return attribute.Key == key
	})
	return append(result, platform.Attribute{Key: key, Value: value, Origin: model.AttrOrigin})
}

// RootOf is the node nothing points up from - the top of the graph.
//
// Found by structure rather than by id: a user may have rebuilt the graph, and
// the graph view itself identifies the root this way.
func RootOf(graph platform.Graph) (platform.Node, bool) {
	for _, node := range graph.Nodes {
		isSource := true
		for _, edge := range graph.Edges {
			if edge.FromNodeId == node.Id {
				isSource = false
				break
			}
		}
		if isSource {
			return node, true
		}
	}
	return platform.Node{}, false
}

// DeviceIdsIn is every device a graph contains, by the id it points at.
func DeviceIdsIn(graph platform.Graph) []string {
	result := []string{}
	for _, node := range graph.Nodes {
		if node.ResourceType == platform.GraphResourceTypeDevice && node.ResourceId != "" {
			result = append(result, node.ResourceId)
		}
	}
	slices.Sort(result)
	return result
}
