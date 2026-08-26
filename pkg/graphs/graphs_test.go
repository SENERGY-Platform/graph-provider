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

package graphs

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"slices"
	"strconv"
	"testing"

	devicerepomodel "github.com/SENERGY-Platform/device-repository/lib/model"
	"github.com/SENERGY-Platform/graph-provider/pkg/model"
	platform "github.com/SENERGY-Platform/models/go/models"
	permissions "github.com/SENERGY-Platform/permissions-v2/pkg/client"
)

const (
	graphTopic = "graphs"
	serviceUsr = "service-user"
)

type fakeRepository struct {
	graphs []platform.Graph
	calls  []devicerepomodel.GraphListOptions
	saved  []platform.Graph
	err    error
}

func (this *fakeRepository) ListGraphs(_ string, options devicerepomodel.GraphListOptions) ([]platform.Graph, int64, error, int) {
	this.calls = append(this.calls, options)
	if this.err != nil {
		return nil, 0, this.err, 500
	}
	matching := []platform.Graph{}
	for _, graph := range this.graphs {
		if options.Ids != nil {
			for _, id := range options.Ids {
				if graph.Id == id {
					matching = append(matching, graph)
				}
			}
			continue
		}
		if !hasEveryAttributeKey(graph, options.Attributes) {
			continue
		}
		matching = append(matching, graph)
	}
	if options.Ids != nil {
		return matching, int64(len(matching)), nil, 200
	}
	start := int(options.Offset)
	if start > len(matching) {
		start = len(matching)
	}
	end := start + int(options.Limit)
	if end > len(matching) {
		end = len(matching)
	}
	return matching[start:end], int64(len(matching)), nil, 200
}

func (this *fakeRepository) SetGraph(_ string, graph platform.Graph) (platform.Graph, error, int) {
	if this.err != nil {
		return graph, this.err, 500
	}
	this.saved = append(this.saved, graph)
	return graph, nil, 200
}

// hasEveryAttributeKey mirrors the repository's plain attributes filter: it
// builds one $elemMatch per listed attribute and ANDs them, so an element has
// to carry ALL of the listed keys - not any of them, as the parameter's own
// documentation suggests. Established against the real repository in
// pkg/tests, and the reason Defaults asks once per key.
func hasEveryAttributeKey(graph platform.Graph, wanted []platform.Attribute) bool {
	for _, attribute := range wanted {
		if AttributeOf(graph.Attributes, attribute.Key) == "" {
			return false
		}
	}
	return true
}

type fakePermissions struct {
	resources map[string]permissions.Resource
	writes    []permissions.ResourcePermissions
	err       error
}

func (this *fakePermissions) GetResource(_ string, _ string, id string) (permissions.Resource, error, int) {
	resource, ok := this.resources[id]
	if !ok {
		return permissions.Resource{}, errors.New("not found"), http.StatusNotFound
	}
	return resource, nil, http.StatusOK
}

func (this *fakePermissions) SetPermission(_ string, _ string, id string, perms permissions.ResourcePermissions) (permissions.ResourcePermissions, error, int) {
	if this.err != nil {
		return perms, this.err, 500
	}
	this.writes = append(this.writes, perms)
	resource := this.resources[id]
	resource.Id = id
	resource.ResourcePermissions = perms
	this.resources[id] = resource
	return perms, nil, http.StatusOK
}

func ownedResource(id string) permissions.Resource {
	return permissions.Resource{
		Id:      id,
		TopicId: graphTopic,
		ResourcePermissions: permissions.ResourcePermissions{
			UserPermissions: map[string]permissions.PermissionsMap{
				serviceUsr: {Read: true, Write: true, Execute: true, Administrate: true},
			},
			GroupPermissions: map[string]permissions.PermissionsMap{},
			RolePermissions:  map[string]permissions.PermissionsMap{},
		},
	}
}

// validGraph is the smallest graph the platform model accepts: one custom root
// and nothing else.
func validGraph(id string, groupPath string, name string) platform.Graph {
	return platform.Graph{
		Id:    id,
		Owner: serviceUsr,
		Attributes: []platform.Attribute{
			{Key: model.AttrKeycloakGroupId, Value: "kc" + groupPath, Origin: model.AttrOrigin},
			{Key: model.AttrKeycloakGroup, Value: groupPath, Origin: model.AttrOrigin},
			{Key: model.AttrName, Value: name, Origin: model.AttrOrigin},
		},
		Nodes: []platform.Node{{
			Id:           model.RootNodeId,
			ResourceType: model.ResourceTypeCustom,
			Attributes:   []platform.Attribute{{Key: model.NodeAttrName, Value: name, Origin: model.AttrOrigin}},
		}},
		Edges: []platform.Edge{},
	}
}

func TestDefaultsKeysByGroupAndFiltersByKeyOnly(t *testing.T) {
	repository := &fakeRepository{graphs: []platform.Graph{
		validGraph("g1", "/acme", "acme"),
		validGraph("g2", "/beta", "beta"),
		// A graph a user made: no group attribute, so not ours.
		{Id: "g3", Owner: "someone", Nodes: []platform.Node{{Id: "root", ResourceType: model.ResourceTypeCustom}}},
	}}
	store := New(repository, &fakePermissions{}, "token", graphTopic, serviceUsr, true)

	found, err := store.Defaults(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(found.ById) != 2 || len(found.ByPath) != 0 {
		t.Fatalf("expected two graphs indexed by id, got %v by id and %v by path",
			len(found.ById), len(found.ByPath))
	}
	if found.ById["kc/acme"].Id != "g1" || found.ById["kc/beta"].Id != "g2" {
		t.Errorf("graphs keyed by the wrong group: %+v", found.ById)
	}

	// One call per provenance key, each naming exactly one. The repository
	// ANDs a multi-key filter, so a single call for both would hide the graph
	// that carries only the path.
	if len(repository.calls) != 2 {
		t.Fatalf("expected one call per provenance key, got %v", len(repository.calls))
	}
	asked := []string{}
	for _, call := range repository.calls {
		if len(call.Attributes) != 1 {
			t.Fatalf("expected exactly one key per filter, got %+v", call.Attributes)
		}
		asked = append(asked, call.Attributes[0].Key)
		// A value would have to travel through attributes_json, which the
		// repository client double-escapes into a 400.
		if call.Attributes[0].Value != "" || call.Attributes[0].Origin != "" {
			t.Errorf("value and origin must stay empty so the client uses the plain attributes parameter, got %+v", call.Attributes[0])
		}
		// Ids nil is what earns the admin bypass in the repository.
		if call.Ids != nil {
			t.Errorf("expected no id filter, got %+v", call.Ids)
		}
	}
	slices.Sort(asked)
	want := []string{model.AttrKeycloakGroup, model.AttrKeycloakGroupId}
	slices.Sort(want)
	if !slices.Equal(asked, want) {
		t.Errorf("filtered on %v, want one call per provenance key %v", asked, want)
	}
}

func TestDefaultsPagesAndKeepsTheLowerIdOnDuplicates(t *testing.T) {
	repository := &fakeRepository{}
	for i := 0; i < pageSize+3; i++ {
		repository.graphs = append(repository.graphs, validGraph("g"+strconv.Itoa(i), "/group"+strconv.Itoa(i), "n"))
	}
	// Two graphs claiming one group - a state this service cannot produce, so
	// the choice has to be stable rather than dependent on paging order.
	repository.graphs = append(repository.graphs,
		validGraph("zzz", "/contested", "later"),
		validGraph("aaa", "/contested", "earlier"),
	)
	store := New(repository, &fakePermissions{}, "token", graphTopic, serviceUsr, true)

	found, err := store.Defaults(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// Two pages per provenance key: one full, one short.
	if len(repository.calls) != 4 {
		t.Errorf("expected two pages per provenance key, got %v calls", len(repository.calls))
	}
	if got := found.ById["kc/contested"].Id; got != "aaa" {
		t.Errorf("expected the lower id to win, got %v", got)
	}
}

func TestSaveRefusesAnInvalidGraph(t *testing.T) {
	repository := &fakeRepository{}
	store := New(repository, &fakePermissions{}, "token", graphTopic, serviceUsr, true)

	// Two nodes without outputs: the model demands exactly one.
	broken := validGraph("", "/acme", "acme")
	broken.Nodes = append(broken.Nodes, platform.Node{Id: "second", ResourceType: model.ResourceTypeCustom})

	if _, err := store.Save(context.Background(), broken); err == nil {
		t.Error("an invalid graph must be refused locally")
	}
	if len(repository.saved) != 0 {
		t.Error("nothing may be sent when validation failed")
	}
}

func TestKillSwitchBlocksEveryWrite(t *testing.T) {
	repository := &fakeRepository{}
	perms := &fakePermissions{resources: map[string]permissions.Resource{"g1": ownedResource("g1")}}
	store := New(repository, perms, "token", graphTopic, serviceUsr, false)

	if _, err := store.Save(context.Background(), validGraph("g1", "/acme", "acme")); !errors.Is(err, ErrReadOnly) {
		t.Errorf("expected ErrReadOnly, got %v", err)
	}
	if _, err := store.Share(context.Background(), "g1", "/acme"); !errors.Is(err, ErrReadOnly) {
		t.Errorf("expected ErrReadOnly, got %v", err)
	}
	if len(repository.saved) != 0 || len(perms.writes) != 0 {
		t.Error("the kill switch must let nothing through")
	}
}

func TestShareGrantsReadWriteExecuteButNotAdministrate(t *testing.T) {
	perms := &fakePermissions{resources: map[string]permissions.Resource{"g1": ownedResource("g1")}}
	store := New(&fakeRepository{}, perms, "token", graphTopic, serviceUsr, true)

	changed, err := store.Share(context.Background(), "g1", "/acme")
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Error("the first share is a change")
	}
	if len(perms.writes) != 1 {
		t.Fatalf("expected one write, got %v", len(perms.writes))
	}
	got := perms.writes[0].GroupPermissions["/acme"]
	want := permissions.PermissionsMap{Read: true, Write: true, Execute: true}
	if got != want {
		t.Errorf("a group may edit but not delete or re-share: want %+v, got %+v", want, got)
	}
	if !perms.writes[0].UserPermissions[serviceUsr].Administrate {
		t.Error("the owner must keep administrate or permissions-v2 rejects the write")
	}
}

func TestShareIsSkippedWhenItWouldChangeNothing(t *testing.T) {
	resource := ownedResource("g1")
	resource.GroupPermissions["/acme"] = permissions.PermissionsMap{Read: true, Write: true, Execute: true}
	perms := &fakePermissions{resources: map[string]permissions.Resource{"g1": resource}}
	store := New(&fakeRepository{}, perms, "token", graphTopic, serviceUsr, true)

	changed, err := store.Share(context.Background(), "g1", "/acme")
	if err != nil {
		t.Fatal(err)
	}
	if changed {
		t.Error("nothing differed, so nothing should have been written")
	}
	// This is what stops the loop: every write echoes back as a rights
	// command this service consumes itself.
	if len(perms.writes) != 0 {
		t.Errorf("expected no write, got %v", len(perms.writes))
	}
}

func TestUnshareRemovesOnlyThatGroup(t *testing.T) {
	resource := ownedResource("g1")
	resource.GroupPermissions["/acme"] = permissions.PermissionsMap{Read: true, Write: true, Execute: true}
	resource.GroupPermissions["/beta"] = permissions.PermissionsMap{Read: true}
	perms := &fakePermissions{resources: map[string]permissions.Resource{"g1": resource}}
	store := New(&fakeRepository{}, perms, "token", graphTopic, serviceUsr, true)

	changed, err := store.Unshare(context.Background(), "g1", "/acme")
	if err != nil {
		t.Fatal(err)
	}
	if !changed || len(perms.writes) != 1 {
		t.Fatalf("expected one write, got changed=%v writes=%v", changed, len(perms.writes))
	}
	written := perms.writes[0]
	if _, still := written.GroupPermissions["/acme"]; still {
		t.Error("/acme should be gone")
	}
	if _, other := written.GroupPermissions["/beta"]; !other {
		t.Error("/beta must be left alone")
	}
	if !written.UserPermissions[serviceUsr].Administrate {
		t.Error("the owner must keep administrate")
	}

	// A second unshare has nothing left to do.
	changed, err = store.Unshare(context.Background(), "g1", "/acme")
	if err != nil {
		t.Fatal(err)
	}
	if changed || len(perms.writes) != 1 {
		t.Error("unsharing twice must be a no-op")
	}
}

func TestShareFailsWhenTheGraphHasNoPermissionResource(t *testing.T) {
	perms := &fakePermissions{resources: map[string]permissions.Resource{}}
	store := New(&fakeRepository{}, perms, "token", graphTopic, serviceUsr, true)

	if _, err := store.Share(context.Background(), "missing", "/acme"); err == nil {
		t.Error("a graph without a permission resource cannot be shared, and inventing an administrator would be worse")
	}
	if len(perms.writes) != 0 {
		t.Error("nothing may be written")
	}
}

func TestAttributeAndRootHelpers(t *testing.T) {
	graph := validGraph("g1", "/acme/werk-nord", "werk-nord")
	if got := GroupPathOf(graph); got != "/acme/werk-nord" {
		t.Errorf("group path: %v", got)
	}
	if got := GeneratedNameOf(graph); got != "werk-nord" {
		t.Errorf("generated name: %v", got)
	}
	root, ok := RootOf(graph)
	if !ok || root.Id != model.RootNodeId {
		t.Errorf("root: %+v ok=%v", root, ok)
	}

	graph.Attributes = SetAttribute(graph.Attributes, model.AttrName, "neuer name")
	if got := GeneratedNameOf(graph); got != "neuer name" {
		t.Errorf("expected the attribute to be replaced, got %v", got)
	}
	// Three: the group id, the path and the name. Replacing must not add a
	// fourth.
	if count := len(graph.Attributes); count != 3 {
		t.Errorf("replacing must not duplicate, got %v attributes", count)
	}
}

func TestRootOfAndDeviceIdsInOnARealTree(t *testing.T) {
	graph := validGraph("g1", "/acme", "acme")
	graph.Nodes = append(graph.Nodes,
		platform.Node{Id: "dev-a", ResourceType: platform.GraphResourceTypeDevice, ResourceId: "dev-a"},
		platform.Node{Id: "dev-b", ResourceType: platform.GraphResourceTypeDevice, ResourceId: "dev-b"},
	)
	graph.Edges = []platform.Edge{
		{Id: "edge-dev-b-dev-a", FromNodeId: "dev-b", ToNodeId: "dev-a", Weight: model.FullWeight},
		{Id: "edge-dev-a-root", FromNodeId: "dev-a", ToNodeId: model.RootNodeId, Weight: model.FullWeight},
	}
	if err := graph.Valid(); err != nil {
		t.Fatalf("test fixture is not a valid graph: %v", err)
	}

	root, ok := RootOf(graph)
	if !ok || root.Id != model.RootNodeId {
		t.Errorf("the root is the node nothing points up from, got %+v", root)
	}
	if got := DeviceIdsIn(graph); !reflect.DeepEqual(got, []string{"dev-a", "dev-b"}) {
		t.Errorf("device ids: %+v", got)
	}
}

func TestListErrorsAreReturned(t *testing.T) {
	store := New(&fakeRepository{err: errors.New("boom")}, &fakePermissions{}, "token", graphTopic, serviceUsr, true)
	if _, err := store.Defaults(context.Background()); err == nil {
		t.Error("a repository error must not be swallowed")
	}
}

// A graph carrying only the path attribute - written before the id was
// recorded, or a write interrupted between the two - is still recognised, so
// the caller can adopt it instead of creating a second graph beside it.
func TestDefaultsIndexesAnIdlessGraphByPath(t *testing.T) {
	legacy := validGraph("old", "/acme", "acme")
	legacy.Attributes = slices.DeleteFunc(legacy.Attributes, func(a platform.Attribute) bool {
		return a.Key == model.AttrKeycloakGroupId
	})
	repository := &fakeRepository{graphs: []platform.Graph{legacy}}
	store := New(repository, &fakePermissions{}, "token", graphTopic, serviceUsr, true)

	found, err := store.Defaults(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(found.ById) != 0 {
		t.Errorf("a graph without an id attribute cannot be indexed by id, got %+v", found.ById)
	}
	if found.ByPath["/acme"].Id != "old" {
		t.Errorf("expected the graph indexed by path, got %+v", found.ByPath)
	}
}
