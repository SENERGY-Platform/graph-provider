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

package reconcile

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"testing"
	"time"

	devicerepomodel "github.com/SENERGY-Platform/device-repository/lib/model"
	"github.com/SENERGY-Platform/graph-provider/pkg/cache"
	"github.com/SENERGY-Platform/graph-provider/pkg/events"
	"github.com/SENERGY-Platform/graph-provider/pkg/graphs"
	"github.com/SENERGY-Platform/graph-provider/pkg/model"
	platform "github.com/SENERGY-Platform/models/go/models"
	permissions "github.com/SENERGY-Platform/permissions-v2/pkg/client"
)

const (
	deviceTopic = "devices"
	graphTopic  = "graphs"
	serviceUser = "service-user"
	token       = "token"
)

// fixedNow is the clock the reconciler is given. Every window the heuristic
// asks about is derived from it, so the reading window of a pass is a value
// the test can name.
var fixedNow = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

// referenceWindow is the default of SPEC.md's reference_window.
const referenceWindow = 720 * time.Hour

// expectedWindow is the window every device of one pass has to be asked about.
var expectedWindow = model.Window{Start: fixedNow.Add(-referenceWindow), End: fixedNow}

// world is the state the four foreign systems hold, in one place.
//
// One struct rather than four, because the systems are not independent: the
// device repository creating a graph is what makes a permission resource for
// it exist, and that coupling is exactly what the reconciler's write ordering
// depends on. Guarded by a mutex because the loop test drives it from another
// goroutine.
type world struct {
	mu sync.Mutex

	// calls is every fake call in the order it happened, by name. The order of
	// the writes is a claim of its own, so it has to be observable.
	calls []string

	devices      map[string]platform.ExtendedDevice
	deviceGroups map[string][]string // device id -> group paths holding read
	groups       []model.Group

	graphs     map[string]platform.Graph
	graphOrder []string
	nextGraph  int

	// resources is keyed topic -> id. Device resources are derived from
	// deviceGroups; graph resources are created by SetGraph.
	resources map[string]map[string]permissions.Resource

	// listedDeviceIds records the id filter of every single-device lookup, so
	// a fan-out can be checked by what was asked rather than by its effect.
	listedDeviceIds [][]string

	readingValues map[model.ReadingKey]float64
	fetchWindows  []model.Window
	fetchDevices  [][]string
}

func newWorld() *world {
	return &world{
		devices:       map[string]platform.ExtendedDevice{},
		deviceGroups:  map[string][]string{},
		graphs:        map[string]platform.Graph{},
		resources:     map[string]map[string]permissions.Resource{deviceTopic: {}, graphTopic: {}},
		readingValues: map[model.ReadingKey]float64{},
	}
}

func (this *world) record(name string) {
	this.calls = append(this.calls, name)
}

// resetCalls forgets the call log. A pass writing nothing is asserted by
// counting calls, so every scenario needs a point to count from.
func (this *world) resetCalls() {
	this.mu.Lock()
	defer this.mu.Unlock()
	this.calls = nil
	this.listedDeviceIds = nil
	this.fetchWindows = nil
	this.fetchDevices = nil
}

func (this *world) countCalls(name string) int {
	this.mu.Lock()
	defer this.mu.Unlock()
	count := 0
	for _, call := range this.calls {
		if call == name {
			count++
		}
	}
	return count
}

func (this *world) callOrder() []string {
	this.mu.Lock()
	defer this.mu.Unlock()
	return append([]string{}, this.calls...)
}

func (this *world) setGroups(paths ...string) {
	this.mu.Lock()
	defer this.mu.Unlock()
	this.groups = nil
	for _, path := range paths {
		this.groups = append(this.groups, model.Group{Id: "kc" + path, Path: path, Name: model.GroupName(path)})
	}
}

// setGroupsWithIds sets the group tree from an explicit path-to-id mapping.
//
// What a rename looks like from the admin API: the same group id under a new
// path. setGroups derives the id from the path and so cannot express it.
func (this *world) setGroupsWithIds(byPath map[string]string) {
	this.mu.Lock()
	defer this.mu.Unlock()
	this.groups = nil
	paths := []string{}
	for path := range byPath {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		this.groups = append(this.groups, model.Group{
			Id:   byPath[path],
			Path: path,
			Name: model.GroupName(path),
		})
	}
}

// addDevice adds a device to the repository and its group rights to
// permissions-v2, which is the pair the cache assembles a device from.
func (this *world) addDevice(id string, name string, deviceTypeId string, deviceType *platform.DeviceType, groups ...string) {
	this.mu.Lock()
	defer this.mu.Unlock()
	this.devices[id] = platform.ExtendedDevice{
		Device:      platform.Device{Id: id, Name: name, DeviceTypeId: deviceTypeId},
		DisplayName: name,
		DeviceType:  deviceType,
	}
	this.deviceGroups[id] = append([]string{}, groups...)
}

func (this *world) setDeviceGroups(id string, groups ...string) {
	this.mu.Lock()
	defer this.mu.Unlock()
	this.deviceGroups[id] = append([]string{}, groups...)
}

// removeDevice is a device the repository no longer returns.
func (this *world) removeDevice(id string) {
	this.mu.Lock()
	defer this.mu.Unlock()
	delete(this.devices, id)
	delete(this.deviceGroups, id)
}

func (this *world) setReading(deviceId string, carrier model.Carrier, value float64) {
	this.mu.Lock()
	defer this.mu.Unlock()
	this.readingValues[model.ReadingKey{DeviceId: deviceId, Carrier: carrier}] = value
}

// putGraph seeds a graph as if it had been written earlier, optionally already
// shared with the given group paths.
func (this *world) putGraph(graph platform.Graph, shareWith ...string) platform.Graph {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.storeGraph(graph, shareWith...)
}

func (this *world) storeGraph(graph platform.Graph, shareWith ...string) platform.Graph {
	if graph.Id == "" {
		this.nextGraph++
		graph.Id = fmt.Sprintf("g%v", this.nextGraph)
	}
	if _, known := this.graphs[graph.Id]; !known {
		this.graphOrder = append(this.graphOrder, graph.Id)
	}
	this.graphs[graph.Id] = cloneGraph(graph)

	// The repository creates a permission resource for a graph it did not know
	// yet, with the graph's owner as its administrator. Share fails without
	// one, so the fake has to behave the same way or the ordering the
	// reconciler depends on would be untestable.
	if _, known := this.resources[graphTopic][graph.Id]; !known {
		resource := permissions.Resource{
			Id:      graph.Id,
			TopicId: graphTopic,
			ResourcePermissions: permissions.ResourcePermissions{
				UserPermissions: map[string]permissions.PermissionsMap{
					graph.Owner: {Read: true, Write: true, Execute: true, Administrate: true},
				},
				GroupPermissions: map[string]permissions.PermissionsMap{},
				RolePermissions:  map[string]permissions.PermissionsMap{},
			},
		}
		for _, path := range shareWith {
			resource.GroupPermissions[path] = permissions.PermissionsMap{Read: true, Write: true, Execute: true}
		}
		this.resources[graphTopic][graph.Id] = resource
	}
	return cloneGraph(graph)
}

func (this *world) graph(id string) (platform.Graph, bool) {
	this.mu.Lock()
	defer this.mu.Unlock()
	graph, ok := this.graphs[id]
	return cloneGraph(graph), ok
}

// graphOfGroup is the stored graph carrying a group's attribute.
func (this *world) graphOfGroup(path string) (platform.Graph, bool) {
	this.mu.Lock()
	defer this.mu.Unlock()
	for _, id := range this.graphOrder {
		graph := this.graphs[id]
		if graphs.GroupPathOf(graph) == path {
			return cloneGraph(graph), true
		}
	}
	return platform.Graph{}, false
}

func (this *world) graphCount() int {
	this.mu.Lock()
	defer this.mu.Unlock()
	return len(this.graphs)
}

func (this *world) groupPermission(graphId string, path string) (permissions.PermissionsMap, bool) {
	this.mu.Lock()
	defer this.mu.Unlock()
	resource := this.resources[graphTopic][graphId]
	perm, ok := resource.GroupPermissions[path]
	return perm, ok
}

func (this *world) userPermission(graphId string, userId string) (permissions.PermissionsMap, bool) {
	this.mu.Lock()
	defer this.mu.Unlock()
	resource := this.resources[graphTopic][graphId]
	perm, ok := resource.UserPermissions[userId]
	return perm, ok
}

// listedIds is the id filter of every single-device lookup since the last reset.
func (this *world) listedIds() [][]string {
	this.mu.Lock()
	defer this.mu.Unlock()
	return append([][]string{}, this.listedDeviceIds...)
}

// windows is every window the consumption fetcher was asked about.
func (this *world) windows() []model.Window {
	this.mu.Lock()
	defer this.mu.Unlock()
	return append([]model.Window{}, this.fetchWindows...)
}

// askedDevices is every device the consumption fetcher was asked about.
func (this *world) askedDevices() map[string]bool {
	this.mu.Lock()
	defer this.mu.Unlock()
	result := map[string]bool{}
	for _, devices := range this.fetchDevices {
		for _, id := range devices {
			result[id] = true
		}
	}
	return result
}

// dropGroupPermission is somebody taking the group's rights away behind the
// service's back.
func (this *world) dropGroupPermission(graphId string, path string) {
	this.mu.Lock()
	defer this.mu.Unlock()
	resource := this.resources[graphTopic][graphId]
	delete(resource.GroupPermissions, path)
	this.resources[graphTopic][graphId] = resource
}

// deviceResource builds a device's permission resource from its group rights.
func (this *world) deviceResource(id string) (permissions.Resource, bool) {
	groups, known := this.deviceGroups[id]
	if !known {
		return permissions.Resource{}, false
	}
	perms := map[string]permissions.PermissionsMap{}
	for _, path := range groups {
		perms[path] = permissions.PermissionsMap{Read: true, Write: true, Execute: true}
	}
	return permissions.Resource{
		Id:                  id,
		TopicId:             deviceTopic,
		ResourcePermissions: permissions.ResourcePermissions{GroupPermissions: perms},
	}, true
}

func (this *world) deviceIds() []string {
	result := make([]string, 0, len(this.devices))
	for id := range this.devices {
		result = append(result, id)
	}
	sort.Strings(result)
	return result
}

// fakeRepository serves both cache.DeviceRepository and graphs.Repository:
// both are the device repository, and a test that split them would be able to
// let them disagree about a graph the other just wrote.
type fakeRepository struct {
	w *world
}

func (this *fakeRepository) ListExtendedDevices(_ string, options devicerepomodel.ExtendedDeviceListOptions) ([]platform.ExtendedDevice, int64, error, int) {
	this.w.mu.Lock()
	defer this.w.mu.Unlock()
	this.w.record("ListExtendedDevices")

	if options.Ids != nil {
		this.w.listedDeviceIds = append(this.w.listedDeviceIds, append([]string{}, options.Ids...))
		result := []platform.ExtendedDevice{}
		for _, id := range options.Ids {
			if device, known := this.w.devices[id]; known {
				result = append(result, device)
			}
		}
		return result, int64(len(result)), nil, http.StatusOK
	}

	all := []platform.ExtendedDevice{}
	for _, id := range this.w.deviceIds() {
		all = append(all, this.w.devices[id])
	}
	return page(all, options.Offset, options.Limit), int64(len(all)), nil, http.StatusOK
}

func (this *fakeRepository) ListGraphs(_ string, options devicerepomodel.GraphListOptions) ([]platform.Graph, int64, error, int) {
	this.w.mu.Lock()
	defer this.w.mu.Unlock()
	this.w.record("ListGraphs")

	matching := []platform.Graph{}
	for _, id := range this.w.graphOrder {
		graph := this.w.graphs[id]
		if options.Ids != nil {
			for _, wanted := range options.Ids {
				if graph.Id == wanted {
					matching = append(matching, cloneGraph(graph))
				}
			}
			continue
		}
		// The repository requires ALL of the listed keys, not any of them:
		// it builds one $elemMatch per listed attribute and ANDs them, which
		// pkg/tests established against the real thing and which is why
		// graphs.Defaults asks once per key. The parameter's own
		// documentation says otherwise ("lists elements only if they have an
		// attribute that is in the given list"), and believing it here was
		// what hid a graph the service must find.
		matches := true
		for _, attribute := range options.Attributes {
			if graphs.AttributeOf(graph.Attributes, attribute.Key) == "" {
				matches = false
				break
			}
		}
		if matches {
			matching = append(matching, cloneGraph(graph))
		}
	}
	if options.Ids != nil {
		return matching, int64(len(matching)), nil, http.StatusOK
	}
	return page(matching, options.Offset, options.Limit), int64(len(matching)), nil, http.StatusOK
}

func (this *fakeRepository) SetGraph(_ string, graph platform.Graph) (platform.Graph, error, int) {
	this.w.mu.Lock()
	defer this.w.mu.Unlock()
	this.w.record("SetGraph")
	if err := graph.Valid(); err != nil {
		// The repository answers an invalid graph with a 400 rather than
		// storing it, and a test that stored it would hide the bug.
		return graph, fmt.Errorf("invalid graph: %w", err), http.StatusBadRequest
	}
	return this.w.storeGraph(graph), nil, http.StatusOK
}

// fakePermissions serves both cache.Permissions and graphs.Permissions.
type fakePermissions struct {
	w *world
}

func (this *fakePermissions) ListResourcesWithAdminPermission(_ string, topicId string, options permissions.ListOptions) ([]permissions.Resource, error, int) {
	this.w.mu.Lock()
	defer this.w.mu.Unlock()
	this.w.record("ListResourcesWithAdminPermission")

	all := []permissions.Resource{}
	if topicId == deviceTopic {
		for _, id := range this.w.deviceIds() {
			if resource, ok := this.w.deviceResource(id); ok {
				all = append(all, resource)
			}
		}
	}
	return page(all, options.Offset, options.Limit), nil, http.StatusOK
}

func (this *fakePermissions) GetResource(_ string, topicId string, id string) (permissions.Resource, error, int) {
	this.w.mu.Lock()
	defer this.w.mu.Unlock()
	this.w.record("GetResource")

	if topicId == deviceTopic {
		if resource, ok := this.w.deviceResource(id); ok {
			return resource, nil, http.StatusOK
		}
		return permissions.Resource{}, errors.New("not found"), http.StatusNotFound
	}
	if resource, ok := this.w.resources[topicId][id]; ok {
		return resource, nil, http.StatusOK
	}
	return permissions.Resource{}, errors.New("not found"), http.StatusNotFound
}

func (this *fakePermissions) SetPermission(_ string, topicId string, id string, perms permissions.ResourcePermissions) (permissions.ResourcePermissions, error, int) {
	this.w.mu.Lock()
	defer this.w.mu.Unlock()
	this.w.record("SetPermission")

	if _, ok := this.w.resources[topicId]; !ok {
		this.w.resources[topicId] = map[string]permissions.Resource{}
	}
	resource := this.w.resources[topicId][id]
	resource.Id = id
	resource.TopicId = topicId
	resource.ResourcePermissions = clonePermissions(perms)
	this.w.resources[topicId][id] = resource
	return perms, nil, http.StatusOK
}

// fakeGroupSource is the Keycloak group tree.
type fakeGroupSource struct {
	w *world
}

func (this *fakeGroupSource) Groups(context.Context) ([]model.Group, error) {
	this.w.mu.Lock()
	defer this.w.mu.Unlock()
	this.w.record("Groups")
	return append([]model.Group{}, this.w.groups...), nil
}

// fakeReadings is the consumption fetcher.
type fakeReadings struct {
	w *world
}

func (this *fakeReadings) Fetch(_ context.Context, devices []model.Device, window model.Window) (map[model.ReadingKey]model.Reading, error) {
	this.w.mu.Lock()
	defer this.w.mu.Unlock()
	this.w.record("Fetch")
	this.w.fetchWindows = append(this.w.fetchWindows, window)

	asked := []string{}
	result := map[model.ReadingKey]model.Reading{}
	for _, device := range devices {
		asked = append(asked, device.Id)
		for _, carrier := range device.CarriersOf() {
			key := model.ReadingKey{DeviceId: device.Id, Carrier: carrier}
			value, known := this.w.readingValues[key]
			if !known {
				// Absent is not zero: a meter that never reported is a missing
				// entry, which is what makes the device hang off the root.
				continue
			}
			result[key] = model.Reading{
				DeviceId: device.Id,
				Carrier:  carrier,
				Value:    value,
				Window:   window,
				// Real time, not the injected clock: the cache's ttl is
				// checked against its own wall clock.
				FetchedAt: time.Now(),
			}
		}
	}
	sort.Strings(asked)
	this.w.fetchDevices = append(this.w.fetchDevices, asked)
	return result, nil
}

// harness wires the real cache, store, pending set and reconciler over the
// fakes, so a test exercises the actual composition rather than a mock of it.
type harness struct {
	world   *world
	cache   *cache.Cache
	store   *graphs.Store
	pending *events.Pending
	rec     *Reconciler
}

type harnessOptions struct {
	writable   bool
	allowGroup func(string) bool
	config     Config
}

func writeDisabled(options *harnessOptions) {
	options.writable = false
}

func rejectGroups(rejected ...string) func(*harnessOptions) {
	return func(options *harnessOptions) {
		options.allowGroup = func(path string) bool {
			for _, reject := range rejected {
				if path == reject {
					return false
				}
			}
			return true
		}
	}
}

// collectPlanned records every change a pass reports through the hook.
func collectPlanned(into *[]model.PlannedChange) func(*harnessOptions) {
	return func(options *harnessOptions) {
		options.config.Planned = func(change model.PlannedChange) {
			*into = append(*into, change)
		}
	}
}

func intervals(groupPoll time.Duration, reconcile time.Duration) func(*harnessOptions) {
	return func(options *harnessOptions) {
		options.config.GroupPollInterval = groupPoll
		options.config.ReconcileInterval = reconcile
	}
}

func newHarness(t *testing.T, tune ...func(*harnessOptions)) *harness {
	t.Helper()
	options := harnessOptions{
		writable: true,
		config: Config{
			ServiceUserId: serviceUser,
			Window:        referenceWindow,
			Tolerance:     0.05,
			ReadingTtl:    24 * time.Hour,
			// Long by default: a test that means to exercise a ticker says so.
			GroupPollInterval: time.Hour,
			ReconcileInterval: time.Hour,
		},
	}
	for _, apply := range tune {
		apply(&options)
	}

	w := newWorld()
	repository := &fakeRepository{w: w}
	perms := &fakePermissions{w: w}

	c := cache.New(repository, perms, &fakeGroupSource{w: w}, token, deviceTopic, options.allowGroup)
	store := graphs.New(repository, perms, token, graphTopic, serviceUser, options.writable)
	pending := events.NewPending()

	// io.Discard rather than t.Log: the loop test keeps a goroutine running
	// while the test tears down, and logging into a finished test panics.
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	rec := New(c, store, &fakeReadings{w: w}, pending, options.config, logger)
	// The reading window has to be a value the test can name. Same package,
	// so the clock can simply be replaced.
	rec.now = func() time.Time { return fixedNow }

	return &harness{world: w, cache: c, store: store, pending: pending, rec: rec}
}

func (this *harness) mustFull(t *testing.T) {
	t.Helper()
	if err := this.rec.Full(context.Background()); err != nil {
		t.Fatalf("full pass failed: %v", err)
	}
}

// graphOfGroup fails the test when a group has no default graph.
func (this *harness) graphOfGroup(t *testing.T, path string) platform.Graph {
	t.Helper()
	graph, ok := this.world.graphOfGroup(path)
	if !ok {
		t.Fatalf("no default graph for group %v", path)
	}
	return graph
}

// --- graph inspection helpers -------------------------------------------------

// parentOf is the node id a device's node points at, or "" when the device is
// not in the graph. Structure, not edge ids: a detach reroutes through the
// model's own uuid provider, so no edge id survives one.
func parentOf(graph platform.Graph, deviceId string) string {
	nodeId := ""
	for _, node := range graph.Nodes {
		if node.ResourceId == deviceId {
			nodeId = node.Id
			break
		}
	}
	if nodeId == "" {
		return ""
	}
	for _, edge := range graph.Edges {
		if edge.FromNodeId == nodeId {
			return edge.ToNodeId
		}
	}
	return ""
}

func edgeById(graph platform.Graph, id string) (platform.Edge, bool) {
	for _, edge := range graph.Edges {
		if edge.Id == id {
			return edge, true
		}
	}
	return platform.Edge{}, false
}

func nodeIds(graph platform.Graph) []string {
	result := []string{}
	for _, node := range graph.Nodes {
		result = append(result, node.Id)
	}
	sort.Strings(result)
	return result
}

func rootName(t *testing.T, graph platform.Graph) string {
	t.Helper()
	root, found := graphs.RootOf(graph)
	if !found {
		t.Fatalf("graph %v has no root", graph.Id)
	}
	return graphs.AttributeOf(root.Attributes, model.NodeAttrName)
}

// --- fixtures ----------------------------------------------------------------

// electricityType is a device type whose output carries the electricity
// measuring function, which is the only way a carrier is identifiable.
func electricityType(id string) *platform.DeviceType {
	return &platform.DeviceType{
		Id: id,
		Services: []platform.Service{{
			Id: id + ":svc",
			Outputs: []platform.Content{{
				ContentVariable: platform.ContentVariable{
					Name: "reading",
					SubContentVariables: []platform.ContentVariable{{
						Name:       "energy",
						FunctionId: model.CarrierFunctionId[model.Electricity],
					}},
				},
			}},
		}},
	}
}

// meter adds an electricity meter with a reading to the world.
func (this *world) meter(id string, name string, value float64, groups ...string) {
	this.addDevice(id, name, "dt1", electricityType("dt1"), groups...)
	this.setReading(id, model.Electricity, value)
}

// seededGraph is a graph as an earlier run would have left it: a custom root
// carrying the display name, one node per device, every device on the root.
func seededGraph(groupPath string, recordedName string, rootName string, deviceIds ...string) platform.Graph {
	graph := platform.Graph{
		Owner: serviceUser,
		Attributes: []platform.Attribute{
			// The group id is the identity, matching what setGroups hands out.
			// A graph without it is the legacy shape and gets adopted - see
			// TestAnIdlessGraphIsAdoptedOnce.
			{Key: model.AttrKeycloakGroupId, Value: "kc" + groupPath, Origin: model.AttrOrigin},
			{Key: model.AttrKeycloakGroup, Value: groupPath, Origin: model.AttrOrigin},
			{Key: model.AttrName, Value: recordedName, Origin: model.AttrOrigin},
		},
		Nodes: []platform.Node{{
			Id:           model.RootNodeId,
			ResourceType: model.ResourceTypeCustom,
			Attributes:   []platform.Attribute{{Key: model.NodeAttrName, Value: rootName, Origin: model.AttrOrigin}},
		}},
		Edges: []platform.Edge{},
	}
	for _, id := range deviceIds {
		graph.Nodes = append(graph.Nodes, platform.Node{
			Id:           id,
			ResourceId:   id,
			ResourceType: platform.GraphResourceTypeDevice,
			Attributes:   []platform.Attribute{},
		})
		graph.Edges = append(graph.Edges, platform.Edge{
			Id:         "edge-" + id + "-" + model.RootNodeId,
			FromNodeId: id,
			ToNodeId:   model.RootNodeId,
			Weight:     model.FullWeight,
			Attributes: []platform.Attribute{},
		})
	}
	return graph
}

// --- plumbing ----------------------------------------------------------------

// page mimics the offset/limit paging of both clients.
func page[T any](all []T, offset int64, limit int64) []T {
	start := int(offset)
	if start > len(all) {
		start = len(all)
	}
	end := start + int(limit)
	if end > len(all) {
		end = len(all)
	}
	return all[start:end]
}

// cloneGraph copies a graph down to its attribute slices, the way a JSON round
// trip through the repository does. Without it a fake would hand out the very
// slices the reconciler mutates while trying a change out, and a write that
// was skipped would still show up in the store.
func cloneGraph(graph platform.Graph) platform.Graph {
	result := graph
	result.Attributes = cloneAttributes(graph.Attributes)
	result.Nodes = make([]platform.Node, len(graph.Nodes))
	for i, node := range graph.Nodes {
		node.Attributes = cloneAttributes(node.Attributes)
		result.Nodes[i] = node
	}
	result.Edges = make([]platform.Edge, len(graph.Edges))
	for i, edge := range graph.Edges {
		edge.Attributes = cloneAttributes(edge.Attributes)
		result.Edges[i] = edge
	}
	return result
}

func cloneAttributes(attributes []platform.Attribute) []platform.Attribute {
	if attributes == nil {
		return nil
	}
	result := make([]platform.Attribute, len(attributes))
	copy(result, attributes)
	return result
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

// waitFor polls a condition until it holds or the deadline passes. Sleeping a
// fixed time would either be slow or flaky; a condition is neither.
func waitFor(t *testing.T, timeout time.Duration, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if condition() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %v", timeout, what)
		}
		time.Sleep(time.Millisecond)
	}
}
