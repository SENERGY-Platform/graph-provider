//go:build integration

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

// Package tests is the one place this service speaks to the real
// device-repository, the real permissions-v2 and a real Kafka.
//
// Everything else is tested against fakes, and a fake can only be as right as
// the belief that was written into it. What this suite is for is the beliefs
// themselves: that a plain attributes filter matches an element carrying ANY
// of the listed keys, that a graph written without an id comes back with a
// permission resource the caller administrates, that a short page is the last
// page. Each of those is asserted on its own and named in the failure, because
// the answer is a fact about a foreign API and the reader needs to know which
// fact moved.
//
// One suite, one stack, scenarios in order: containers cost minutes, and the
// scenarios are a story - a graph is created, a device leaves, a user edits -
// which is also how the service meets them.
package tests

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	devicerepoclient "github.com/SENERGY-Platform/device-repository/v3/lib/client"
	"github.com/SENERGY-Platform/graph-provider/pkg/carrier"
	"github.com/SENERGY-Platform/graph-provider/pkg/graphs"
	"github.com/SENERGY-Platform/graph-provider/pkg/model"
	"github.com/SENERGY-Platform/graph-provider/pkg/things"
	platform "github.com/SENERGY-Platform/models/go/models"
	permissions "github.com/SENERGY-Platform/permissions-v2/pkg/client"
)

func TestAgainstTheRealPlatform(t *testing.T) {
	world := start(t)
	deviceTypeId, meters := world.seed(t)
	main, sub, small := meters[0], meters[1], meters[2]
	t.Logf("devices: main=%v (%v) sub=%v (%v) small=%v (%v)",
		main.id, main.value, sub.id, sub.value, small.id, small.value)

	// The scenarios build on each other, so the first failure ends the run
	// rather than reporting a pile of consequences of it. The assumption
	// probes come last, are independent of each other, and each one is worth
	// an answer even when its neighbour broke - so a failure there does not
	// end the run.
	steps := []struct {
		name string
		// scenario marks a step later ones depend on.
		scenario bool
		run      func(*testing.T)
	}{
		{"a group's first graph", true, func(t *testing.T) {
			world.full(t)

			graph := world.defaultGraph(t)
			if err := graph.Valid(); err != nil {
				t.Fatalf("the repository handed back a graph its own model rejects: %v", err)
			}
			if graph.Owner != world.serviceUserId {
				t.Errorf("graph owner is %v, want the service user %v", graph.Owner, world.serviceUserId)
			}

			// Provenance: the group id is the identity and the path is what
			// sharing is keyed by. Both have to survive the round trip
			// through the repository or the next pass cannot recognise its
			// own work.
			if got := graphs.GroupIdOf(graph); got != groupId {
				t.Errorf("attribute %v is %q, want %q", model.AttrKeycloakGroupId, got, groupId)
			}
			if got := graphs.GroupPathOf(graph); got != groupPath {
				t.Errorf("attribute %v is %q, want %q", model.AttrKeycloakGroup, got, groupPath)
			}
			if got := graphs.GeneratedNameOf(graph); got != groupName {
				t.Errorf("attribute %v is %q, want %q", model.AttrName, got, groupName)
			}

			// The display name of a graph is the name of its root node, not a
			// graph-level attribute. Getting this wrong renders wrongly
			// rather than failing, which is why it is asserted here.
			root, found := graphs.RootOf(graph)
			if !found {
				t.Fatalf("graph %v has no root node", graph.Id)
			}
			if name := graphs.AttributeOf(root.Attributes, model.NodeAttrName); name != groupName {
				t.Errorf("root node is named %q, want the group's leaf name %q", name, groupName)
			}

			// A new device is placed as a meter thing, not as a device node:
			// the node is of resource type thing and carries the thing's id,
			// and the device is reached through the thing's open link.
			if got := graphs.DeviceIdsIn(graph); len(got) != 0 {
				t.Errorf("graph holds device nodes %v, want none: a new device is placed as a meter thing", got)
			}
			want := []string{main.id, sub.id, small.id}
			slices.Sort(want)
			if got := world.devicesIn(t, graph); !reflect.DeepEqual(got, want) {
				t.Errorf("graph reaches devices %v, want %v", got, want)
			}
			if got := graphs.ThingIdsIn(graph); len(got) != len(want) {
				t.Errorf("graph holds thing nodes %v, want one per device", got)
			}
			for _, device := range meters {
				world.assertMeter(t, graph, device)
			}

			// The tree the injected figures have to produce: the 600 sits
			// inside the 1000, and the 200 goes to the tighter of the
			// containers that still fit - 400 unexplained on the 1000
			// against 600 on the 600 - which is the 1000 again. See
			// SPEC.md, "Tightest fit is not deepest fit". The figures are
			// read through the device each meter is linked to.
			assertParent(t, world, graph, main.id, "", "the largest meter hangs off the root")
			assertParent(t, world, graph, sub.id, main.id, "the 600 fits inside the 1000")
			assertParent(t, world, graph, small.id, main.id, "the 200 goes to the tightest container that fits")

			// r w x and deliberately not a: the group edits the company
			// graph, the owner keeps the right to delete and re-share it.
			assertGroupPermission(t, world, graph.Id, groupPath, permissions.PermissionsMap{
				Read: true, Write: true, Execute: true,
			})
		}},

		{"a device withdrawn from the group", true, func(t *testing.T) {
			world.shareDevice(t, small.id)

			// A device that left is not taken out of the graph. Its meter is
			// the measuring point and carries what was measured there, so the
			// thing and its node stay; what ends is the device's link to it,
			// and only once the device was missing in two consecutive passes,
			// so that a permissions read that glitched once cannot cut the
			// history. See docs/reconciliation.md, "Graph exists".
			world.full(t)
			graph := world.defaultGraph(t)
			if err := graph.Valid(); err != nil {
				t.Fatalf("the graph is invalid after the first pass without a device: %v", err)
			}
			_, thing := world.meterOf(t, graph, small.id)
			if len(thing.DeviceLinks) != 1 || !world.linkOpen(thing.DeviceLinks[0]) {
				t.Errorf("after one pass the link of the missing device is %+v, want it still open: a link is ended only after two consecutive passes",
					thing.DeviceLinks)
			}

			world.full(t)
			graph = world.defaultGraph(t)
			if err := graph.Valid(); err != nil {
				t.Fatalf("the graph is invalid after the second pass without a device: %v", err)
			}
			node, thing := world.meterOf(t, graph, small.id)
			if len(thing.DeviceLinks) != 1 || world.linkOpen(thing.DeviceLinks[0]) {
				t.Errorf("after two passes the link of the withdrawn device is %+v, want it ended", thing.DeviceLinks)
			}
			if node.ResourceType != model.ResourceTypeThing {
				t.Errorf("the withdrawn device's meter has lost its node: %+v", node)
			}

			// What the graph accounts for is what is linked now: the
			// withdrawn device is gone from it, its meter is not.
			want := []string{main.id, sub.id}
			slices.Sort(want)
			if got := world.devicesIn(t, graph); !reflect.DeepEqual(got, want) {
				t.Errorf("graph reaches devices %v, want %v - the withdrawn device should be gone", got, want)
			}
			if got := graphs.ThingIdsIn(graph); len(got) != len(meters) {
				t.Errorf("graph holds thing nodes %v, want all %v meters to stay", got, len(meters))
			}
			if got := world.thingsLinkedTo(t, small.id); len(got) != 1 {
				t.Errorf("the withdrawn device is linked to %v things, want its one meter and no new one", len(got))
			}
			assertParent(t, world, graph, main.id, "", "the remaining structure is untouched")
			assertParent(t, world, graph, sub.id, main.id, "the remaining structure is untouched")
			assertParent(t, world, graph, small.id, main.id, "the meter of the withdrawn device stays where it was")
			assertGroupPermission(t, world, graph.Id, groupPath, permissions.PermissionsMap{
				Read: true, Write: true, Execute: true,
			})
		}},

		{"two further passes write nothing", true, func(t *testing.T) {
			// The claim this checks is idempotence against the real
			// repository, which includes everything the repository does to a
			// graph on the way in and out: what it echoes back from a write,
			// what Mongo makes of an empty attribute list, what a read
			// returns. If any of that differs from what the reconciler
			// compares against, a pass writes again - and since every write
			// produces a command this service consumes, the loop never ends.
			before := world.defaultGraph(t)
			thingsBefore := world.allThings(t)
			world.resetWrites()
			world.full(t)
			world.full(t)
			after := world.defaultGraph(t)
			thingsAfter := world.allThings(t)

			// Counted rather than only compared: writing the same content
			// again leaves the stored graph equal and the loop endless.
			if graphWrites, permissionWrites := world.writes(); graphWrites != 0 || permissionWrites != 0 {
				t.Errorf("two further passes wrote %v graphs and %v permissions, want none: a pass that writes what is already there feeds itself its own events",
					graphWrites, permissionWrites)
			}
			if !reflect.DeepEqual(before, after) {
				t.Errorf("two further passes changed the stored graph, so reconciliation is not idempotent against the real repository:\nbefore: %v\nafter:  %v",
					pretty(before), pretty(after))
			}
			// Things are written too, though not counted above: a pass that
			// creates a meter again or ends a link again is the same loop one
			// level down.
			if !reflect.DeepEqual(thingsBefore, thingsAfter) {
				t.Errorf("two further passes changed the things, so a pass still creates or ends something:\nbefore: %v\nafter:  %v",
					thingsBefore, thingsAfter)
			}
		}},

		{"a user's edit survives a pass", true, func(t *testing.T) {
			graph := world.defaultGraph(t)

			// Move the sub-meter from under the main meter to the root, the
			// way a user correcting the guess would, and write it back
			// through the repository rather than into a fake.
			subNode, _ := world.meterOf(t, graph, sub.id)
			moved := false
			for i, edge := range graph.Edges {
				if edge.FromNodeId == subNode.Id {
					graph.Edges[i].ToNodeId = model.RootNodeId
					graph.Edges[i].Id = "user-edit-" + subNode.Id
					moved = true
				}
			}
			if !moved {
				t.Fatalf("no edge leaves the sub-meter's node %v, nothing to move", subNode.Id)
			}
			if err := graph.Valid(); err != nil {
				t.Fatalf("the edited graph is invalid before it is written: %v", err)
			}
			if _, err, code := world.repository.SetGraph(world.ctx, world.token, graph); err != nil {
				t.Fatalf("unable to write the user's edit (%v): %v", code, err)
			}

			world.full(t)

			after := world.defaultGraph(t)
			if err := after.Valid(); err != nil {
				t.Fatalf("the graph is invalid after the pass: %v", err)
			}
			assertParent(t, world, after, sub.id, "", "the service never re-parents an edge a user moved")
			assertParent(t, world, after, main.id, "", "the main meter is where it was")
			assertParent(t, world, after, small.id, main.id, "the meter of the withdrawn device is where it was")
		}},

		{"assumption: the attributes filter and the admin bypass", false, func(t *testing.T) {
			// A graph carrying only the path attribute is the legacy shape
			// SPEC.md says is adopted rather than duplicated: written before
			// the id was recorded, or a write interrupted between the two.
			// pkg/graphs has to be able to find it.
			legacy := world.mustSetGraph(t, platform.Graph{
				Owner: world.serviceUserId,
				Attributes: []platform.Attribute{
					{Key: model.AttrKeycloakGroup, Value: "/acme/legacy", Origin: model.AttrOrigin},
				},
				Nodes: []platform.Node{{Id: model.RootNodeId, ResourceType: model.ResourceTypeCustom}},
				Edges: []platform.Edge{},
			})

			// A graph of the same shape owned by somebody else, which this
			// service holds no permission on at all. Only the admin bypass
			// can return it.
			foreign := world.mustSetGraph(t, platform.Graph{
				Owner: "somebody-else",
				Attributes: []platform.Attribute{
					{Key: model.AttrKeycloakGroupId, Value: "kc-foreign", Origin: model.AttrOrigin},
					{Key: model.AttrKeycloakGroup, Value: "/foreign", Origin: model.AttrOrigin},
				},
				Nodes: []platform.Node{{Id: model.RootNodeId, ResourceType: model.ResourceTypeCustom}},
				Edges: []platform.Edge{},
			})

			// What the two filter shapes actually match, reported rather than
			// assumed. The fake believes ANY of the listed keys.
			both := world.listGraphIds(t, devicerepoclient.GraphListOptions{
				Limit: 100,
				Attributes: []platform.Attribute{
					{Key: model.AttrKeycloakGroupId},
					{Key: model.AttrKeycloakGroup},
				},
			})
			pathOnly := world.listGraphIds(t, devicerepoclient.GraphListOptions{
				Limit:      100,
				Attributes: []platform.Attribute{{Key: model.AttrKeycloakGroup}},
			})

			if !slices.Contains(pathOnly, legacy.Id) {
				broken(t, "a single-key attributes filter matches every graph carrying that key",
					"a graph carrying %v was not returned when filtering on that key alone", model.AttrKeycloakGroup)
			}
			if slices.Contains(both, legacy.Id) {
				t.Logf("the plain attributes filter matches ANY of the listed keys, as the fakes believe")
			} else {
				t.Logf("CONFIRMED WRONG: the plain attributes filter matches ALL of the listed keys - "+
					"a graph carrying only %v is invisible to a two-key filter (graph %v). "+
					"pkg/graphs.Defaults therefore asks once per key.",
					model.AttrKeycloakGroup, legacy.Id)
			}

			// The bypass, which is what leaving Ids nil is for.
			if !slices.Contains(pathOnly, foreign.Id) {
				broken(t, "listing graphs with Ids nil applies the admin bypass",
					"graph %v, owned by somebody else and shared with nobody, was not returned", foreign.Id)
			}

			// What the service actually relies on: both shapes are found.
			defaults, err := world.store.Defaults(world.ctx)
			if err != nil {
				t.Fatalf("unable to list the default graphs: %v", err)
			}
			if _, found := defaults.ByPath["/acme/legacy"]; !found {
				broken(t, "Defaults finds a graph that carries only the path attribute",
					"ByPath holds %v", keysOf(defaults.ByPath))
			}
			if _, found := defaults.ById["kc-foreign"]; !found {
				broken(t, "Defaults finds a default graph owned by another user",
					"ById holds %v", keysOf(defaults.ById))
			}

			// Get reads through an id filter, which the repository sends
			// through a per-id permission check rather than the bypass.
			if _, found, err := world.store.Get(world.ctx, foreign.Id); err != nil {
				t.Errorf("unable to read graph %v by id: %v", foreign.Id, err)
			} else if !found {
				t.Logf("NOTE: ListGraphs with an id filter does not return graph %v, which this service "+
					"holds no permission on. Store.Get is only ever used on the service's own graphs, "+
					"so nothing depends on it - but a graph a user took the service's rights away from "+
					"is invisible to the graph trigger.", foreign.Id)
			}
		}},

		{"assumption: attributes_json is broken in the client", false, func(t *testing.T) {
			// The client percent-encodes the JSON and the query encoder
			// escapes it again, so the repository is handed something that is
			// no longer JSON. An Origin on the filter is what makes the
			// client take that path. pkg/graphs avoids it for this reason;
			// if it works, that reason is gone.
			_, _, err, code := world.repository.ListGraphs(world.ctx, world.token, devicerepoclient.GraphListOptions{
				Limit: 100,
				Attributes: []platform.Attribute{
					{Key: model.AttrKeycloakGroup, Origin: model.AttrOrigin},
				},
			})
			if err == nil {
				t.Logf("REFUTED: attributes_json is accepted by the repository (code %v). "+
					"pkg/graphs could filter on attribute values after all.", code)
				return
			}
			t.Logf("CONFIRMED: attributes_json is unusable through this client (code %v): %v", code, err)
		}},

		{"assumption: GetResource answers 404 for an unknown id", false, func(t *testing.T) {
			// pkg/graphs and pkg/cache both branch on the code being 404 and
			// treat anything else as a failure. A 200 with an empty resource,
			// or a 403, would silently change what "no resource yet" means.
			_, err, code := world.permissions.GetResource(world.token, graphTopic, "urn:does-not-exist")
			if code != 404 {
				broken(t, "GetResource answers 404 for an unknown id",
					"code was %v, error %v", code, err)
			}
			if err == nil {
				broken(t, "GetResource reports an unknown id as an error",
					"error was nil at code %v", code)
			}
		}},

		{"assumption: a short page is the last page", false, func(t *testing.T) {
			// Both sweeps in pkg/cache stop on a page shorter than the limit.
			// If a page could be short for any other reason - a permission
			// filter applied after paging, an unstable sort - the sweep would
			// silently drop every device behind that page.
			allDevices := world.listDeviceIds(t, 1000, 0)
			paged := []string{}
			for offset := int64(0); ; offset += 2 {
				page := world.listDeviceIds(t, 2, offset)
				paged = append(paged, page...)
				if len(page) < 2 {
					break
				}
			}
			assertSameSet(t, "ListExtendedDevices paged in twos", paged, allDevices)

			allResources := world.listResourceIds(t, 1000, 0)
			pagedResources := []string{}
			for offset := int64(0); ; offset += 2 {
				page := world.listResourceIds(t, 2, offset)
				pagedResources = append(pagedResources, page...)
				if len(page) < 2 {
					break
				}
			}
			assertSameSet(t, "ListResourcesWithAdminPermission paged in twos", pagedResources, allResources)
		}},

		{"assumption: FullDt ships the whole device type", false, func(t *testing.T) {
			// Without this, deriving a device's carriers costs one request
			// per device, and pkg/cache would be quietly wrong instead of
			// slow: a device with no columns is attached to the root.
			withDt, _, err, code := world.repository.ListExtendedDevices(world.ctx, world.token, devicerepoclient.ExtendedDeviceListOptions{
				Ids:    []string{main.id},
				FullDt: true,
			})
			if err != nil || len(withDt) != 1 {
				t.Fatalf("unable to read device %v with FullDt (%v): %v", main.id, code, err)
			}
			if withDt[0].DeviceType == nil {
				broken(t, "ListExtendedDevices with FullDt true ships the full device type",
					"device_type was null for device %v", main.id)
				return
			}
			if withDt[0].DeviceType.Id != deviceTypeId {
				t.Errorf("device type shipped is %v, want %v", withDt[0].DeviceType.Id, deviceTypeId)
			}
			columns := carrier.Columns(*withDt[0].DeviceType)
			want := []model.CarrierColumn{{
				ServiceId: withDt[0].DeviceType.Services[0].Id,
				Name:      "reading" + model.ColumnPathSeparator + "energy",
				Carrier:   model.Electricity,
			}}
			if !reflect.DeepEqual(columns, want) {
				t.Errorf("carrier columns derived from the shipped device type are %v, want %v", columns, want)
			}

			withoutDt, _, err, code := world.repository.ListExtendedDevices(world.ctx, world.token, devicerepoclient.ExtendedDeviceListOptions{
				Ids: []string{main.id},
			})
			if err != nil || len(withoutDt) != 1 {
				t.Fatalf("unable to read device %v without FullDt (%v): %v", main.id, code, err)
			}
			if withoutDt[0].DeviceType != nil {
				t.Logf("NOTE: the device type ships even without FullDt, so the flag is not what makes it work")
			}
		}},

		{"assumption: the admin token sees every device", false, func(t *testing.T) {
			// A device this service holds no permission on at all. If the
			// bypass did not apply, a company's devices would silently drop
			// out of its graph whenever somebody took the service's rights
			// away - and the service would report itself healthy.
			device, err, code := world.repository.CreateDevice(world.ctx, world.token, platform.Device{
				LocalId:      "foreign-meter",
				Name:         "foreign-meter",
				DeviceTypeId: deviceTypeId,
			})
			if err != nil {
				t.Fatalf("unable to create the foreign device (%v): %v", code, err)
			}
			if _, err, code = world.permissions.SetPermission(world.token, deviceTopic, device.Id, permissions.ResourcePermissions{
				UserPermissions: map[string]permissions.PermissionsMap{
					"somebody-else": {Read: true, Write: true, Execute: true, Administrate: true},
				},
				GroupPermissions: map[string]permissions.PermissionsMap{},
				RolePermissions:  map[string]permissions.PermissionsMap{},
			}); err != nil {
				t.Fatalf("unable to hand device %v to another user (%v): %v", device.Id, code, err)
			}

			if ids := world.listDeviceIds(t, 1000, 0); !slices.Contains(ids, device.Id) {
				broken(t, "the internal admin token sees every device",
					"device %v, owned by another user and shared with nobody, is not in the listing", device.Id)
			}
			if ids := world.listResourceIds(t, 1000, 0); !slices.Contains(ids, device.Id) {
				broken(t, "the internal admin token lists every device's permissions",
					"resource %v is not in ListResourcesWithAdminPermission", device.Id)
			}
		}},
	}

	for _, step := range steps {
		if !t.Run(step.name, step.run) && step.scenario {
			t.Fatalf("stopping after %q: the scenarios build on each other", step.name)
		}
	}
}

// --- assertions ---------------------------------------------------------------

// broken reports a belief the fakes encode and the real API does not keep, in
// the words of the belief. A number that differs says nothing on its own; the
// belief says where to go and read.
func broken(t *testing.T, assumption string, format string, args ...any) {
	t.Helper()
	t.Errorf("ASSUMPTION BROKEN: %v\n  %v", assumption, fmt.Sprintf(format, args...))
}

// assertParent checks the node a device's meter points at. Structure rather
// than edge ids: a removal reroutes edges through the model's own id provider,
// so no edge id survives one. The device is resolved to its meter's node
// through the things API; wantParentDevice is the device whose meter node the
// edge has to end at, or "" for the root.
func assertParent(t *testing.T, world *stack, graph platform.Graph, deviceId string, wantParentDevice string, why string) {
	t.Helper()
	node, _, found := world.findMeter(t, graph, deviceId)
	if !found {
		t.Errorf("%v: device %v has no meter node in graph %v", why, deviceId, graph.Id)
		return
	}
	wantParent := model.RootNodeId
	if wantParentDevice != "" {
		parentNode, _, parentFound := world.findMeter(t, graph, wantParentDevice)
		if !parentFound {
			t.Errorf("%v: device %v, the expected parent, has no meter node in graph %v", why, wantParentDevice, graph.Id)
			return
		}
		wantParent = parentNode.Id
	}
	parents := []string{}
	for _, edge := range graph.Edges {
		if edge.FromNodeId == node.Id {
			parents = append(parents, edge.ToNodeId)
			if edge.Weight != model.FullWeight {
				t.Errorf("edge %v has weight %v, want %v - in a tree an edge carries its whole node",
					edge.Id, edge.Weight, model.FullWeight)
			}
		}
	}
	if len(parents) != 1 {
		t.Errorf("%v: the meter node %v of device %v points at %v, want exactly one parent", why, node.Id, deviceId, parents)
		return
	}
	if parents[0] != wantParent {
		t.Errorf("%v: the meter node %v of device %v hangs under %v, want %v", why, node.Id, deviceId, parents[0], wantParent)
	}
}

// findMeter resolves a device to the node it is placed as: a node of resource
// type thing, pointing at a thing the device has a link to - open or ended - and
// present in the graph. A device with such a node twice is reported as not found
// after the error, because there is no one answer.
func (this *stack) findMeter(t *testing.T, graph platform.Graph, deviceId string) (platform.Node, things.Thing, bool) {
	t.Helper()
	linked := this.thingsLinkedTo(t, deviceId)
	var foundNode platform.Node
	var foundThing things.Thing
	count := 0
	for _, thing := range linked {
		for _, node := range graph.Nodes {
			if node.ResourceType == model.ResourceTypeThing && node.ResourceId == thing.Id {
				foundNode, foundThing = node, thing
				count++
			}
		}
	}
	if count > 1 {
		t.Errorf("device %v is placed by %v thing nodes in graph %v, want one", deviceId, count, graph.Id)
		return platform.Node{}, things.Thing{}, false
	}
	return foundNode, foundThing, count == 1
}

// meterOf is findMeter for a device that has to be there.
func (this *stack) meterOf(t *testing.T, graph platform.Graph, deviceId string) (platform.Node, things.Thing) {
	t.Helper()
	node, thing, found := this.findMeter(t, graph, deviceId)
	if !found {
		t.Fatalf("device %v has no meter node in graph %v", deviceId, graph.Id)
	}
	return node, thing
}

// assertMeter checks what the contract says about the meter a device was
// placed as: a node of resource type thing in the thing's id, no attributes on
// the node, a meter thing named after the device, owned by the service user and
// carrying the group's id, with one open link to the device, shared with the
// group read, write and execute.
func (this *stack) assertMeter(t *testing.T, graph platform.Graph, device meter) {
	t.Helper()
	node, thing, found := this.findMeter(t, graph, device.id)
	if !found {
		t.Errorf("device %v has no meter node in graph %v", device.id, graph.Id)
		return
	}
	if node.Id != node.ResourceId || node.Id != thing.Id {
		t.Errorf("meter node of device %v is %v / %v, want the thing's id %v as node id and resource id",
			device.id, node.Id, node.ResourceId, thing.Id)
	}
	if len(node.Attributes) != 0 {
		t.Errorf("meter node %v carries attributes %+v, want none", node.Id, node.Attributes)
	}
	if thing.Kind != things.KindMeter {
		t.Errorf("thing %v of device %v is of kind %q, want %q", thing.Id, device.id, thing.Kind, things.KindMeter)
	}
	if thing.Name != device.name {
		t.Errorf("thing %v is named %q, want the device's name %q", thing.Id, thing.Name, device.name)
	}
	if thing.OwnerId != this.serviceUserId {
		t.Errorf("thing %v is owned by %q, want the service user %q", thing.Id, thing.OwnerId, this.serviceUserId)
	}
	if got := graphs.AttributeOf(thing.Attributes, model.AttrKeycloakGroupId); got != groupId {
		t.Errorf("thing %v carries %v %q, want %q: a later pass recognises its own meters by it",
			thing.Id, model.AttrKeycloakGroupId, got, groupId)
	}
	if len(thing.DeviceLinks) != 1 || thing.DeviceLinks[0].DeviceId != device.id || !this.linkOpen(thing.DeviceLinks[0]) {
		t.Errorf("thing %v has links %+v, want one open link to device %v", thing.Id, thing.DeviceLinks, device.id)
	}
	resource, err, code := this.permissions.GetResource(this.token, thingTopic, thing.Id)
	if err != nil {
		t.Errorf("unable to read the permissions of thing %v (%v): %v", thing.Id, code, err)
		return
	}
	want := permissions.PermissionsMap{Read: true, Write: true, Execute: true}
	if got := resource.GroupPermissions[groupPath]; got != want {
		t.Errorf("thing %v grants %v %+v, want %+v: a member who reads the graph has to read its meters",
			thing.Id, groupPath, got, want)
	}
}

// linkOpen is whether a device link holds now or later, the way the service
// reads it: no end, or an end still ahead.
func (this *stack) linkOpen(link things.Link) bool {
	return link.Until == nil || link.Until.After(time.Now())
}

// thingsLinkedTo lists the things a device has a link to, open or ended, by
// asking the repository for exactly that.
func (this *stack) thingsLinkedTo(t *testing.T, deviceId string) []things.Thing {
	t.Helper()
	found, err, code := this.things.List(this.ctx, this.token, nil, []string{deviceId})
	if err != nil {
		t.Fatalf("unable to list the things of device %v (%v): %v", deviceId, code, err)
	}
	return found
}

// allThings is every thing the token may read, in id order.
func (this *stack) allThings(t *testing.T) []things.Thing {
	t.Helper()
	found, err, code := this.things.List(this.ctx, this.token, nil, nil)
	if err != nil {
		t.Fatalf("unable to list the things (%v): %v", code, err)
	}
	return found
}

// devicesIn is every device a graph accounts for: those with a device node and
// those with an open link to a thing that has a node in it. Sorted.
func (this *stack) devicesIn(t *testing.T, graph platform.Graph) []string {
	t.Helper()
	result := graphs.DeviceIdsIn(graph)
	if thingIds := graphs.ThingIdsIn(graph); len(thingIds) > 0 {
		found, err, code := this.things.List(this.ctx, this.token, thingIds, nil)
		if err != nil {
			t.Fatalf("unable to list the things of graph %v (%v): %v", graph.Id, code, err)
		}
		for _, thing := range found {
			for _, link := range thing.DeviceLinks {
				if this.linkOpen(link) && !slices.Contains(result, link.DeviceId) {
					result = append(result, link.DeviceId)
				}
			}
		}
	}
	slices.Sort(result)
	return result
}

// assertGroupPermission reads a graph's sharing back out of permissions-v2.
func assertGroupPermission(t *testing.T, world *stack, graphId string, path string, want permissions.PermissionsMap) {
	t.Helper()
	resource, err, code := world.permissions.GetResource(world.token, graphTopic, graphId)
	if err != nil {
		if code == 404 {
			broken(t, "writing a graph creates a permission resource for it",
				"graph %v has no resource, so Share could not have worked", graphId)
			return
		}
		t.Fatalf("unable to read the permissions of graph %v (%v): %v", graphId, code, err)
	}

	// The service is the graph's sole administrator. Without it, it could not
	// change the sharing of a graph it created itself.
	if owner := resource.UserPermissions[world.serviceUserId]; !owner.Administrate {
		broken(t, "a graph written without an id gets the caller as its administrator",
			"user permissions of graph %v are %v", graphId, resource.UserPermissions)
	}

	got, held := resource.GroupPermissions[path]
	if !held {
		t.Errorf("graph %v is not shared with %v (group permissions: %v)", graphId, path, resource.GroupPermissions)
		return
	}
	if got != want {
		t.Errorf("graph %v grants %v %+v, want %+v", graphId, path, got, want)
	}
	if got.Administrate {
		t.Errorf("graph %v grants %v administrate: the group may edit the graph, not delete or re-share it", graphId, path)
	}
}

// assertSameSet compares two id listings as sets, so a paging bug is reported
// as what it costs - a device nobody sees - rather than as an order.
func assertSameSet(t *testing.T, what string, got []string, want []string) {
	t.Helper()
	gotSorted, wantSorted := slices.Clone(got), slices.Clone(want)
	slices.Sort(gotSorted)
	slices.Sort(wantSorted)
	if !reflect.DeepEqual(slices.Compact(gotSorted), slices.Compact(wantSorted)) {
		broken(t, "a page shorter than the limit is the last page",
			"%v yielded %v, one unpaged call yielded %v", what, got, want)
	}
	if len(slices.Compact(gotSorted)) != len(gotSorted) {
		broken(t, "paging returns every element exactly once",
			"%v returned duplicates: %v", what, got)
	}
}

// --- reading the world --------------------------------------------------------

func (this *stack) mustSetGraph(t *testing.T, graph platform.Graph) platform.Graph {
	t.Helper()
	result, err, code := this.repository.SetGraph(this.ctx, this.token, graph)
	if err != nil {
		t.Fatalf("unable to write a graph (%v): %v", code, err)
	}
	return result
}

func (this *stack) listGraphIds(t *testing.T, options devicerepoclient.GraphListOptions) []string {
	t.Helper()
	page, _, err, code := this.repository.ListGraphs(this.ctx, this.token, options)
	if err != nil {
		t.Fatalf("unable to list graphs (%v): %v", code, err)
	}
	result := []string{}
	for _, graph := range page {
		result = append(result, graph.Id)
	}
	return result
}

func (this *stack) listDeviceIds(t *testing.T, limit int64, offset int64) []string {
	t.Helper()
	page, _, err, code := this.repository.ListExtendedDevices(this.ctx, this.token, devicerepoclient.ExtendedDeviceListOptions{
		Limit:  limit,
		Offset: offset,
		FullDt: true,
		SortBy: "id.asc",
	})
	if err != nil {
		t.Fatalf("unable to list devices (%v): %v", code, err)
	}
	result := []string{}
	for _, device := range page {
		result = append(result, device.Id)
	}
	return result
}

func (this *stack) listResourceIds(t *testing.T, limit int64, offset int64) []string {
	t.Helper()
	page, err, code := this.permissions.ListResourcesWithAdminPermission(this.token, deviceTopic, permissions.ListOptions{
		Limit:  limit,
		Offset: offset,
	})
	if err != nil {
		t.Fatalf("unable to list device permissions (%v): %v", code, err)
	}
	result := []string{}
	for _, resource := range page {
		result = append(result, resource.Id)
	}
	return result
}

func keysOf[T any](m map[string]T) []string {
	result := []string{}
	for key := range m {
		result = append(result, key)
	}
	slices.Sort(result)
	return result
}

// pretty is a graph in a shape a human can compare two of.
func pretty(graph platform.Graph) string {
	encoded, err := json.MarshalIndent(graph, "", "  ")
	if err != nil {
		return fmt.Sprintf("%+v", graph)
	}
	return string(encoded)
}
