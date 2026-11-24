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
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/SENERGY-Platform/graph-provider/pkg/graphs"
	"github.com/SENERGY-Platform/graph-provider/pkg/model"
	"github.com/SENERGY-Platform/graph-provider/pkg/things"
	platform "github.com/SENERGY-Platform/models/go/models"
)

// thingGroupHas reports whether a group has any right on a thing.
func (this *world) thingGroupHas(thingId string, path string) bool {
	this.mu.Lock()
	defer this.mu.Unlock()
	_, ok := this.resources[thingTopic][thingId].GroupPermissions[path]
	return ok
}

func (this *world) thingList() []things.Thing {
	this.mu.Lock()
	defer this.mu.Unlock()
	result := []things.Thing{}
	for _, id := range this.thingOrder {
		result = append(result, this.things[id])
	}
	return result
}

func thingOfNode(t *testing.T, h *harness, node platform.Node) things.Thing {
	t.Helper()
	for _, thing := range h.world.thingList() {
		if thing.Id == node.ResourceId {
			return thing
		}
	}
	t.Fatalf("node %v points at thing %v, which does not exist", node.Id, node.ResourceId)
	return things.Thing{}
}

// A new device is not a device node but a measuring point: a meter thing named
// after the device, owned like the graph, linked to the device from now, shared
// with the group, and a node of resource type thing in the thing's id.
func TestNewDevicesLandAsMeterThings(t *testing.T) {
	h := acmeHarness(t)
	h.mustFull(t)

	graph, _ := h.world.graphOfGroup("/acme")
	if got := graphs.DeviceIdsIn(graph); len(got) != 0 {
		t.Errorf("a new device must not get a device node, got %v", got)
	}
	thingNodes := 0
	for _, node := range graph.Nodes {
		if node.ResourceType != model.ResourceTypeThing {
			continue
		}
		thingNodes++
		if node.Id != node.ResourceId {
			t.Errorf("a thing node carries the thing's id as its own, got %v / %v", node.Id, node.ResourceId)
		}
		if len(node.Attributes) != 0 {
			t.Errorf("a meter node carries no carrier or grid attribute, got %+v", node.Attributes)
		}
		thing := thingOfNode(t, h, node)
		if thing.Kind != things.KindMeter {
			t.Errorf("expected a meter, got kind %q", thing.Kind)
		}
		if thing.OwnerId != serviceUser {
			t.Errorf("a meter is owned like its graph, got %q", thing.OwnerId)
		}
		if len(thing.DeviceLinks) != 1 {
			t.Fatalf("expected one link, got %+v", thing.DeviceLinks)
		}
		link := thing.DeviceLinks[0]
		if link.Until != nil || link.DeviceId == "" || !link.From.Equal(fixedNow) {
			t.Errorf("expected an open link from now, got %+v", link)
		}
		wantName := map[string]string{"d1": "Hauptzaehler", "d2": "Nebenzaehler"}[link.DeviceId]
		if thing.Name != wantName {
			t.Errorf("a meter is named after its device, got %q for %v", thing.Name, link.DeviceId)
		}
		if !h.world.thingGroupHas(thing.Id, "/acme") {
			t.Errorf("the group must be able to read meter %v", thing.Id)
		}
	}
	if thingNodes != 2 {
		t.Fatalf("expected both devices as meters, got %v thing nodes", thingNodes)
	}

	// Placement is the heuristic's, unchanged: it reads the device's readings.
	view := h.graphOfGroup(t, "/acme")
	if got := parentOf(view, "d2"); got != "d1" {
		t.Errorf("expected the sub-meter under the supply meter, got %q", got)
	}
	if err := graph.Valid(); err != nil {
		t.Errorf("invalid graph: %v", err)
	}

	// What is sent never carries a validity: the server sets it, and answers
	// the field on an added element with a 400.
	for _, body := range h.world.setGraphBodies {
		if strings.Contains(string(body), "valid_from") || strings.Contains(string(body), "valid_until") {
			t.Errorf("a new element must not carry a validity: %s", body)
		}
	}
}

// A device that already has a node, or a meter with an open link in the graph,
// is not added again - and a graph that had device nodes keeps them as they are.
func TestExistingDeviceNodesAreLeftAsTheyAre(t *testing.T) {
	h := acmeHarness(t)
	seeded := seedNested(h, "/acme", "acme", "d1", "d2")
	h.world.meter("d3", "Drittzaehler", 100, "/acme")
	h.mustFull(t)

	after, _ := h.world.graph(seeded.Id)
	byId := map[string]platform.Node{}
	for _, node := range after.Nodes {
		byId[node.Id] = node
	}
	for _, id := range []string{"d1", "d2"} {
		node, ok := byId[id]
		if !ok || node.ResourceType != platform.GraphResourceTypeDevice || node.ResourceId != id {
			t.Errorf("the device node %v must stay as it was, got %+v", id, node)
		}
	}
	if err := graphs.NodeIdentityKept(seeded, after); err != nil {
		t.Error(err)
	}
	if got := len(graphs.ThingIdsIn(after)); got != 1 {
		t.Errorf("only the new device becomes a meter, got %v things", got)
	}
	if got := len(h.world.thingList()); got != 1 {
		t.Errorf("expected one thing created, got %v", got)
	}
}

func TestADeviceOfAMeterInTheGraphIsNotAddedAgain(t *testing.T) {
	h := acmeHarness(t)
	h.mustFull(t)
	created := len(h.world.thingList())
	h.world.resetCalls()

	h.mustFull(t)
	if got := h.world.countCalls("SetGraph"); got != 0 {
		t.Errorf("the second pass must write nothing, got %v graph writes", got)
	}
	if got := h.world.countCalls("CreateThing") + h.world.countCalls("AddDeviceLink"); got != 0 {
		t.Errorf("the second pass must create no thing and no link, got %v", got)
	}
	if got := len(h.world.thingList()); got != created {
		t.Errorf("things went from %v to %v", created, got)
	}
}

// A thing a person put in the graph and linked to the device counts too: the
// repository refuses a graph holding both a thing and a device linked to it, so
// adding the device would fail on every pass.
func TestADeviceLinkedToAForeignThingOfTheGraphIsNotAddedAgain(t *testing.T) {
	h := newHarness(t)
	h.world.setGroups("/acme")
	h.world.meter("d1", "Hauptzaehler", 1000, "/acme")
	h.world.things["x1"] = things.Thing{Id: "x1", Kind: "", Name: "Heizung", OwnerId: "somebody",
		DeviceLinks: []things.Link{{Id: "l", DeviceId: "d1", From: fixedNow.Add(-time.Hour)}}}
	h.world.thingOrder = append(h.world.thingOrder, "x1")
	graph := seededGraph("/acme", "acme", "acme")
	graph.Nodes = append(graph.Nodes, platform.Node{Id: "n1", ResourceType: model.ResourceTypeThing, ResourceId: "x1", Attributes: []platform.Attribute{}})
	graph.Edges = append(graph.Edges, platform.Edge{Id: "e1", FromNodeId: "n1", ToNodeId: model.RootNodeId, Weight: 100, Attributes: []platform.Attribute{}})
	h.world.putGraph(graph, "/acme")

	h.mustFull(t)
	if got := h.world.countCalls("SetGraph"); got != 0 {
		t.Errorf("d1 is accounted for through x1, nothing to write, got %v writes", got)
	}
}

// A link that ended no longer holds the device in the graph.
func TestADeviceWhoseLinkEndedIsPlacedAgain(t *testing.T) {
	h := newHarness(t)
	h.world.setGroups("/acme")
	h.world.meter("d1", "Hauptzaehler", 1000, "/acme")
	ended := fixedNow.Add(-time.Hour)
	h.world.things["m1"] = things.Thing{Id: "m1", Kind: things.KindMeter, Name: "alt", OwnerId: serviceUser,
		DeviceLinks: []things.Link{{Id: "l", DeviceId: "d1", From: fixedNow.Add(-48 * time.Hour), Until: &ended}}}
	h.world.thingOrder = append(h.world.thingOrder, "m1")
	graph := seededGraph("/acme", "acme", "acme")
	graph.Nodes = append(graph.Nodes, platform.Node{Id: "m1", ResourceType: model.ResourceTypeThing, ResourceId: "m1", Attributes: []platform.Attribute{}})
	graph.Edges = append(graph.Edges, platform.Edge{Id: "e1", FromNodeId: "m1", ToNodeId: model.RootNodeId, Weight: 100, Attributes: []platform.Attribute{}})
	h.world.putGraph(graph, "/acme")

	h.mustFull(t)
	if got := len(h.world.thingList()); got != 2 {
		t.Errorf("the device gets a new meter, got %v things", got)
	}
}

// A device linked to a thing this service did not make for the group cannot
// get a second open link, so it stays a device node.
func TestADeviceLinkedElsewhereStaysADeviceNode(t *testing.T) {
	h := newHarness(t)
	h.world.setGroups("/acme")
	h.world.meter("d1", "Hauptzaehler", 1000, "/acme")
	h.world.things["x1"] = things.Thing{Id: "x1", Name: "Heizung", OwnerId: "somebody",
		DeviceLinks: []things.Link{{Id: "l", DeviceId: "d1", From: fixedNow.Add(-time.Hour)}}}
	h.world.thingOrder = append(h.world.thingOrder, "x1")

	h.mustFull(t)
	graph, _ := h.world.graphOfGroup("/acme")
	if got := graphs.DeviceIdsIn(graph); len(got) != 1 || got[0] != "d1" {
		t.Errorf("expected d1 as a device node, got %v", got)
	}
	if got := len(h.world.thingList()); got != 1 {
		t.Errorf("no thing may be created for it, got %v", got)
	}
}

// Writing the graph fails after the things exist: the next pass finds each
// thing by its link and uses it, where a second thing would be refused anyway.
func TestThingsOfAFailedWriteAreAdoptedByTheNextPass(t *testing.T) {
	h := acmeHarness(t)
	h.world.failSetGraphWith = http.StatusInternalServerError
	if err := h.rec.Full(context.Background()); err == nil {
		t.Fatal("expected the failed write to be reported")
	}
	if got := len(h.world.thingList()); got != 2 {
		t.Fatalf("expected the things to exist, got %v", got)
	}
	before := h.world.thingList()

	h.mustFull(t)
	if got := len(h.world.thingList()); got != 2 {
		t.Errorf("the next pass must adopt, not create, got %v things", got)
	}
	graph, _ := h.world.graphOfGroup("/acme")
	for _, thing := range before {
		if !strings.Contains(strings.Join(graphs.ThingIdsIn(graph), ","), thing.Id) {
			t.Errorf("meter %v is not in the graph", thing.Id)
		}
	}
}

// A link that cannot be created takes its thing with it, and the group's pass
// ends without a graph, so the full heuristic still runs once the repository
// answers again.
func TestAFailedLinkRemovesItsThingAndWritesNoGraph(t *testing.T) {
	h := acmeHarness(t)
	h.world.failLink = true
	if err := h.rec.Full(context.Background()); err == nil {
		t.Fatal("expected the failed link to be reported")
	}
	if got := len(h.world.thingList()); got != 0 {
		t.Errorf("a thing without a link must not stay, got %v", got)
	}
	if got := h.world.graphCount(); got != 0 {
		t.Errorf("no graph may be written with a device missing, got %v", got)
	}
	h.world.failLink = false
	h.mustFull(t)
	if got := h.world.graphCount(); got != 1 {
		t.Errorf("expected the graph once the repository answers, got %v", got)
	}
}

// 409: the repository's compare-and-set lost five times. A transient failure
// like any other: reported, nothing lost, applied again by the next pass.
func TestALostCompareAndSetIsTransient(t *testing.T) {
	h := acmeHarness(t)
	h.mustFull(t)
	h.world.meter("d3", "Drittzaehler", 100, "/acme")
	h.world.failSetGraphWith = http.StatusConflict

	err := h.rec.Full(context.Background())
	if !errors.Is(err, graphs.ErrConflict) {
		t.Fatalf("expected the conflict to be named, got %v", err)
	}
	h.mustFull(t)
	graph, _ := h.world.graphOfGroup("/acme")
	if got := len(graphs.ThingIdsIn(graph)); got != 3 {
		t.Errorf("the edit must be applied by the next pass, got %v things in the graph", got)
	}
	if got := len(h.world.thingList()); got != 3 {
		t.Errorf("the meter made before the conflict is adopted, got %v things", got)
	}
}

// A graph made of meters still offers its nodes as parents, read through the
// device linked to each: without it every later device would go to the root.
func TestAMeterOfTheGraphIsAParentForANewDevice(t *testing.T) {
	h := acmeHarness(t)
	h.mustFull(t)
	h.world.meter("d3", "Drittzaehler", 100, "/acme")
	h.mustFull(t)

	view := h.graphOfGroup(t, "/acme")
	if got := parentOf(view, "d3"); got == model.RootNodeId || got == "" {
		t.Errorf("expected d3 below one of the meters, got %q", got)
	}
}

func openLinks(h *harness, deviceId string) int {
	count := 0
	for _, thing := range h.world.thingList() {
		for _, link := range thing.DeviceLinks {
			if link.DeviceId == deviceId && link.Until == nil {
				count++
			}
		}
	}
	return count
}

// A device that leaves the group has the link of its meter ended, but only when
// it was missing in two consecutive passes. Thing and node stay.
func TestTheLinkOfADepartedDeviceEndsAfterTwoPasses(t *testing.T) {
	h := acmeHarness(t)
	h.mustFull(t)
	h.world.setDeviceGroups("d2")
	h.world.resetCalls()

	h.mustFull(t)
	if got := h.world.countCalls("EndDeviceLink"); got != 0 || openLinks(h, "d2") != 1 {
		t.Fatalf("missing once must end nothing, got %v ends", got)
	}
	h.mustFull(t)
	if got := h.world.countCalls("EndDeviceLink"); got != 1 || openLinks(h, "d2") != 0 {
		t.Fatalf("missing twice must end the link, got %v ends", got)
	}
	if openLinks(h, "d1") != 1 {
		t.Error("the other device's link is untouched")
	}
	graph, _ := h.world.graphOfGroup("/acme")
	if got := len(graphs.ThingIdsIn(graph)); got != 2 {
		t.Errorf("the meter and its node stay, got %v things in the graph", got)
	}
	if got := len(h.world.thingList()); got != 2 {
		t.Errorf("no thing is deleted, got %v", got)
	}
	if h.world.countCalls("Correction")+h.world.countCalls("ChangeGraph") != 0 {
		t.Error("ending a link writes no graph")
	}
	// Ended once: a third pass has nothing left to end.
	h.mustFull(t)
	if got := h.world.countCalls("EndDeviceLink"); got != 1 {
		t.Errorf("the link is ended once, got %v", got)
	}
}

// A device that is back after one pass starts counting again.
func TestADeviceBackAfterOnePassResetsTheCount(t *testing.T) {
	h := acmeHarness(t)
	h.mustFull(t)
	h.world.setDeviceGroups("d2")
	h.mustFull(t)
	h.world.setDeviceGroups("d2", "/acme")
	h.mustFull(t)
	h.world.setDeviceGroups("d2")
	h.world.resetCalls()
	h.mustFull(t)
	if got := h.world.countCalls("EndDeviceLink"); got != 0 {
		t.Fatalf("one pass missing after a reset ends nothing, got %v", got)
	}
	h.mustFull(t)
	if got := h.world.countCalls("EndDeviceLink"); got != 1 {
		t.Errorf("the second consecutive pass ends it, got %v", got)
	}
}

// A link on a thing this service did not make for the group is never touched.
func TestALinkOnAForeignThingIsNeverEnded(t *testing.T) {
	h := newHarness(t)
	h.world.setGroups("/acme")
	h.world.meter("d1", "Hauptzaehler", 1000, "/acme")
	h.world.things["x1"] = things.Thing{Id: "x1", Kind: things.KindMeter, Name: "fremd", OwnerId: "somebody",
		Attributes:  []platform.Attribute{{Key: model.AttrKeycloakGroupId, Value: "kc/acme"}},
		DeviceLinks: []things.Link{{Id: "l", DeviceId: "gone", From: fixedNow.Add(-time.Hour)}}}
	h.world.things["x2"] = things.Thing{Id: "x2", Kind: things.KindMeter, Name: "ohne Marke", OwnerId: serviceUser,
		DeviceLinks: []things.Link{{Id: "l2", DeviceId: "gone2", From: fixedNow.Add(-time.Hour)}}}
	h.world.thingOrder = append(h.world.thingOrder, "x1", "x2")
	graph := seededGraph("/acme", "acme", "acme")
	for _, id := range []string{"x1", "x2"} {
		graph.Nodes = append(graph.Nodes, platform.Node{Id: id, ResourceType: model.ResourceTypeThing, ResourceId: id, Attributes: []platform.Attribute{}})
		graph.Edges = append(graph.Edges, platform.Edge{Id: "e" + id, FromNodeId: id, ToNodeId: model.RootNodeId, Weight: 100, Attributes: []platform.Attribute{}})
	}
	h.world.putGraph(graph, "/acme")
	for i := 0; i < 4; i++ {
		h.mustFull(t)
	}
	if got := h.world.countCalls("EndDeviceLink"); got != 0 {
		t.Errorf("foreign links must stay open, got %v ends", got)
	}
}

// With writing off nothing is ended.
func TestReadOnlyEndsNoLink(t *testing.T) {
	h := acmeHarness(t, writeDisabled)
	h.mustFull(t)
	for i := 0; i < 3; i++ {
		h.mustFull(t)
	}
	if h.world.countCalls("EndDeviceLink") != 0 {
		t.Error("writing is off")
	}
}

// The meters follow a rename of their group, as the graph does.
func TestMetersFollowAGroupRename(t *testing.T) {
	h := acmeHarness(t)
	h.mustFull(t)
	ids := []string{}
	for _, thing := range h.world.thingList() {
		ids = append(ids, thing.Id)
	}

	h.world.setGroupsWithIds(map[string]string{"/acme-gmbh": "kc/acme"})
	h.mustFull(t)
	for _, id := range ids {
		if !h.world.thingGroupHas(id, "/acme-gmbh") || h.world.thingGroupHas(id, "/acme") {
			t.Errorf("meter %v must be shared with the new path and no longer with the old", id)
		}
	}
}

// With writing off the pass creates and shares nothing, and the report still
// shows the graph it would write.
func TestReadOnlyPassCreatesNoThing(t *testing.T) {
	h := acmeHarness(t, writeDisabled)
	h.mustFull(t)
	for _, call := range []string{"CreateThing", "AddDeviceLink", "SetGraph", "SetPermission"} {
		if got := h.world.countCalls(call); got != 0 {
			t.Errorf("%v called %v times with writing disabled", call, got)
		}
	}
}

// Removing a departed device node is a change at now, not a correction: the
// repository refuses a correction while an ended version of an edge still
// points at the node, and a change keeps that history.
func TestRemovalIsAChangeAndKeepsEndedHistory(t *testing.T) {
	h := acmeHarness(t)
	seeded := seedNested(h, "/acme", "acme", "d1", "d2")
	// An earlier version of d2's edge, ended, pointing at d1 and at d2.
	h.world.ended = map[string][]platform.Edge{seeded.Id: {{Id: "old", FromNodeId: "d2", ToNodeId: "d1", Weight: 100}}}

	h.world.setDeviceGroups("d2")
	h.world.resetCalls()
	h.mustFull(t)

	if got := h.world.countCalls("ChangeGraph"); got != 1 {
		t.Errorf("expected the removal as one change, got %v", got)
	}
	if got := h.world.countCalls("Correction"); got != 0 {
		t.Errorf("a removal is not a correction, got %v corrections", got)
	}
	after, _ := h.world.graph(seeded.Id)
	for _, node := range after.Nodes {
		if node.Id == "d2" {
			t.Error("the departed device's node must be gone from the projection")
		}
	}
	if got := len(h.world.ended[seeded.Id]); got != 2 {
		t.Errorf("the ended version must stay and the removed edge ends, got %v ended edges", got)
	}
}

// A write that only adds stays a correction without an instant.
func TestAdditionStaysACorrection(t *testing.T) {
	h := acmeHarness(t)
	seedNested(h, "/acme", "acme", "d1", "d2")
	h.world.meter("d3", "Drittzaehler", 100, "/acme")
	h.world.resetCalls()
	h.mustFull(t)
	if h.world.countCalls("Correction") != 1 || h.world.countCalls("ChangeGraph") != 0 {
		t.Errorf("expected one correction, got %v", h.world.callOrder())
	}
}
