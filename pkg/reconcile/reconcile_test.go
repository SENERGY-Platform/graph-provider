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
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/SENERGY-Platform/graph-provider/pkg/events"
	"github.com/SENERGY-Platform/graph-provider/pkg/graphs"
	"github.com/SENERGY-Platform/graph-provider/pkg/model"
	platform "github.com/SENERGY-Platform/models/go/models"
	permissions "github.com/SENERGY-Platform/permissions-v2/pkg/client"
)

// acmeHarness is the scenario most cases start from: one group with a supply
// meter and a sub-meter that fits inside it.
func acmeHarness(t *testing.T, tune ...func(*harnessOptions)) *harness {
	t.Helper()
	h := newHarness(t, tune...)
	h.world.setGroups("/acme")
	h.world.meter("d1", "Hauptzaehler", 1000, "/acme")
	h.world.meter("d2", "Nebenzaehler", 400, "/acme")
	return h
}

// --- creation -----------------------------------------------------------------

// A group with no graph gets one, and it follows every convention the graph
// view reads a graph by. SPEC.md: "for each group there is a graph named after
// the group, every device of the group is in it".
func TestCreateGivesAGroupWithoutAGraphOne(t *testing.T) {
	h := acmeHarness(t)
	h.mustFull(t)

	graph := h.graphOfGroup(t, "/acme")
	if graph.Owner != serviceUser {
		t.Errorf("owner must be the service user, got %v", graph.Owner)
	}
	if got := graphs.AttributeOf(graph.Attributes, model.AttrKeycloakGroup); got != "/acme" {
		t.Errorf("expected the group path in %v, got %v", model.AttrKeycloakGroup, got)
	}
	if got := graphs.AttributeOf(graph.Attributes, model.AttrName); got != "acme" {
		t.Errorf("expected the written name in %v, got %v", model.AttrName, got)
	}

	root, found := graphs.RootOf(graph)
	if !found {
		t.Fatal("graph has no root")
	}
	if root.Id != model.RootNodeId {
		t.Errorf("the root node must be called %v, got %v", model.RootNodeId, root.Id)
	}
	if root.ResourceType != model.ResourceTypeCustom {
		t.Errorf("the root must be a custom node, got resource type %v", root.ResourceType)
	}
	if name := rootName(t, graph); name != "acme" {
		t.Errorf("the display name is the root's name attribute; expected acme, got %v", name)
	}

	if got := nodeIds(graph); !reflect.DeepEqual(got, []string{"d1", "d2", model.RootNodeId}) {
		t.Errorf("expected every device of the group as a node, got %v", got)
	}
	for _, node := range graph.Nodes {
		if node.Id == model.RootNodeId {
			continue
		}
		if node.ResourceId != node.Id {
			t.Errorf("device nodes use id == resource_id, got %v / %v", node.Id, node.ResourceId)
		}
		if node.ResourceType != platform.GraphResourceTypeDevice {
			t.Errorf("expected resource type %v on %v, got %v", platform.GraphResourceTypeDevice, node.Id, node.ResourceType)
		}
	}

	// The sub-meter sits inside the supply meter's reading, which is the whole
	// of the containment heuristic.
	if got := parentOf(graph, "d1"); got != model.RootNodeId {
		t.Errorf("the largest meter hangs off the root, got %v", got)
	}
	if got := parentOf(graph, "d2"); got != "d1" {
		t.Errorf("expected d2 under d1, got %v", got)
	}

	if err := graph.Valid(); err != nil {
		t.Errorf("a generated graph must satisfy the model's own validation: %v", err)
	}
}

// The group may edit the company graph but not delete or re-share it: r w x
// and explicitly not administrate.
func TestCreateSharesWithGroupWithoutAdministrate(t *testing.T) {
	h := acmeHarness(t)
	h.mustFull(t)

	graph := h.graphOfGroup(t, "/acme")
	perm, shared := h.world.groupPermission(graph.Id, "/acme")
	if !shared {
		t.Fatal("the new graph is not shared with its group")
	}
	want := permissions.PermissionsMap{Read: true, Write: true, Execute: true}
	if perm != want {
		t.Errorf("expected %+v for the group, got %+v", want, perm)
	}
	if perm.Administrate {
		t.Error("administrate stays with the owner")
	}

	owner, ok := h.world.userPermission(graph.Id, serviceUser)
	if !ok || !owner.Administrate {
		t.Errorf("the owner must keep administrate, got %+v", owner)
	}
}

// Sharing happens after the write: a graph has no permission resource until
// the repository created one, so a Share before the SetGraph could only fail.
func TestCreateSharesAfterTheWrite(t *testing.T) {
	h := acmeHarness(t)
	h.mustFull(t)

	calls := h.world.callOrder()
	write := slices.Index(calls, "SetGraph")
	share := slices.Index(calls, "SetPermission")
	if write < 0 || share < 0 {
		t.Fatalf("expected a write and a share, got %v", calls)
	}
	if write > share {
		t.Errorf("the graph must be written before it is shared, got %v", calls)
	}
}

// --- idempotence --------------------------------------------------------------

// Running the pass twice writes once. This is what stops the service from
// chasing the Kafka echo of its own writes.
func TestSecondPassWritesNothing(t *testing.T) {
	h := acmeHarness(t)
	h.mustFull(t)
	if got := h.world.countCalls("SetGraph"); got != 1 {
		t.Fatalf("expected one graph write on creation, got %v", got)
	}

	h.world.resetCalls()
	h.mustFull(t)

	if got := h.world.countCalls("SetGraph"); got != 0 {
		t.Errorf("the second pass must not write a graph, got %v writes", got)
	}
	if got := h.world.countCalls("SetPermission"); got != 0 {
		t.Errorf("the second pass must not write permissions, got %v writes", got)
	}
}

// Sharing is checked every pass whether or not the graph changed: only the
// permissions are rewritten when only the sharing drifted.
func TestDriftedSharingIsRestoredWithoutRewritingTheGraph(t *testing.T) {
	h := acmeHarness(t)
	h.mustFull(t)
	graph := h.graphOfGroup(t, "/acme")

	h.world.dropGroupPermission(graph.Id, "/acme")
	h.world.resetCalls()
	h.mustFull(t)

	if got := h.world.countCalls("SetPermission"); got != 1 {
		t.Errorf("expected the sharing to be restored once, got %v writes", got)
	}
	if got := h.world.countCalls("SetGraph"); got != 0 {
		t.Errorf("nothing about the graph changed, got %v graph writes", got)
	}
	if perm, shared := h.world.groupPermission(graph.Id, "/acme"); !shared || !perm.Write {
		t.Errorf("expected the group's rights back, got %+v (shared=%v)", perm, shared)
	}
}

// --- add only -----------------------------------------------------------------

// A new device is attached and the edges that were there stay where a user put
// them: "Existing edges are never re-parented, even when newer readings suggest
// a better tree."
func TestArrivalLeavesAManualMoveAlone(t *testing.T) {
	h := acmeHarness(t)
	h.mustFull(t)

	// A user moves the sub-meter out from under the supply meter, by hand.
	graph := h.graphOfGroup(t, "/acme")
	for i, edge := range graph.Edges {
		if edge.FromNodeId == "d2" {
			graph.Edges[i] = platform.Edge{
				Id:         "manual-d2-root",
				FromNodeId: "d2",
				ToNodeId:   model.RootNodeId,
				Weight:     model.FullWeight,
				Attributes: []platform.Attribute{},
			}
		}
	}
	if err := graph.Valid(); err != nil {
		t.Fatalf("the hand-edited graph is not a valid starting point: %v", err)
	}
	h.world.putGraph(graph)

	h.world.meter("d3", "Drittzaehler", 100, "/acme")
	h.world.resetCalls()
	h.mustFull(t)

	after := h.graphOfGroup(t, "/acme")
	moved, found := edgeById(after, "manual-d2-root")
	if !found {
		t.Fatalf("the user's edge is gone, edges are now %+v", after.Edges)
	}
	if moved.ToNodeId != model.RootNodeId {
		t.Errorf("the moved device must stay where it was put, now points at %v", moved.ToNodeId)
	}
	if _, stillThere := edgeById(after, "edge-d1-root"); !stillThere {
		t.Error("an edge that was already there was rewritten")
	}
	if parentOf(after, "d3") == "" {
		t.Error("the new device was not attached")
	}
	if got := h.world.countCalls("SetGraph"); got != 1 {
		t.Errorf("expected exactly one write for the arrival, got %v", got)
	}
	if err := after.Valid(); err != nil {
		t.Errorf("graph invalid after the attach: %v", err)
	}
}

// A device that left the group loses its node, and the model's rerouting keeps
// the graph valid.
func TestDepartureRemovesTheNodeAndKeepsTheGraphValid(t *testing.T) {
	h := acmeHarness(t)
	h.mustFull(t)
	before := h.graphOfGroup(t, "/acme")
	if parentOf(before, "d2") != "d1" {
		t.Fatalf("this case needs d2 under d1, got %v", parentOf(before, "d2"))
	}

	h.world.setDeviceGroups("d1")
	h.world.resetCalls()
	h.mustFull(t)

	after := h.graphOfGroup(t, "/acme")
	if got := nodeIds(after); !reflect.DeepEqual(got, []string{"d2", model.RootNodeId}) {
		t.Errorf("expected the departed device's node to be gone, got %v", got)
	}
	// The edge id is a fresh uuid from the model's rerouting, so only the
	// structure can be asserted on.
	if got := parentOf(after, "d2"); got != model.RootNodeId {
		t.Errorf("expected the orphaned child rerouted to the root, got %v", got)
	}
	if err := after.Valid(); err != nil {
		t.Errorf("graph invalid after the detach: %v", err)
	}
	if got := h.world.countCalls("SetGraph"); got != 1 {
		t.Errorf("expected exactly one write for the departure, got %v", got)
	}
}

// A departure and an arrival in the same pass are one write, not two.
func TestDepartureAndArrivalInOnePassWriteOnce(t *testing.T) {
	h := acmeHarness(t)
	h.mustFull(t)

	h.world.setDeviceGroups("d2")
	h.world.meter("d3", "Drittzaehler", 100, "/acme")
	h.world.resetCalls()
	h.mustFull(t)

	if got := h.world.countCalls("SetGraph"); got != 1 {
		t.Errorf("expected one write for both changes, got %v", got)
	}
	after := h.graphOfGroup(t, "/acme")
	if got := nodeIds(after); !reflect.DeepEqual(got, []string{"d1", "d3", model.RootNodeId}) {
		t.Errorf("expected d1 and d3 only, got %v", got)
	}
	if err := after.Valid(); err != nil {
		t.Errorf("graph invalid: %v", err)
	}
}

// --- rename -------------------------------------------------------------------

// renameHarness seeds a graph whose recorded name is stale, which is the state
// a rename leaves behind, and one device so nothing else about the graph
// changes.
func renameHarness(t *testing.T, recordedName string, currentRootName string) *harness {
	t.Helper()
	h := newHarness(t)
	h.world.setGroups("/acme")
	h.world.meter("d1", "Hauptzaehler", 1000, "/acme")
	h.world.putGraph(seededGraph("/acme", recordedName, currentRootName, "d1"), "/acme")
	return h
}

// The rename is followed while the root still carries the name the service
// wrote, and the record follows with it.
func TestRenameFollowedWhileRootCarriesTheGeneratedName(t *testing.T) {
	h := renameHarness(t, "alte-firma", "alte-firma")
	h.world.resetCalls()
	h.mustFull(t)

	graph := h.graphOfGroup(t, "/acme")
	if name := rootName(t, graph); name != "acme" {
		t.Errorf("expected the root renamed to acme, got %v", name)
	}
	if got := graphs.AttributeOf(graph.Attributes, model.AttrName); got != "acme" {
		t.Errorf("expected the record to follow, got %v", got)
	}
	if got := h.world.countCalls("SetGraph"); got != 1 {
		t.Errorf("expected exactly one write, got %v", got)
	}
}

// A name a user chose stands, but the record is still brought up to date, so
// the next pass does not argue about it again.
func TestRenameLeavesAUserNameAndStillUpdatesTheRecord(t *testing.T) {
	h := renameHarness(t, "alte-firma", "Werk Nord")
	h.world.resetCalls()
	h.mustFull(t)

	graph := h.graphOfGroup(t, "/acme")
	if name := rootName(t, graph); name != "Werk Nord" {
		t.Errorf("the user's name must stand, got %v", name)
	}
	if got := graphs.AttributeOf(graph.Attributes, model.AttrName); got != "acme" {
		t.Errorf("expected the record brought up to date, got %v", got)
	}
	if got := h.world.countCalls("SetGraph"); got != 1 {
		t.Errorf("expected exactly one write, got %v", got)
	}

	// And the argument is over: the pass after it writes nothing.
	h.world.resetCalls()
	h.mustFull(t)
	if got := h.world.countCalls("SetGraph"); got != 0 {
		t.Errorf("the pass after a user rename must write nothing, got %v writes", got)
	}
	if got := h.world.countCalls("SetPermission"); got != 0 {
		t.Errorf("the pass after a user rename must not touch permissions, got %v writes", got)
	}
	if name := rootName(t, h.graphOfGroup(t, "/acme")); name != "Werk Nord" {
		t.Errorf("the user's name must still stand, got %v", name)
	}
}

// Neither the group nor the root changed: no write.
func TestRenameWritesNothingWhenNothingChanged(t *testing.T) {
	h := renameHarness(t, "acme", "acme")
	h.world.resetCalls()
	h.mustFull(t)

	if got := h.world.countCalls("SetGraph"); got != 0 {
		t.Errorf("expected no write, got %v", got)
	}
}

// --- orphans and filters ------------------------------------------------------

// A group that disappeared from Keycloak loses its access and its attribute.
// The graph itself stays: deleting a user's work in response to a directory
// event is the harsher of the two actions.
func TestOrphanedGraphIsUnsharedAndDetachedButKept(t *testing.T) {
	h := newHarness(t)
	// One group survives, and it already has its graph, so the only write this
	// pass has any reason to make is the orphan's. An empty group list is
	// deliberately NOT treated as every group having disappeared - see
	// TestAnEmptyGroupListOrphansNothing - so the realistic scenario is a
	// directory that still answers with the other companies.
	h.world.setGroups("/acme")
	h.world.putGraph(seededGraph("/acme", "acme", "acme", "d1"), "/acme")
	seeded := h.world.putGraph(seededGraph("/gone", "gone", "gone", "d9"), "/gone")

	h.world.resetCalls()
	h.mustFull(t)

	if got := h.world.countCalls("SetPermission"); got != 1 {
		t.Errorf("expected the group unshared once, got %v permission writes", got)
	}
	if perm, shared := h.world.groupPermission(seeded.Id, "/gone"); shared {
		t.Errorf("the vanished group must lose its access, still holds %+v", perm)
	}

	graph, exists := h.world.graph(seeded.Id)
	if !exists {
		t.Fatal("the graph must not be deleted")
	}
	if got := graphs.GroupPathOf(graph); got != "" {
		t.Errorf("expected the group attribute cleared, got %v", got)
	}
	if got := nodeIds(graph); !reflect.DeepEqual(got, nodeIds(seeded)) {
		t.Errorf("the nodes must be untouched, got %v want %v", got, nodeIds(seeded))
	}
	if _, found := edgeById(graph, "edge-d9-root"); !found {
		t.Errorf("the edges must be untouched, got %+v", graph.Edges)
	}

	// Nothing is left to do: without the attribute the graph is not a default
	// graph any more, so the next pass finds nothing.
	h.world.resetCalls()
	h.mustFull(t)
	if got := h.world.countCalls("SetGraph") + h.world.countCalls("SetPermission"); got != 0 {
		t.Errorf("the pass after detaching an orphan must write nothing, got %v writes", got)
	}
}

// A group the path filter rejects never gets a graph - that is what keeps
// platform-internal groups out.
func TestExcludedGroupNeverGetsAGraph(t *testing.T) {
	h := newHarness(t, rejectGroups("/platform"))
	h.world.setGroups("/acme", "/platform")
	h.world.meter("d1", "Hauptzaehler", 1000, "/acme")
	h.world.meter("d2", "Interner Zaehler", 500, "/platform")

	h.mustFull(t)

	if got := h.world.countCalls("SetGraph"); got != 1 {
		t.Errorf("expected exactly one graph written, got %v", got)
	}
	if _, exists := h.world.graphOfGroup("/platform"); exists {
		t.Error("an excluded group must not get a graph")
	}
	graph := h.graphOfGroup(t, "/acme")
	if got := nodeIds(graph); !reflect.DeepEqual(got, []string{"d1", model.RootNodeId}) {
		t.Errorf("expected only the allowed group's device, got %v", got)
	}
}

// --- triggers -----------------------------------------------------------------

// twoGroupHarness has one device in each of two groups, so a move between them
// is observable on both sides.
func twoGroupHarness(t *testing.T) *harness {
	t.Helper()
	h := newHarness(t)
	h.world.setGroups("/a", "/b")
	h.world.meter("d1", "Zaehler A", 1000, "/a")
	h.world.meter("d2", "Zaehler B", 400, "/b")
	return h
}

// A device trigger is resolved against the authoritative source and reconciles
// both the group the device left and the one it joined.
func TestDeviceTriggerReconcilesTheOldAndTheNewGroup(t *testing.T) {
	h := twoGroupHarness(t)
	h.mustFull(t)

	h.world.setDeviceGroups("d1", "/b")
	h.world.resetCalls()

	ctx := context.Background()
	h.rec.resolve(ctx, []events.Trigger{{Kind: events.KindDevice, Id: "d1"}})
	h.rec.drain(ctx)

	left := h.graphOfGroup(t, "/a")
	if got := nodeIds(left); !reflect.DeepEqual(got, []string{model.RootNodeId}) {
		t.Errorf("expected d1 gone from the group it left, got %v", got)
	}
	joined := h.graphOfGroup(t, "/b")
	if got := nodeIds(joined); !reflect.DeepEqual(got, []string{"d1", "d2", model.RootNodeId}) {
		t.Errorf("expected d1 in the group it joined, got %v", got)
	}
	if got := h.world.countCalls("SetGraph"); got != 2 {
		t.Errorf("expected one write per affected group, got %v", got)
	}
	if err := joined.Valid(); err != nil {
		t.Errorf("graph invalid after the move: %v", err)
	}
}

// A device-type trigger names the type, not the devices, so it fans out to
// every device of that type and to no other.
func TestDeviceTypeTriggerFansOutToItsDevices(t *testing.T) {
	h := newHarness(t)
	h.world.setGroups("/acme")
	h.world.addDevice("d1", "Zaehler 1", "dt1", electricityType("dt1"), "/acme")
	h.world.setReading("d1", model.Electricity, 1000)
	h.world.addDevice("d2", "Zaehler 2", "dt1", electricityType("dt1"), "/acme")
	h.world.setReading("d2", model.Electricity, 400)
	h.world.addDevice("d3", "Zaehler 3", "dt2", electricityType("dt2"), "/acme")
	h.world.setReading("d3", model.Electricity, 100)
	h.mustFull(t)

	h.world.resetCalls()
	h.rec.resolve(context.Background(), []events.Trigger{{Kind: events.KindDeviceType, Id: "dt1"}})

	want := [][]string{{"d1"}, {"d2"}}
	if got := h.world.listedIds(); !reflect.DeepEqual(got, want) {
		t.Errorf("expected every device of dt1 re-read and no other, got %v", got)
	}
}

// A graph trigger carries an id; which group the graph belongs to is written on
// the graph itself, so no local state is needed to resolve it.
func TestGraphTriggerResolvesTheGroupFromTheGraphsOwnAttribute(t *testing.T) {
	h := acmeHarness(t)
	h.mustFull(t)
	graph := h.graphOfGroup(t, "/acme")

	// Something the reconcile of that group, and only that group, would fix.
	h.world.dropGroupPermission(graph.Id, "/acme")
	h.world.resetCalls()

	ctx := context.Background()
	h.rec.resolve(ctx, []events.Trigger{{Kind: events.KindGraph, Id: graph.Id}})
	h.rec.drain(ctx)

	if got := h.world.countCalls("SetPermission"); got != 1 {
		t.Errorf("expected the group behind the graph reconciled once, got %v permission writes", got)
	}
	if got := h.world.countCalls("SetGraph"); got != 0 {
		t.Errorf("nothing about the graph changed, got %v graph writes", got)
	}
	if _, shared := h.world.groupPermission(graph.Id, "/acme"); !shared {
		t.Error("expected the sharing restored")
	}
}

// A trigger naming a device the repository no longer returns removes the node
// from the graph the device was in.
func TestDeviceTriggerForAVanishedDeviceRemovesItsNode(t *testing.T) {
	h := acmeHarness(t)
	h.mustFull(t)

	h.world.removeDevice("d2")
	h.world.resetCalls()

	ctx := context.Background()
	h.rec.resolve(ctx, []events.Trigger{{Kind: events.KindDevice, Id: "d2"}})
	h.rec.drain(ctx)

	after := h.graphOfGroup(t, "/acme")
	if got := nodeIds(after); !reflect.DeepEqual(got, []string{"d1", model.RootNodeId}) {
		t.Errorf("expected the vanished device's node removed, got %v", got)
	}
	if got := h.world.countCalls("SetGraph"); got != 1 {
		t.Errorf("expected exactly one write, got %v", got)
	}
	if err := after.Valid(); err != nil {
		t.Errorf("graph invalid: %v", err)
	}
}

// --- kill switch --------------------------------------------------------------

// With writing disabled the pass still reads and computes, and nothing leaves
// the process.
//
// The skipped writes are not an error. A kill switch somebody set is an
// expected condition, and an error here reaches the caller's Error log, which
// in this organisation notifies a channel - so the configured state would page
// somebody once an hour for as long as it holds. What the pass would have
// written is reported through Config.Planned instead; see
// TestReadOnlyPassReportsWhatItWouldWrite.
func TestReadOnlyStoreWritesNothing(t *testing.T) {
	h := newHarness(t, writeDisabled)
	h.world.setGroups("/acme", "/beta")
	h.world.meter("d1", "Hauptzaehler", 1000, "/acme")
	// /beta already has its graph but lost the group's rights: the other of
	// the two writes a pass can want to make.
	seeded := h.world.putGraph(seededGraph("/beta", "beta", "beta"))

	if err := h.rec.Full(context.Background()); err != nil {
		t.Fatalf("a configured kill switch must not fail the pass: %v", err)
	}
	if got := h.world.countCalls("SetGraph"); got != 0 {
		t.Errorf("the kill switch must stop every graph write, got %v", got)
	}
	if got := h.world.countCalls("SetPermission"); got != 0 {
		t.Errorf("the kill switch must stop every permission write, got %v", got)
	}
	if got := h.world.graphCount(); got != 1 {
		t.Errorf("expected only the seeded graph to exist, got %v graphs", got)
	}
	if _, exists := h.world.graphOfGroup("/acme"); exists {
		t.Error("no graph may be created with writing disabled")
	}
	if _, shared := h.world.groupPermission(seeded.Id, "/beta"); shared {
		t.Error("no sharing may be written with writing disabled")
	}
}

// The point of a dry run: with writing disabled the pass says what it would
// have written, in full, and writes nothing.
func TestReadOnlyPassReportsWhatItWouldWrite(t *testing.T) {
	var planned []model.PlannedChange
	h := newHarness(t, writeDisabled, collectPlanned(&planned))
	h.world.setGroups("/acme", "/beta")
	h.world.meter("d1", "Hauptzaehler", 1000, "/acme")
	h.world.meter("d2", "Halle Ost", 400, "/acme")
	// A device whose meter never reported: it hangs off the root, and the
	// report has to be able to say why.
	h.world.addDevice("d3", "Kompressor", "dt1", electricityType("dt1"), "/acme")
	h.world.putGraph(seededGraph("/beta", "beta", "beta"))

	if err := h.rec.Full(context.Background()); err != nil {
		t.Fatalf("full pass failed: %v", err)
	}
	if got := h.world.countCalls("SetGraph"); got != 0 {
		t.Errorf("a reported pass must still write nothing, got %v graph writes", got)
	}
	if got := h.world.countCalls("SetPermission"); got != 0 {
		t.Errorf("a reported pass must still write nothing, got %v permission writes", got)
	}

	created := changesOfAction(planned, model.ActionCreate)
	if len(created) != 1 {
		t.Fatalf("expected one reported creation, got %v of %v changes", len(created), len(planned))
	}
	change := created[0]
	if change.Group.Path != "/acme" {
		t.Errorf("expected the creation to name /acme, got %v", change.Group.Path)
	}
	if got := len(graphs.DeviceIdsIn(change.Graph)); got != 3 {
		t.Errorf("expected all three devices in the reported graph, got %v", got)
	}
	if err := change.Graph.Valid(); err != nil {
		t.Errorf("a reported graph must be one the repository would accept: %v", err)
	}
	if len(change.Devices) != 3 {
		t.Errorf("expected the devices to travel with the change, got %v", len(change.Devices))
	}
	if !slices.Equal(change.NoReading, []string{"d3"}) {
		t.Errorf("expected d3 to be reported as having no reading, got %v", change.NoReading)
	}
	if change.Note == "" {
		t.Error("expected the change to say what it is in words")
	}

	// /beta's graph is unchanged but its sharing is not: the report has to
	// name the permission write too, or a reader concludes nothing would
	// happen to that group.
	shares := changesOfAction(planned, model.ActionShare)
	if len(shares) != 1 || shares[0].Group.Path != "/beta" {
		t.Errorf("expected one reported share for /beta, got %v", shares)
	}
}

// The hook observes, it does not gate: with writing enabled the changes are
// still reported and the writes still happen.
func TestPlannedChangesAreReportedWhileWriting(t *testing.T) {
	var planned []model.PlannedChange
	h := newHarness(t, collectPlanned(&planned))
	h.world.setGroups("/acme")
	h.world.meter("d1", "Hauptzaehler", 1000, "/acme")

	h.mustFull(t)

	if got := h.world.countCalls("SetGraph"); got != 1 {
		t.Errorf("expected the graph to be written once, got %v", got)
	}
	graph := h.graphOfGroup(t, "/acme")
	if _, shared := h.world.groupPermission(graph.Id, "/acme"); !shared {
		t.Error("expected the graph to be shared with its group")
	}
	if len(changesOfAction(planned, model.ActionCreate)) != 1 {
		t.Errorf("expected the creation to be reported, got %v", planned)
	}
	if len(changesOfAction(planned, model.ActionShare)) != 1 {
		t.Errorf("expected the sharing to be reported, got %v", planned)
	}

	// Idempotence: the second pass has nothing to write, so it has nothing to
	// report either. A hook that fired anyway would make a report of a settled
	// platform unreadable.
	planned = nil
	h.mustFull(t)
	if len(planned) != 0 {
		t.Errorf("a pass that changes nothing must report nothing, got %v", planned)
	}
}

// changesOfAction is the reported changes carrying one action.
func changesOfAction(changes []model.PlannedChange, action string) []model.PlannedChange {
	result := []model.PlannedChange{}
	for _, change := range changes {
		if change.Action == action {
			result = append(result, change)
		}
	}
	return result
}

// --- concurrency and the loop -------------------------------------------------

// The request methods are called from an HTTP handler and from a Kafka
// consumer; neither may be made to wait for a pass.
func TestRequestsDoNotBlock(t *testing.T) {
	h := newHarness(t)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			h.rec.RequestGroup("/acme")
			h.rec.RequestGroup("/beta")
			h.rec.RequestAll()
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RequestAll/RequestGroup blocked")
	}
}

// Pending counts both queued group requests and queued triggers, because both
// are work the loop has not done yet.
func TestPendingCountsRequestsAndTriggers(t *testing.T) {
	h := newHarness(t)

	h.rec.RequestGroup("/a")
	h.rec.RequestGroup("/b")
	// The same group twice is one piece of work.
	h.rec.RequestGroup("/b")
	h.rec.RequestAll()
	h.pending.Add(events.Trigger{Kind: events.KindDevice, Id: "d1"})
	h.pending.Add(events.Trigger{Kind: events.KindGraph, Id: "g1"})

	if got := h.rec.Pending(); got != 5 {
		t.Errorf("expected 2 groups + 1 full + 2 triggers = 5, got %v", got)
	}
}

// Run does a full pass first - that is what covers whatever happened while the
// service was down - and a trigger that arrives afterwards is picked up.
func TestRunDrivesAnInitialPassAndPicksUpLaterTriggers(t *testing.T) {
	h := acmeHarness(t, intervals(time.Hour, time.Hour))

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	h.rec.Run(ctx, &wg)
	defer func() {
		cancel()
		wg.Wait()
	}()

	waitFor(t, 5*time.Second, "the initial full pass", func() bool {
		_, exists := h.world.graphOfGroup("/acme")
		return exists
	})

	// Nothing but the trigger can drive this: both tickers are an hour out.
	h.world.meter("d3", "Drittzaehler", 100, "/acme")
	h.pending.Add(events.Trigger{Kind: events.KindDevice, Id: "d3"})

	waitFor(t, 5*time.Second, "the trigger to be worked off", func() bool {
		graph, exists := h.world.graphOfGroup("/acme")
		return exists && slices.Contains(nodeIds(graph), "d3")
	})
}

// The safety-net ticker keeps running, and every pass after the first finds
// nothing to do: the loop terminates instead of writing forever.
func TestSafetyNetKeepsPassingWithoutWriting(t *testing.T) {
	h := acmeHarness(t, intervals(10*time.Millisecond, 10*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	h.rec.Run(ctx, &wg)
	defer func() {
		cancel()
		wg.Wait()
	}()

	waitFor(t, 5*time.Second, "several full passes", func() bool {
		return h.world.countCalls("ListGraphs") >= 4
	})
	if got := h.world.countCalls("SetGraph"); got != 1 {
		t.Errorf("expected the creating write and nothing after it, got %v writes", got)
	}
}

// --- readings -----------------------------------------------------------------

// A reading is fetched once and served from the cache within the ttl, even
// across passes and across groups that share a device.
func TestReadingsAreFetchedOnceWithinTheTtl(t *testing.T) {
	h := newHarness(t)
	h.world.setGroups("/a", "/b")
	// Both devices belong to both groups, so both graphs are built from the
	// same readings.
	h.world.meter("d1", "Hauptzaehler", 1000, "/a", "/b")
	h.world.meter("d2", "Nebenzaehler", 400, "/a", "/b")

	h.mustFull(t)
	h.mustFull(t)

	if got := h.world.countCalls("Fetch"); got != 1 {
		t.Errorf("expected the readings fetched once and reused, got %v fetches", got)
	}
	if _, exists := h.world.graphOfGroup("/a"); !exists {
		t.Error("expected a graph for /a")
	}
	if _, exists := h.world.graphOfGroup("/b"); !exists {
		t.Error("expected a graph for /b")
	}
}

// Every device of one structure is asked about over the same window: figures
// read over different stretches are not comparable, and comparing them is the
// whole of the heuristic.
func TestOneWindowForEveryDeviceOfAPass(t *testing.T) {
	h := newHarness(t)
	h.world.setGroups("/a", "/b")
	h.world.meter("d1", "A1", 1000, "/a")
	h.world.meter("d2", "A2", 400, "/a")
	h.world.meter("d3", "B1", 800, "/b")
	h.world.meter("d4", "B2", 300, "/b")

	h.mustFull(t)

	windows := h.world.windows()
	if len(windows) < 2 {
		t.Fatalf("expected a fetch per group of devices, got %v", windows)
	}
	for _, window := range windows {
		if window != expectedWindow {
			t.Errorf("expected every fetch over %+v, got %+v", expectedWindow, window)
		}
	}
	// Both groups' devices were asked about, so the identical window is a
	// statement about all four devices and not about one call.
	asked := h.world.askedDevices()
	for _, id := range []string{"d1", "d2", "d3", "d4"} {
		if !asked[id] {
			t.Errorf("device %v was never asked about", id)
		}
	}
}

// --- the group tree -----------------------------------------------------------

// A group with no devices gets no graph. A graph holding nothing but its root
// says nothing, and most groups in a realm own no meter at all.
func TestGroupWithoutDevicesGetsNoGraph(t *testing.T) {
	h := newHarness(t)
	h.world.setGroups("/leer", "/acme")
	h.world.meter("d1", "Hauptzaehler", 1000, "/acme")

	h.mustFull(t)

	if _, exists := h.world.graphOfGroup("/leer"); exists {
		t.Error("a group without devices must not get a graph")
	}
	if got := h.world.countCalls("SetGraph"); got != 1 {
		t.Errorf("expected only the group with a device written, got %v writes", got)
	}
	// Not a permanent verdict: the graph is created as soon as the group has
	// something to put in it.
	h.world.meter("d2", "Zaehler Leer", 700, "/leer")
	h.mustFull(t)

	graph := h.graphOfGroup(t, "/leer")
	if got := nodeIds(graph); !reflect.DeepEqual(got, []string{"d2", model.RootNodeId}) {
		t.Errorf("expected the root and the new device, got %v", got)
	}
	if _, shared := h.world.groupPermission(graph.Id, "/leer"); !shared {
		t.Error("the graph is shared with its group like any other")
	}
}

// An existing graph is not withdrawn when its last device leaves. Removing a
// structure a user may have edited is the harsher of the two actions, and the
// same reasoning already governs a graph whose group is gone.
func TestGraphSurvivesItsLastDeviceLeaving(t *testing.T) {
	h := newHarness(t)
	h.world.setGroups("/acme")
	h.world.meter("d1", "Hauptzaehler", 1000, "/acme")
	h.mustFull(t)
	before := h.graphOfGroup(t, "/acme")

	h.world.removeDevice("d1")
	h.mustFull(t)

	after := h.graphOfGroup(t, "/acme")
	if after.Id != before.Id {
		t.Errorf("expected the same graph, got %v instead of %v", after.Id, before.Id)
	}
	if got := nodeIds(after); !reflect.DeepEqual(got, []string{model.RootNodeId}) {
		t.Errorf("expected the root and nothing else, got %v", got)
	}
}

// Group CRUD is polled, because the realm has no admin event listener. A group
// that appears between two polls gets its graph from the poll.
func TestGroupPollPicksUpANewGroup(t *testing.T) {
	h := newHarness(t, intervals(5*time.Millisecond, time.Hour))
	h.world.setGroups("/acme")
	h.world.meter("d1", "Hauptzaehler", 1000, "/acme")
	// The device is there before the group is. Without one the new group would
	// get no graph, and the group poll refreshes groups, not devices.
	h.world.meter("d2", "Zaehler Neu", 700, "/neu")

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	h.rec.Run(ctx, &wg)
	defer func() {
		cancel()
		wg.Wait()
	}()

	waitFor(t, 5*time.Second, "the initial full pass", func() bool {
		_, exists := h.world.graphOfGroup("/acme")
		return exists
	})

	// The safety net is an hour out, so only the group poll can find this.
	h.world.setGroups("/acme", "/neu")
	waitFor(t, 5*time.Second, "the new group's graph", func() bool {
		_, exists := h.world.graphOfGroup("/neu")
		return exists
	})
}

// A directory that answers successfully with nothing is not a directory in
// which every company has been deleted. A misconfigured realm, a filter that
// matches nothing or a client that lost its roles would otherwise cost every
// company its access in one pass.
func TestAnEmptyGroupListOrphansNothing(t *testing.T) {
	h := newHarness(t)
	h.world.setGroups()
	seeded := h.world.putGraph(seededGraph("/acme", "acme", "acme", "d9"), "/acme")

	h.world.resetCalls()
	h.mustFull(t)

	if got := h.world.countCalls("SetPermission"); got != 0 {
		t.Errorf("expected no permission write, got %v", got)
	}
	if got := h.world.countCalls("SetGraph"); got != 0 {
		t.Errorf("expected no graph write, got %v", got)
	}
	if _, shared := h.world.groupPermission(seeded.Id, "/acme"); !shared {
		t.Error("the group must keep its access")
	}
	stored, found := h.world.graph(seeded.Id)
	if !found {
		t.Fatal("the graph must still be there")
	}
	if got := graphs.GroupPathOf(stored); got != "/acme" {
		t.Errorf("the group attribute must stay, got %q", got)
	}
}

// A graph carrying only the path attribute is adopted, not duplicated: the id
// is written once and the pass after that has nothing left to do.
func TestAnIdlessGraphIsAdoptedOnce(t *testing.T) {
	h := newHarness(t)
	h.world.setGroups("/acme")
	legacy := seededGraph("/acme", "acme", "acme", "d1")
	legacy.Attributes = slices.DeleteFunc(legacy.Attributes, func(a platform.Attribute) bool {
		return a.Key == model.AttrKeycloakGroupId
	})
	seeded := h.world.putGraph(legacy, "/acme")

	h.world.resetCalls()
	h.mustFull(t)

	if got := h.world.countCalls("SetGraph"); got != 1 {
		t.Fatalf("expected the adoption to be one write, got %v", got)
	}
	if got := h.world.graphCount(); got != 1 {
		t.Errorf("a second graph must not be created, got %v graphs", got)
	}
	stored, _ := h.world.graph(seeded.Id)
	if got := graphs.GroupIdOf(stored); got != "kc/acme" {
		t.Errorf("expected the group id written, got %q", got)
	}

	h.world.resetCalls()
	h.mustFull(t)
	if got := h.world.countCalls("SetGraph"); got != 0 {
		t.Errorf("the pass after adoption must write nothing, got %v", got)
	}
}

// A renamed group keeps its graph. Keycloak rewrites the path on a rename, so
// a graph identified by path would be orphaned and a second one created - and
// the first carries everything the user had built.
func TestARenamedGroupKeepsItsGraphAndFollowsTheSharing(t *testing.T) {
	h := newHarness(t)
	h.world.setGroups("/acme")
	h.world.addDevice("d1", "Zaehler 1", "dt1", electricityType("dt1"), "/acme")
	seeded := h.world.putGraph(seededGraph("/acme", "acme", "acme", "d1"), "/acme")
	h.mustFull(t)
	if len(graphs.DeviceIdsIn(mustGraph(t, h, seeded.Id))) != 1 {
		t.Fatal("the fixture must start with its device in the graph")
	}

	// The same Keycloak group under a new path: this is what a rename looks
	// like from the admin API. permissions-v2 still keys the device's rights
	// by the old path - Keycloak does not tell it - so the device now looks
	// like one that left the company.
	h.world.setGroupsWithIds(map[string]string{"/acme-gmbh": "kc/acme"})
	h.world.resetCalls()
	h.mustFull(t)

	if got := h.world.graphCount(); got != 1 {
		t.Fatalf("the rename must not create a second graph, got %v", got)
	}
	stored, found := h.world.graph(seeded.Id)
	if !found {
		t.Fatal("the graph must still be there")
	}
	if got := graphs.GroupPathOf(stored); got != "/acme-gmbh" {
		t.Errorf("expected the path attribute to follow, got %q", got)
	}
	if got := rootName(t, stored); got != "acme-gmbh" {
		t.Errorf("expected the root renamed, got %q", got)
	}
	if _, shared := h.world.groupPermission(seeded.Id, "/acme-gmbh"); !shared {
		t.Error("the new path must be shared")
	}
	if _, shared := h.world.groupPermission(seeded.Id, "/acme"); shared {
		t.Error("the old path must be unshared")
	}
	if len(graphs.DeviceIdsIn(stored)) != 1 {
		t.Errorf("the devices must survive a rename, got %+v", graphs.DeviceIdsIn(stored))
	}
}

func mustGraph(t *testing.T, h *harness, id string) platform.Graph {
	t.Helper()
	graph, found := h.world.graph(id)
	if !found {
		t.Fatalf("graph %v is gone", id)
	}
	return graph
}
