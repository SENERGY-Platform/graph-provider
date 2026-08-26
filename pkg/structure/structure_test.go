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

package structure

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/SENERGY-Platform/graph-provider/pkg/model"
	"github.com/SENERGY-Platform/models/go/models"
)

const owner = "service-user"

var group = model.Group{Id: "group-id", Path: "/site/plant", Name: "plant"}

// options is the deterministic variant every test uses: no uuid anywhere, so
// a graph can be compared field by field.
func options(tolerance float64) Options {
	next := 0
	return Options{
		Tolerance: tolerance,
		NewId: func() string {
			next++
			return fmt.Sprintf("generated-%v", next)
		},
	}
}

// device is a meter that reads the given carriers. The columns themselves do
// not matter here - only whether there are any, which is what
// HasEnergyFlow answers.
func device(id string, name string, carriers ...model.Carrier) model.Device {
	result := model.Device{Id: id, Name: name}
	for _, carrier := range carriers {
		result.Columns = append(result.Columns, model.CarrierColumn{
			ServiceId: "service-" + id,
			Name:      "value",
			Carrier:   carrier,
		})
	}
	return result
}

type reading struct {
	deviceId string
	carrier  model.Carrier
	value    float64
}

func readingsOf(entries ...reading) map[model.ReadingKey]model.Reading {
	result := map[model.ReadingKey]model.Reading{}
	for _, entry := range entries {
		result[model.ReadingKey{DeviceId: entry.deviceId, Carrier: entry.carrier}] = model.Reading{
			DeviceId: entry.deviceId,
			Carrier:  entry.carrier,
			Value:    entry.value,
		}
	}
	return result
}

// mustValid is the check the device repository would otherwise answer with a
// 400. Every graph any test produces goes through it.
func mustValid(t *testing.T, graph models.Graph) {
	t.Helper()
	if err := graph.Valid(); err != nil {
		t.Fatalf("graph is invalid: %v\n%#v", err, graph)
	}
}

// parents maps node id -> parent node id.
func parents(graph models.Graph) map[string]string {
	result := map[string]string{}
	for _, edge := range graph.Edges {
		result[edge.FromNodeId] = edge.ToNodeId
	}
	return result
}

func children(graph models.Graph, nodeId string) []string {
	result := []string{}
	for _, edge := range graph.Edges {
		if edge.ToNodeId == nodeId {
			result = append(result, edge.FromNodeId)
		}
	}
	sort.Strings(result)
	return result
}

func edgeById(graph models.Graph, id string) (models.Edge, bool) {
	for _, edge := range graph.Edges {
		if edge.Id == id {
			return edge, true
		}
	}
	return models.Edge{}, false
}

func hasAttribute(attributes []models.Attribute, key string, value string) bool {
	for _, attribute := range attributes {
		if attribute.Key == key && attribute.Value == value {
			return true
		}
	}
	return false
}

func mustBuild(t *testing.T, devices []model.Device, readings map[model.ReadingKey]model.Reading, opts Options) models.Graph {
	t.Helper()
	graph, _, err := Build(owner, group, devices, readings, opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	mustValid(t, graph)
	return graph
}

// mustBuildPlaced is mustBuild for the tests that check the reasoning behind
// an edge and not only the edge: the placements say which meters were weighed,
// what each of them had left and which of them won.
func mustBuildPlaced(t *testing.T, devices []model.Device, readings map[model.ReadingKey]model.Reading, opts Options) (models.Graph, []model.Placement) {
	t.Helper()
	graph, placements, err := Build(owner, group, devices, readings, opts)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	mustValid(t, graph)
	return graph, placements
}

func placementOf(t *testing.T, placements []model.Placement, deviceId string) model.Placement {
	t.Helper()
	for _, placement := range placements {
		if placement.DeviceId == deviceId {
			return placement
		}
	}
	t.Fatalf("no placement recorded for %v: %#v", deviceId, placements)
	return model.Placement{}
}

func candidateOf(t *testing.T, placement model.Placement, deviceId string) model.Candidate {
	t.Helper()
	for _, candidate := range placement.Candidates {
		if candidate.DeviceId == deviceId {
			return candidate
		}
	}
	t.Fatalf("%v was not weighed as a parent of %v: %#v", deviceId, placement.DeviceId, placement.Candidates)
	return model.Candidate{}
}

// levels counts the nodes on the path from a node up to and including the
// root, so the root itself is 1 and a child of the root is 2. Zero means the
// node does not reach the root, which Graph.Valid would have refused.
func levels(graph models.Graph, nodeId string) int {
	parentOf := parents(graph)
	for count := 1; count <= len(graph.Nodes); count++ {
		if nodeId == model.RootNodeId {
			return count
		}
		next, ok := parentOf[nodeId]
		if !ok {
			return 0
		}
		nodeId = next
	}
	return 0
}

func TestBuildRootAndGraphAttributes(t *testing.T) {
	graph := mustBuild(t, nil, nil, options(0.05))

	if graph.Id != "" {
		t.Errorf("graph id should be left to the caller, got %v", graph.Id)
	}
	if graph.Owner != owner {
		t.Errorf("owner: got %v", graph.Owner)
	}
	if !hasAttribute(graph.Attributes, model.AttrKeycloakGroup, group.Path) {
		t.Errorf("missing group attribute: %#v", graph.Attributes)
	}
	if !hasAttribute(graph.Attributes, model.AttrName, group.Name) {
		t.Errorf("missing name attribute: %#v", graph.Attributes)
	}
	for _, attribute := range graph.Attributes {
		if attribute.Origin != model.AttrOrigin {
			t.Errorf("attribute %v has origin %v", attribute.Key, attribute.Origin)
		}
	}

	if len(graph.Nodes) != 1 {
		t.Fatalf("expected only the root node, got %#v", graph.Nodes)
	}
	root := graph.Nodes[0]
	if root.Id != model.RootNodeId || root.ResourceType != model.ResourceTypeCustom || root.ResourceId != "" {
		t.Errorf("root node: %#v", root)
	}
	if !hasAttribute(root.Attributes, model.NodeAttrName, group.Name) {
		t.Errorf("root name attribute: %#v", root.Attributes)
	}
	if len(graph.Edges) != 0 {
		t.Errorf("expected no edges, got %#v", graph.Edges)
	}
}

func TestBuildDeviceNodesAndEdges(t *testing.T) {
	devices := []model.Device{
		device("a", "meter a", model.Electricity),
		device("b", "meter b", model.Electricity),
	}
	graph := mustBuild(t, devices, readingsOf(
		reading{"a", model.Electricity, 1000},
		reading{"b", model.Electricity, 600},
	), options(0.05))

	for _, node := range graph.Nodes {
		if node.Id == model.RootNodeId {
			continue
		}
		if node.ResourceId != node.Id {
			t.Errorf("device node must carry id == resource_id: %#v", node)
		}
		if node.ResourceType != models.GraphResourceTypeDevice {
			t.Errorf("device node resource type: %#v", node)
		}
	}
	for _, edge := range graph.Edges {
		if edge.Weight != model.FullWeight {
			t.Errorf("edge %v weight %v", edge.Id, edge.Weight)
		}
		if edge.Id != "edge-"+edge.FromNodeId+"-"+edge.ToNodeId {
			t.Errorf("unexpected edge id shape: %v", edge.Id)
		}
	}
	if got := parents(graph)["b"]; got != "a" {
		t.Errorf("b should sit under a, got %v", got)
	}
	if got := parents(graph)["a"]; got != model.RootNodeId {
		t.Errorf("the largest device belongs to the root, got %v", got)
	}
}

// Depth where the capacity forces it, which is the counter-case to the flat
// trees below: the 500 no longer fits into the 400 the supply meter has left
// after the 600, so the 600 is the only candidate and a third level is not a
// guess but the only reading of the numbers.
func TestBuildThreeLevelChainWhereCapacityForcesIt(t *testing.T) {
	devices := []model.Device{
		device("supply", "plant supply", model.Electricity),
		device("distributor", "plant distributor", model.Electricity),
		device("strip", "plant strip", model.Electricity),
	}
	graph := mustBuild(t, devices, readingsOf(
		reading{"supply", model.Electricity, 1000},
		reading{"distributor", model.Electricity, 600},
		reading{"strip", model.Electricity, 500},
	), options(0.05))

	got := parents(graph)
	want := map[string]string{
		"supply":      model.RootNodeId,
		"distributor": "supply",
		"strip":       "distributor",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("expected a chain, got %#v", got)
	}
}

// The rule in its own right: a 200 fits into both the 400 the supply meter has
// left and the whole 600 of the distributor, and the roomier of the two wins.
// The tighter candidate is a real candidate here - it fits, it is reported as
// fitting, and it loses - which is the difference to the case above, where the
// capacity left no choice.
//
// The shallow candidate wins even though the deeper one has more room left.
// Capacity only decides eligibility; among eligible parents the shallowest is
// taken, because a level has to be forced before it is invented. See
// shallower's doc comment for why both capacity-ordered rules chained.
func TestBuildPrefersTheShallowestParentThatFits(t *testing.T) {
	devices := []model.Device{
		device("supply", "plant supply", model.Electricity),
		device("distributor", "plant distributor", model.Electricity),
		device("strip", "plant strip", model.Electricity),
	}
	graph, placements := mustBuildPlaced(t, devices, readingsOf(
		reading{"supply", model.Electricity, 1000},
		reading{"distributor", model.Electricity, 600},
		reading{"strip", model.Electricity, 200},
	), options(0.05))

	if got := parents(graph)["strip"]; got != "supply" {
		t.Errorf("the supply meter is the shallowest parent with room, expected it, got %v", got)
	}
	strip := placementOf(t, placements, "strip")
	// Both were eligible. The deeper one had more room and still lost, which
	// is the whole point of the rule.
	if supply := candidateOf(t, strip, "supply"); supply.Remaining != 400 || !supply.Fits || !supply.Chosen {
		t.Errorf("the supply meter had 400 left, fits, and should have won, got %#v", supply)
	}
	if distributor := candidateOf(t, strip, "distributor"); distributor.Remaining != 600 || !distributor.Fits || distributor.Chosen {
		t.Errorf("the distributor had more room and must still lose, got %#v", distributor)
	}
	if strip.Reason != model.PlacedByCapacity {
		t.Errorf("reason: got %v", strip.Reason)
	}
}

// The site that motivated the rule, with its names, so nobody reintroduces
// the chain: a supply meter and a sub-meter reading very nearly the same -
// genuine sub-metering, one level down - and six consumers behind them whose
// readings say nothing about any order among themselves. The tightest fit made
// every one of them the child of its predecessor.
//
// The claim: two device levels earn themselves, Stromzaehler -> Qubino, and
// the six consumers are siblings under the Qubino. No consumer is the parent
// of another, because nothing in the numbers puts one behind another.
//
// THIS TEST FAILS AGAINST THE CURRENT RULE AND IS LEFT FAILING ON PURPOSE.
// Do not weaken it and do not delete it. The roomiest rule gets the first two
// levels right and then chains part of the tail anyway:
//
//	Leiste PV     900 -> Qubino        (Qubino keeps 767)
//	Plug TV       355 -> Leiste PV     (900 left beats the Qubino's 767)
//	Plug MGW      150 -> Qubino
//	Plug Drucker  120 -> Qubino
//	Leiste Sofa    90 -> Leiste PV     (545 left beats the Qubino's 497)
//	Plug Trockner  60 -> Qubino
//
// which is four device levels, not two. The mechanism is the mirror image of
// the one roomier's doc comment describes: a device that has just been placed
// carries its full reading as remaining capacity, while its parent's was
// reduced by exactly that reading, so any child taking more than half of what
// its parent had left comes out roomier than that parent and captures the next
// device. Five levels where the tightest fit takes eight is an improvement,
// but "depth appears only where capacity forces it" does not hold: the level
// Leiste PV adds here is not forced - the Qubino had room for all six
// consumers. Deciding what to do about that is not this test's job;
// recording it is.
func TestBuildDoesNotChainASiteOfSmallConsumers(t *testing.T) {
	devices := []model.Device{
		device("stromzaehler", "Stromzaehler", model.Electricity),
		device("qubino", "Qubino", model.Electricity),
		device("leiste-pv", "Leiste PV", model.Electricity),
		device("plug-tv", "Plug TV", model.Electricity),
		device("plug-mgw", "Plug MGW", model.Electricity),
		device("plug-drucker", "Plug Drucker", model.Electricity),
		device("leiste-sofa", "Leiste Sofa", model.Electricity),
		device("plug-trockner", "Plug Trockner", model.Electricity),
	}
	// The small consumers sum to 1520, comfortably inside the Qubino's 1667.
	// That matters: if they summed to more than it measured, the flat tree
	// this test asks for would be arithmetically impossible and the heuristic
	// would be right to nest one of them. The first version of this test had
	// exactly that fault - 1675 against 1667 - and the last device had to go
	// somewhere.
	graph := mustBuild(t, devices, readingsOf(
		reading{"stromzaehler", model.Electricity, 1669},
		reading{"qubino", model.Electricity, 1667},
		reading{"leiste-pv", model.Electricity, 800},
		reading{"plug-tv", model.Electricity, 300},
		reading{"plug-mgw", model.Electricity, 150},
		reading{"plug-drucker", model.Electricity, 120},
		reading{"leiste-sofa", model.Electricity, 90},
		reading{"plug-trockner", model.Electricity, 60},
	), options(0.05))

	got := parents(graph)
	if got["stromzaehler"] != model.RootNodeId {
		t.Errorf("the largest meter belongs to the root, got %v", got["stromzaehler"])
	}
	if got["qubino"] != "stromzaehler" {
		t.Errorf("the Qubino is the only meter with room for it, expected it under the Stromzaehler, got %v", got["qubino"])
	}
	for _, d := range devices[2:] {
		if got[d.Id] != "qubino" {
			t.Errorf("%v (%v) sits under %v, expected the Qubino", d.Id, d.Name, got[d.Id])
		}
	}

	// Stated a second way, because it is the shape and not the single edge
	// that was wrong: Stromzaehler -> Qubino is the only edge between two
	// devices that this data justifies.
	for _, d := range devices {
		if d.Id == "stromzaehler" || d.Id == "qubino" {
			continue
		}
		if kids := children(graph, d.Id); len(kids) != 0 {
			t.Errorf("%v (%v) must not be the parent of another device, got %v", d.Id, d.Name, kids)
		}
	}
	if kids := children(graph, "stromzaehler"); !reflect.DeepEqual(kids, []string{"qubino"}) {
		t.Errorf("the Stromzaehler should have the Qubino as its only child, got %v", kids)
	}

	// root -> Stromzaehler -> Qubino -> consumer, counting the root as the
	// first level.
	deepest, deepestId := 0, ""
	for _, d := range devices {
		if level := levels(graph, d.Id); level > deepest {
			deepest, deepestId = level, d.Id
		}
	}
	if deepest != 4 {
		t.Errorf("the deepest device is %v at level %v, expected 4 counting the root; tree: %#v", deepestId, deepest, got)
	}
}

func TestBuildGasIsNeverChildOfElectricity(t *testing.T) {
	devices := []model.Device{
		device("elec", "same name", model.Electricity),
		device("gas", "same name", model.Gas),
	}
	graph := mustBuild(t, devices, readingsOf(
		reading{"elec", model.Electricity, 1000},
		reading{"gas", model.Gas, 500},
	), options(0.05))

	got := parents(graph)
	if got["gas"] != model.RootNodeId {
		t.Errorf("gas must not hang under an electricity meter, got %v", got["gas"])
	}
	if got["elec"] != model.RootNodeId {
		t.Errorf("elec: got %v", got["elec"])
	}
}

func TestBuildTwoCarriersDoNotInterfere(t *testing.T) {
	devices := []model.Device{
		device("e1", "e one", model.Electricity),
		device("e2", "e two", model.Electricity),
		device("g1", "g one", model.Gas),
		device("g2", "g two", model.Gas),
	}
	graph := mustBuild(t, devices, readingsOf(
		reading{"e1", model.Electricity, 1000},
		reading{"e2", model.Electricity, 600},
		reading{"g1", model.Gas, 800},
		reading{"g2", model.Gas, 300},
	), options(0.05))

	got := parents(graph)
	want := map[string]string{
		"e1": model.RootNodeId,
		"e2": "e1",
		"g1": model.RootNodeId,
		"g2": "g1",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("carriers interfered: %#v", got)
	}
}

func TestBuildDeviceWithoutReadingLandsAtRootAndIsNoParent(t *testing.T) {
	devices := []model.Device{
		device("silent", "silent meter", model.Electricity),
		device("a", "meter a", model.Electricity),
		device("b", "meter b", model.Electricity),
	}
	// The silent meter has a column but never reported.
	graph := mustBuild(t, devices, readingsOf(
		reading{"a", model.Electricity, 100},
		reading{"b", model.Electricity, 50},
	), options(0.05))

	got := parents(graph)
	if got["silent"] != model.RootNodeId {
		t.Errorf("a device with no reading belongs to the root, got %v", got["silent"])
	}
	if kids := children(graph, "silent"); len(kids) != 0 {
		t.Errorf("a device with no reading must never be a parent, got %v", kids)
	}
	if got["b"] != "a" {
		t.Errorf("b: got %v", got["b"])
	}
}

func TestBuildDeviceWithoutCarrierColumnsLandsAtRoot(t *testing.T) {
	devices := []model.Device{
		device("thermostat", "thermostat"),
		device("a", "meter a", model.Electricity),
	}
	graph := mustBuild(t, devices, readingsOf(
		reading{"a", model.Electricity, 100},
		// A reading that exists although the device reads no carrier at all
		// must not pull it into a placement pass.
		reading{"thermostat", model.Electricity, 10},
	), options(0.05))

	if got := parents(graph)["thermostat"]; got != model.RootNodeId {
		t.Errorf("a device without carrier columns belongs to the root, got %v", got)
	}
}

func TestBuildChpIsPlacedExactlyOnce(t *testing.T) {
	devices := []model.Device{
		device("chp", "chp", model.Electricity, model.Gas),
		device("gas-supply", "gas supply", model.Gas),
		device("elec-supply", "elec supply", model.Electricity),
	}
	graph := mustBuild(t, devices, readingsOf(
		reading{"gas-supply", model.Gas, 2000},
		reading{"chp", model.Gas, 900},
		reading{"chp", model.Electricity, 400},
		reading{"elec-supply", model.Electricity, 1500},
	), options(0.05))

	nodes := 0
	for _, node := range graph.Nodes {
		if node.ResourceId == "chp" {
			nodes++
		}
	}
	if nodes != 1 {
		t.Errorf("the chp must appear exactly once, got %v nodes", nodes)
	}
	edges := 0
	for _, edge := range graph.Edges {
		if edge.FromNodeId == "chp" {
			edges++
		}
	}
	if edges != 1 {
		t.Errorf("the chp must have exactly one parent, got %v edges", edges)
	}
	// Gas is its larger reading, so it takes part in the gas pass only.
	if got := parents(graph)["chp"]; got != "gas-supply" {
		t.Errorf("chp should be placed by its gas reading, got %v", got)
	}
}

func TestBuildIsIndependentOfInputOrder(t *testing.T) {
	devices := []model.Device{
		device("supply", "plant supply", model.Electricity),
		device("distributor", "plant distributor", model.Electricity),
		device("strip", "plant strip", model.Electricity),
		device("gas", "gas supply", model.Gas),
		device("boiler", "gas boiler", model.Gas),
		device("silent", "silent meter", model.Electricity),
		device("thermostat", "thermostat"),
	}
	readings := readingsOf(
		reading{"supply", model.Electricity, 1000},
		reading{"distributor", model.Electricity, 600},
		reading{"strip", model.Electricity, 500},
		reading{"gas", model.Gas, 800},
		reading{"boiler", model.Gas, 780},
	)

	want := mustBuild(t, devices, readings, options(0.05))
	for shift := 1; shift < len(devices); shift++ {
		shuffled := append(append([]model.Device{}, devices[shift:]...), devices[:shift]...)
		got := mustBuild(t, shuffled, readings, options(0.05))
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("order changed the graph (shift %v):\ngot  %#v\nwant %#v", shift, got, want)
		}
	}
	// The reverse order too, which no rotation covers.
	reversed := []model.Device{}
	for i := len(devices) - 1; i >= 0; i-- {
		reversed = append(reversed, devices[i])
	}
	if got := mustBuild(t, reversed, readings, options(0.05)); !reflect.DeepEqual(got, want) {
		t.Fatalf("reverse order changed the graph:\ngot  %#v\nwant %#v", got, want)
	}
}

// What the tolerance decides is whether a candidate fits - the 410 exceeds the
// 400 the supply meter has left by 2.5 % and is still admitted there, the 460
// exceeds it by 15 % and is not. Which of the fitting candidates then wins is
// the separate question the roomiest rule answers, and the distributor still
// holds its full 600 at that moment, so it wins every row below whether or not
// the supply meter was admitted alongside it.
//
// That makes the fit test itself the thing to assert on, and it is reported per
// candidate. The boundary sits at 400/0.95, a little above 421.
func TestBuildTolerance(t *testing.T) {
	tests := []struct {
		name           string
		value          float64
		tolerance      float64
		wantSupplyFits bool
	}{
		{name: "within tolerance", value: 410, tolerance: 0.05, wantSupplyFits: true},
		{name: "beyond tolerance", value: 460, tolerance: 0.05, wantSupplyFits: false},
		{name: "no tolerance at all", value: 410, tolerance: 0, wantSupplyFits: false},
		{name: "just inside the boundary", value: 421, tolerance: 0.05, wantSupplyFits: true},
		{name: "just outside the boundary", value: 422, tolerance: 0.05, wantSupplyFits: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			devices := []model.Device{
				device("supply", "plant supply", model.Electricity),
				device("distributor", "plant distributor", model.Electricity),
				device("strip", "plant strip", model.Electricity),
			}
			graph, placements := mustBuildPlaced(t, devices, readingsOf(
				reading{"supply", model.Electricity, 1000},
				reading{"distributor", model.Electricity, 600},
				reading{"strip", model.Electricity, test.value},
			), options(test.tolerance))

			strip := placementOf(t, placements, "strip")
			supply := candidateOf(t, strip, "supply")
			if supply.Remaining != 400 {
				t.Fatalf("precondition: the supply meter should have 400 left, got %#v", supply)
			}
			if supply.Fits != test.wantSupplyFits {
				t.Errorf("supply fits %v, want %v (%v against 400 left at tolerance %v)", supply.Fits, test.wantSupplyFits, test.value, test.tolerance)
			}
			// Under the depth rule the tolerance decides the parent, not just
			// eligibility: the supply meter is the shallower candidate, so
			// admitting it moves the device a level up. That is the whole
			// visible effect of the setting in Build.
			wantParent := "distributor"
			if test.wantSupplyFits {
				wantParent = "supply"
			}
			if got := parents(graph)["strip"]; got != wantParent {
				t.Errorf("strip: got %v, want %v - the tolerance decides whether the shallower candidate is eligible", got, wantParent)
			}
		})
	}
}

// A tie is a tie in the *largest* remaining capacity, and two siblings of
// equal reading are what produces one: the 1000 main meter keeps 200 once both
// 400s hang under it, and each 400 still has its full reading at the same
// depth. The 300 fits into both siblings and into neither the main, so only
// the names say which sibling it belongs to. The id tie-break would answer
// "a", so this also shows that the prefix is consulted first.
//
// Two candidates tied in a small remainder - the shape this test used to
// build - is no longer a reachable tie: the tie has to be at the maximum to be
// consulted at all, and a smaller remainder never gets that far.
func TestBuildTieBreaks(t *testing.T) {
	tests := []struct {
		name       string
		newName    string
		wantParent string
	}{
		{name: "longest common name prefix wins", newName: "hall east strip", wantParent: "b"},
		{name: "without a shared prefix the id decides", newName: "zzz", wantParent: "a"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			devices := []model.Device{
				device("main", "plant main", model.Electricity),
				device("a", "office west main", model.Electricity),
				device("b", "hall east main", model.Electricity),
				device("d", test.newName, model.Electricity),
			}
			graph, placements := mustBuildPlaced(t, devices, readingsOf(
				reading{"main", model.Electricity, 1000},
				reading{"a", model.Electricity, 400},
				reading{"b", model.Electricity, 400},
				reading{"d", model.Electricity, 300},
			), options(0.05))

			got := parents(graph)
			if got["a"] != "main" || got["b"] != "main" {
				t.Fatalf("precondition: the two 400s should be siblings under the main meter, a is under %v and b under %v", got["a"], got["b"])
			}
			placement := placementOf(t, placements, "d")
			for _, id := range []string{"a", "b"} {
				candidate := candidateOf(t, placement, id)
				if candidate.Remaining != 400 || !candidate.Fits {
					t.Fatalf("precondition: %v should be a fitting candidate with 400 left, got %#v", id, candidate)
				}
			}
			if candidate := candidateOf(t, placement, "main"); candidate.Fits {
				t.Fatalf("precondition: the main meter keeps %v and must not hold the 300", candidate.Remaining)
			}
			if got["d"] != test.wantParent {
				t.Errorf("d: got %v, want %v", got["d"], test.wantParent)
			}
		})
	}
}

func TestBuildRejectsMissingOwner(t *testing.T) {
	graph, _, err := Build("", group, []model.Device{device("a", "a", model.Electricity)}, nil, options(0.05))
	if err == nil {
		t.Fatalf("expected an error for a graph without owner, got %#v", graph)
	}
}

func TestAttachKeepsUserEditsAndMarksItsOwnEdge(t *testing.T) {
	devices := []model.Device{
		device("a", "meter a", model.Electricity),
		device("b", "meter b", model.Electricity),
	}
	readings := readingsOf(
		reading{"a", model.Electricity, 1000},
		reading{"b", model.Electricity, 600},
		reading{"c", model.Electricity, 500},
	)
	graph := mustBuild(t, devices, readings, options(0.05))

	// The user has moved b out from under a, up to the root.
	for i, edge := range graph.Edges {
		if edge.FromNodeId == "b" {
			graph.Edges[i].ToNodeId = model.RootNodeId
		}
	}
	mustValid(t, graph)
	before := clone(graph)

	if _, err := Attach(&graph, device("c", "meter c", model.Electricity), readings, options(0.05)); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	mustValid(t, graph)

	got := parents(graph)
	if got["b"] != model.RootNodeId {
		t.Errorf("the user's edit was undone: b sits under %v", got["b"])
	}
	if got["a"] != model.RootNodeId {
		t.Errorf("a: got %v", got["a"])
	}
	// a has its whole 1000 unexplained again after the move, b has 600, and
	// the 500 fits into both - the roomier one takes it. Which parent that is
	// is not the claim of this test; that the user's edit survives it and that
	// the new edge is marked is.
	if got["c"] != "a" {
		t.Errorf("c: got %v, want a", got["c"])
	}

	newEdge, ok := edgeById(graph, "edge-c-a")
	if !ok {
		t.Fatalf("expected the new edge to be called edge-c-a: %#v", graph.Edges)
	}
	if !hasAttribute(newEdge.Attributes, models.GraphEdgeAttrSystemChanged, "true") {
		t.Errorf("the new edge must be marked system_changed: %#v", newEdge)
	}
	if newEdge.Weight != model.FullWeight {
		t.Errorf("new edge weight: %v", newEdge.Weight)
	}

	for _, old := range before.Edges {
		current, ok := edgeById(graph, old.Id)
		if !ok {
			t.Fatalf("edge %v disappeared", old.Id)
		}
		if !reflect.DeepEqual(current, old) {
			t.Errorf("existing edge was touched:\ngot  %#v\nwant %#v", current, old)
		}
	}
	if len(graph.Edges) != len(before.Edges)+1 {
		t.Errorf("expected exactly one new edge, got %v", len(graph.Edges)-len(before.Edges))
	}
}

func TestAttachFallsBackToRoot(t *testing.T) {
	graph := mustBuild(t, []model.Device{device("a", "meter a", model.Electricity)}, readingsOf(
		reading{"a", model.Electricity, 1000},
	), options(0.05))

	tests := []struct {
		name     string
		device   model.Device
		readings map[model.ReadingKey]model.Reading
	}{
		{
			name:   "no reading of its own",
			device: device("b", "meter b", model.Electricity),
			readings: readingsOf(
				reading{"a", model.Electricity, 1000},
			),
		},
		{
			name:   "no carrier columns",
			device: device("b", "thermostat"),
			readings: readingsOf(
				reading{"a", model.Electricity, 1000},
				reading{"b", model.Electricity, 10},
			),
		},
		{
			name:   "other carrier than everything placed",
			device: device("b", "gas meter", model.Gas),
			readings: readingsOf(
				reading{"a", model.Electricity, 1000},
				reading{"b", model.Gas, 10},
			),
		},
		{
			name:   "larger than every candidate",
			device: device("b", "meter b", model.Electricity),
			readings: readingsOf(
				reading{"a", model.Electricity, 1000},
				reading{"b", model.Electricity, 5000},
			),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			working := clone(graph)
			if _, err := Attach(&working, test.device, test.readings, options(0.05)); err != nil {
				t.Fatalf("Attach: %v", err)
			}
			mustValid(t, working)
			if got := parents(working)["b"]; got != model.RootNodeId {
				t.Errorf("expected the root, got %v", got)
			}
		})
	}
}

func TestAttachCountsExistingChildrenAgainstCapacity(t *testing.T) {
	devices := []model.Device{
		device("supply", "plant supply", model.Electricity),
		device("distributor", "plant distributor", model.Electricity),
	}
	readings := readingsOf(
		reading{"supply", model.Electricity, 1000},
		reading{"distributor", model.Electricity, 600},
		reading{"strip", model.Electricity, 500},
	)
	graph := mustBuild(t, devices, readings, options(0.05))
	if got := parents(graph)["distributor"]; got != "supply" {
		t.Fatalf("precondition: distributor under %v", got)
	}

	// The supply meter has 400 left with the distributor already under it,
	// which no longer holds the 500.
	if _, err := Attach(&graph, device("strip", "plant strip", model.Electricity), readings, options(0.05)); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	mustValid(t, graph)
	if got := parents(graph)["strip"]; got != "distributor" {
		t.Errorf("strip: got %v, want distributor", got)
	}
}

func TestAttachIsANoOpForAKnownDevice(t *testing.T) {
	readings := readingsOf(reading{"a", model.Electricity, 1000})
	graph := mustBuild(t, []model.Device{device("a", "meter a", model.Electricity)}, readings, options(0.05))
	before := clone(graph)

	if _, err := Attach(&graph, device("a", "meter a renamed", model.Electricity), readings, options(0.05)); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if !reflect.DeepEqual(graph, before) {
		t.Errorf("a device already in the graph must change nothing:\ngot  %#v\nwant %#v", graph, before)
	}
}

// The frontend's edge ids are derived from the node pair, so the only way
// one can be taken already is a foreign edge in a graph a user has edited.
func TestAttachAvoidsAnEdgeIdCollision(t *testing.T) {
	readings := readingsOf(reading{"a", model.Electricity, 1000})
	graph := mustBuild(t, []model.Device{device("a", "meter a", model.Electricity)}, readings, options(0.05))
	graph.Edges[0].Id = "edge-b-root"
	mustValid(t, graph)

	if _, err := Attach(&graph, device("b", "meter b"), readings, options(0.05)); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	mustValid(t, graph)
	if _, ok := edgeById(graph, "generated-1"); !ok {
		t.Errorf("expected the injected id provider to be used: %#v", graph.Edges)
	}
}

func TestAttachRejectsAGraphWithoutRoot(t *testing.T) {
	graph := models.Graph{Owner: owner}
	_, err := Attach(&graph, device("a", "meter a"), nil, options(0.05))
	if err == nil {
		t.Errorf("expected an error for a graph with no nodes at all")
	}
	if !errors.Is(err, ErrInvalidInput) {
		t.Errorf("a graph with no nodes is a broken input, got %v", err)
	}
	if len(graph.Nodes) != 0 {
		t.Errorf("a failed attach must not change the graph: %#v", graph)
	}
}

func TestDetachReroutesChildrenUpwards(t *testing.T) {
	devices := []model.Device{
		device("supply", "plant supply", model.Electricity),
		device("distributor", "plant distributor", model.Electricity),
		device("strip", "plant strip", model.Electricity),
	}
	graph := mustBuild(t, devices, readingsOf(
		reading{"supply", model.Electricity, 1000},
		reading{"distributor", model.Electricity, 600},
		reading{"strip", model.Electricity, 500},
	), options(0.05))

	if removed, err := Detach(&graph, "distributor"); !removed || err != nil {
		t.Fatalf("Detach: removed=%v err=%v", removed, err)
	}
	mustValid(t, graph)

	for _, node := range graph.Nodes {
		if node.Id == "distributor" || node.ResourceId == "distributor" {
			t.Errorf("the node is still there: %#v", node)
		}
	}
	got := parents(graph)
	if got["strip"] != "supply" {
		t.Errorf("the child should have moved up to the supply meter, got %v", got["strip"])
	}
	if got["supply"] != model.RootNodeId {
		t.Errorf("supply: got %v", got["supply"])
	}
	rerouted := false
	for _, edge := range graph.Edges {
		if edge.FromNodeId == "strip" && hasAttribute(edge.Attributes, models.GraphEdgeAttrSystemChanged, "true") {
			rerouted = true
		}
	}
	if !rerouted {
		t.Errorf("the rerouted edge should be marked system_changed: %#v", graph.Edges)
	}
}

func TestDetachLeafAndUnknownDevice(t *testing.T) {
	devices := []model.Device{
		device("a", "meter a", model.Electricity),
		device("b", "meter b", model.Electricity),
	}
	readings := readingsOf(
		reading{"a", model.Electricity, 1000},
		reading{"b", model.Electricity, 600},
	)
	graph := mustBuild(t, devices, readings, options(0.05))

	if removed, err := Detach(&graph, "unknown"); removed || err != nil {
		t.Errorf("detaching an unknown device is no change and no error, got removed=%v err=%v", removed, err)
	}
	if removed, err := Detach(&graph, "b"); !removed || err != nil {
		t.Fatalf("Detach b: removed=%v err=%v", removed, err)
	}
	mustValid(t, graph)
	if len(graph.Nodes) != 2 || len(graph.Edges) != 1 {
		t.Errorf("expected root and a with one edge, got %#v / %#v", graph.Nodes, graph.Edges)
	}

	// The last device leaves a graph that is still valid: the root alone.
	if removed, err := Detach(&graph, "a"); !removed || err != nil {
		t.Fatalf("Detach a: removed=%v err=%v", removed, err)
	}
	mustValid(t, graph)
	if len(graph.Nodes) != 1 || len(graph.Edges) != 0 {
		t.Errorf("expected the bare root, got %#v / %#v", graph.Nodes, graph.Edges)
	}
}

func TestDetachDoesNotWriteAnInvalidGraph(t *testing.T) {
	// Deleting the root itself would leave two nodes without an outgoing
	// edge, which Graph.Valid rejects. The graph must come back untouched.
	devices := []model.Device{
		device("a", "meter a", model.Electricity),
		device("b", "meter b", model.Gas),
	}
	graph := mustBuild(t, devices, readingsOf(
		reading{"a", model.Electricity, 1000},
		reading{"b", model.Gas, 600},
	), options(0.05))
	before := clone(graph)

	// The root is addressed by node id, which findDeviceNode also accepts.
	removed, err := Detach(&graph, model.RootNodeId)
	if removed {
		t.Errorf("expected no change")
	}
	if err == nil {
		t.Errorf("a refused removal must be reported, not returned as if the node had not been there")
	}
	if !reflect.DeepEqual(graph, before) {
		t.Errorf("the graph was modified:\ngot  %#v\nwant %#v", graph, before)
	}
	mustValid(t, graph)
}

// Five meters that all read a difference of zero over the window: a gas meter
// in summer, a water meter in an empty building, a meter whose counter stands
// at the same value at both ends. Containment has nothing to say about them -
// zero contains nothing - and a hierarchy under the alphabetically first of
// them would be invented out of nothing.
func TestBuildZeroConsumptionIsNoHierarchy(t *testing.T) {
	devices := []model.Device{}
	entries := []reading{}
	for _, id := range []string{"m1", "m2", "m3", "m4", "m5"} {
		devices = append(devices, device(id, "meter "+id, model.Gas))
		entries = append(entries, reading{id, model.Gas, 0})
	}
	graph := mustBuild(t, devices, readingsOf(entries...), options(0.05))

	got := parents(graph)
	for _, d := range devices {
		if got[d.Id] != model.RootNodeId {
			t.Errorf("%v reads zero and belongs to the root, got %v", d.Id, got[d.Id])
		}
		if kids := children(graph, d.Id); len(kids) != 0 {
			t.Errorf("%v reads zero and must never be a parent, got %v", d.Id, kids)
		}
	}
}

// A meter exchange or a counter rollover gives a negative difference. It is no
// better grounded than a zero and follows the same rule.
func TestBuildNegativeConsumptionGoesToRoot(t *testing.T) {
	devices := []model.Device{
		device("supply", "plant supply", model.Electricity),
		device("swapped", "plant swapped", model.Electricity),
	}
	graph := mustBuild(t, devices, readingsOf(
		reading{"supply", model.Electricity, 1000},
		reading{"swapped", model.Electricity, -50},
	), options(0.05))

	got := parents(graph)
	if got["swapped"] != model.RootNodeId {
		t.Errorf("a negative reading belongs to the root, got %v", got["swapped"])
	}
	if kids := children(graph, "swapped"); len(kids) != 0 {
		t.Errorf("a negative reading must never be a parent, got %v", kids)
	}
}

func TestAttachIgnoresANonPositiveReading(t *testing.T) {
	base := mustBuild(t, []model.Device{device("a", "meter a", model.Electricity)}, readingsOf(
		reading{"a", model.Electricity, 1000},
	), options(0.05))

	tests := []struct {
		name  string
		value float64
	}{
		{name: "zero", value: 0},
		{name: "negative", value: -50},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			graph := clone(base)
			readings := readingsOf(
				reading{"a", model.Electricity, 1000},
				reading{"b", model.Electricity, test.value},
			)
			if _, err := Attach(&graph, device("b", "meter b", model.Electricity), readings, options(0.05)); err != nil {
				t.Fatalf("Attach: %v", err)
			}
			mustValid(t, graph)
			if got := parents(graph)["b"]; got != model.RootNodeId {
				t.Errorf("expected the root, got %v", got)
			}
		})
	}
}

// Attach is handed one device and has no way to know what the others are
// called, so it decides a tie in the largest remaining capacity by node id.
// Build, which sees every name, decides the same tie by the longest common
// name prefix. Both answers are pinned here: they differ, and that is
// documented behaviour rather than a defect to be "fixed" into agreement.
//
// The tie is the one from TestBuildTieBreaks - two siblings of equal reading
// under a main meter that itself has too little left - because that is the
// shape a tie at the maximum takes.
func TestAttachBreaksCapacityTiesByNodeId(t *testing.T) {
	devices := []model.Device{
		device("main", "plant main", model.Electricity),
		device("a", "office west main", model.Electricity),
		device("b", "hall east main", model.Electricity),
	}
	arriving := device("d", "hall east strip", model.Electricity)
	readings := readingsOf(
		reading{"main", model.Electricity, 1000},
		reading{"a", model.Electricity, 400},
		reading{"b", model.Electricity, 400},
		reading{"d", model.Electricity, 300},
	)

	// The main meter keeps 200 of its 1000 with both 400s under it, which no
	// longer holds the 300; a and b have their full 400 each.
	base := mustBuild(t, devices, readings, options(0.05))
	if got := parents(base); got["a"] != "main" || got["b"] != "main" {
		t.Fatalf("precondition: a under %v, b under %v", got["a"], got["b"])
	}

	t.Run("attach takes the lower node id", func(t *testing.T) {
		graph := clone(base)
		if _, err := Attach(&graph, arriving, readings, options(0.05)); err != nil {
			t.Fatalf("Attach: %v", err)
		}
		mustValid(t, graph)
		if got := parents(graph)["d"]; got != "a" {
			t.Errorf("d: got %v, want a", got)
		}
	})

	t.Run("a name on a node does not decide the tie", func(t *testing.T) {
		// Only a user's graph carries node names; this service writes none,
		// because a device's name lives on the device list and a copy here
		// would go stale on the next rename.
		graph := clone(base)
		for i, node := range graph.Nodes {
			for _, d := range devices {
				if node.Id == d.Id {
					graph.Nodes[i].Attributes = []models.Attribute{{Key: model.NodeAttrName, Value: d.Name}}
				}
			}
		}
		if _, err := Attach(&graph, arriving, readings, options(0.05)); err != nil {
			t.Fatalf("Attach: %v", err)
		}
		mustValid(t, graph)
		if got := parents(graph)["d"]; got != "a" {
			t.Errorf("d: got %v, want a - a node name must not enter the tie-break", got)
		}
	})

	// About Build, not about Attach: the same numbers and the same tie, decided
	// by the name prefix because Build has the names.
	t.Run("build has the names and uses them", func(t *testing.T) {
		full := mustBuild(t, append(append([]model.Device{}, devices...), arriving), readings, options(0.05))
		if got := parents(full)["d"]; got != "b" {
			t.Errorf("d: got %v, want b - the prefix of \"hall east strip\" is what separates the tie here", got)
		}
	})
}

// The first structure is guessed in its entirety, so the marker sits on the
// graph rather than on any one edge.
func TestBuildMarksTheGraphSystemChanged(t *testing.T) {
	graph := mustBuild(t, []model.Device{
		device("a", "meter a", model.Electricity),
		device("b", "meter b", model.Electricity),
	}, readingsOf(
		reading{"a", model.Electricity, 1000},
		reading{"b", model.Electricity, 600},
	), options(0.05))

	if !hasAttribute(graph.Attributes, models.GraphEdgeAttrSystemChanged, "true") {
		t.Errorf("a wholly guessed graph must be marked system_changed: %#v", graph.Attributes)
	}
	for _, edge := range graph.Edges {
		if hasAttribute(edge.Attributes, models.GraphEdgeAttrSystemChanged, "true") {
			t.Errorf("the graph-level marker covers every edge, %v should not carry its own", edge.Id)
		}
	}
}

// splitParentsGraph is a graph as a user can leave one behind: d hangs under
// two parents with the given shares, and the service's own edge from c to d
// still carries the full weight. Nothing this package writes ever splits a
// node - only a user does - so the cases below cannot be built through Build,
// and they are the ones where Detach actually delegates to the model.
func splitParentsGraph(t *testing.T, toP1 int, toP2 int) models.Graph {
	t.Helper()
	graph := models.Graph{
		Owner: owner,
		Nodes: []models.Node{
			{Id: model.RootNodeId, ResourceType: model.ResourceTypeCustom},
			{Id: "p1", ResourceId: "p1", ResourceType: models.GraphResourceTypeDevice},
			{Id: "p2", ResourceId: "p2", ResourceType: models.GraphResourceTypeDevice},
			{Id: "d", ResourceId: "d", ResourceType: models.GraphResourceTypeDevice},
			{Id: "c", ResourceId: "c", ResourceType: models.GraphResourceTypeDevice},
		},
		Edges: []models.Edge{
			{Id: "edge-p1-root", FromNodeId: "p1", ToNodeId: model.RootNodeId, Weight: 100, Attributes: []models.Attribute{}},
			{Id: "edge-p2-root", FromNodeId: "p2", ToNodeId: model.RootNodeId, Weight: 100, Attributes: []models.Attribute{}},
			{Id: "edge-d-p1", FromNodeId: "d", ToNodeId: "p1", Weight: toP1, Attributes: []models.Attribute{}},
			{Id: "edge-d-p2", FromNodeId: "d", ToNodeId: "p2", Weight: toP2, Attributes: []models.Attribute{}},
			{Id: "edge-c-d", FromNodeId: "c", ToNodeId: "d", Weight: model.FullWeight, Attributes: []models.Attribute{}},
		},
	}
	mustValid(t, graph)
	return graph
}

// The rerouting the model does when a node with more than one parent goes
// away: the child inherits its shares.
func TestDetachReroutesAcrossTwoParents(t *testing.T) {
	graph := splitParentsGraph(t, 25, 75)

	removed, err := Detach(&graph, "d")
	if !removed || err != nil {
		t.Fatalf("Detach: removed=%v err=%v", removed, err)
	}
	mustValid(t, graph)

	if findDeviceNode(&graph, "d") != "" {
		t.Errorf("the node is still there: %#v", graph.Nodes)
	}
	weights := map[string]int{}
	for _, edge := range graph.Edges {
		if edge.FromNodeId != "c" {
			continue
		}
		weights[edge.ToNodeId] = edge.Weight
		if !hasAttribute(edge.Attributes, models.GraphEdgeAttrSystemChanged, "true") {
			t.Errorf("a rerouted edge must be marked system_changed: %#v", edge)
		}
	}
	if !reflect.DeepEqual(weights, map[string]int{"p1": 25, "p2": 75}) {
		t.Errorf("c should have inherited both shares, got %#v", weights)
	}
}

// Six of the 99 two-way splits a user can enter make the model index an
// edge's attributes with the index of the edge. Without the guard in Detach
// this does not fail, it takes the process down - see guarded.
func TestDetachSurvivesAPanicInTheModel(t *testing.T) {
	for _, split := range [][2]int{{29, 71}, {71, 29}, {42, 58}, {58, 42}, {43, 57}, {57, 43}} {
		t.Run(fmt.Sprintf("%v-%v", split[0], split[1]), func(t *testing.T) {
			graph := splitParentsGraph(t, split[0], split[1])
			before := clone(graph)

			removed, err := Detach(&graph, "d")
			if removed {
				t.Errorf("nothing was removed, so nothing may be reported as removed")
			}
			if err == nil {
				t.Fatalf("the panic must reach the caller as an error")
			}
			if !reflect.DeepEqual(graph, before) {
				t.Errorf("a failed detach must leave the graph alone:\ngot  %#v\nwant %#v", graph, before)
			}
			mustValid(t, graph)
		})
	}
}

// The three outcomes have to be told apart: a device that was not there is
// nothing to report, a removal that was refused is, or the device sits in the
// graph forever and every pass retries it in silence.
func TestDetachDistinguishesItsOutcomes(t *testing.T) {
	devices := []model.Device{
		device("a", "meter a", model.Electricity),
		device("b", "meter b", model.Electricity),
	}
	readings := readingsOf(
		reading{"a", model.Electricity, 1000},
		reading{"b", model.Electricity, 600},
	)

	t.Run("no node for the device", func(t *testing.T) {
		graph := mustBuild(t, devices, readings, options(0.05))
		removed, err := Detach(&graph, "unknown")
		if removed || err != nil {
			t.Errorf("got removed=%v err=%v, want false and no error", removed, err)
		}
	})

	t.Run("removed", func(t *testing.T) {
		graph := mustBuild(t, devices, readings, options(0.05))
		removed, err := Detach(&graph, "b")
		if !removed || err != nil {
			t.Errorf("got removed=%v err=%v, want true and no error", removed, err)
		}
	})

	t.Run("refused", func(t *testing.T) {
		// c takes 1 % of its flow through d and 99 % through e, and d itself
		// is split evenly between two parents. Rerouting c's 1 % over that
		// split rounds both new edges to zero, which the model may not write
		// and Graph.Valid rejects.
		graph := models.Graph{
			Owner: owner,
			Nodes: []models.Node{
				{Id: model.RootNodeId, ResourceType: model.ResourceTypeCustom},
				{Id: "p1", ResourceId: "p1", ResourceType: models.GraphResourceTypeDevice},
				{Id: "p2", ResourceId: "p2", ResourceType: models.GraphResourceTypeDevice},
				{Id: "d", ResourceId: "d", ResourceType: models.GraphResourceTypeDevice},
				{Id: "e", ResourceId: "e", ResourceType: models.GraphResourceTypeDevice},
				{Id: "c", ResourceId: "c", ResourceType: models.GraphResourceTypeDevice},
			},
			Edges: []models.Edge{
				{Id: "edge-p1-root", FromNodeId: "p1", ToNodeId: model.RootNodeId, Weight: 100, Attributes: []models.Attribute{}},
				{Id: "edge-p2-root", FromNodeId: "p2", ToNodeId: model.RootNodeId, Weight: 100, Attributes: []models.Attribute{}},
				{Id: "edge-e-root", FromNodeId: "e", ToNodeId: model.RootNodeId, Weight: 100, Attributes: []models.Attribute{}},
				{Id: "edge-d-p1", FromNodeId: "d", ToNodeId: "p1", Weight: 50, Attributes: []models.Attribute{}},
				{Id: "edge-d-p2", FromNodeId: "d", ToNodeId: "p2", Weight: 50, Attributes: []models.Attribute{}},
				{Id: "edge-c-d", FromNodeId: "c", ToNodeId: "d", Weight: 1, Attributes: []models.Attribute{}},
				{Id: "edge-c-e", FromNodeId: "c", ToNodeId: "e", Weight: 99, Attributes: []models.Attribute{}},
			},
		}
		mustValid(t, graph)
		before := clone(graph)

		removed, err := Detach(&graph, "d")
		if removed {
			t.Errorf("expected no change")
		}
		if err == nil {
			t.Fatalf("a refusal must be reported: d stays in the graph and every pass will refuse it again")
		}
		if !reflect.DeepEqual(graph, before) {
			t.Errorf("a refused detach must leave the graph alone:\ngot  %#v\nwant %#v", graph, before)
		}
	})
}

// A graph a user broke - here by deleting the edge that took a to the root -
// must be reported as what it is, not as damage the attach did.
func TestAttachSeparatesABrokenInputGraph(t *testing.T) {
	devices := []model.Device{
		device("a", "meter a", model.Electricity),
		device("b", "meter b", model.Electricity),
	}
	readings := readingsOf(
		reading{"a", model.Electricity, 1000},
		reading{"b", model.Electricity, 600},
		reading{"c", model.Electricity, 100},
	)
	sound := mustBuild(t, devices, readings, options(0.05))

	t.Run("sound input attaches", func(t *testing.T) {
		graph := clone(sound)
		if _, err := Attach(&graph, device("c", "meter c", model.Electricity), readings, options(0.05)); err != nil {
			t.Fatalf("Attach: %v", err)
		}
		mustValid(t, graph)
	})

	t.Run("broken input is named as such", func(t *testing.T) {
		graph := clone(sound)
		kept := []models.Edge{}
		for _, edge := range graph.Edges {
			if edge.FromNodeId != "a" {
				kept = append(kept, edge)
			}
		}
		graph.Edges = kept
		before := clone(graph)

		_, err := Attach(&graph, device("c", "meter c", model.Electricity), readings, options(0.05))
		if err == nil {
			t.Fatalf("expected an error")
		}
		if !errors.Is(err, ErrInvalidInput) {
			t.Errorf("the error must be distinguishable from one this package caused, got %v", err)
		}
		if !reflect.DeepEqual(graph, before) {
			t.Errorf("a failed attach must leave the graph alone:\ngot  %#v\nwant %#v", graph, before)
		}
	})
}

// A meter its own children already explain is not a candidate, however shallow
// it sits. Measured against a real site: a 156 kWh supply meter with 0.1 left
// after its 155.9 kWh sub-main still collected four devices that belong under
// that sub-main, purely because it was one level higher.
func TestBuildDoesNotHangDevicesOnAnExplainedMeter(t *testing.T) {
	devices := []model.Device{
		device("supply", "Stromzaehler", model.Electricity),
		device("submain", "Qubino", model.Electricity),
		device("small-a", "Leiste Gym", model.Electricity),
		device("small-b", "Plug Drucker", model.Electricity),
	}
	graph, placements := mustBuildPlaced(t, devices, readingsOf(
		reading{"supply", model.Electricity, 156},
		reading{"submain", model.Electricity, 155.9},
		reading{"small-a", model.Electricity, 0.2},
		reading{"small-b", model.Electricity, 0.1},
	), options(0.05))

	parentOf := parents(graph)
	if parentOf["submain"] != "supply" {
		t.Errorf("the sub-main belongs under the supply meter, got %v", parentOf["submain"])
	}
	for _, id := range []string{"small-a", "small-b"} {
		if got := parentOf[id]; got != "submain" {
			t.Errorf("%v: got %v, want the sub-main - the supply meter has 0.1 of 156 left and is explained", id, got)
		}
	}
	// And the report has to say so, or nobody can tell this from a capacity
	// problem.
	smallA := placementOf(t, placements, "small-a")
	if supply := candidateOf(t, smallA, "supply"); supply.Fits {
		t.Errorf("the explained supply meter must not count as fitting, got %#v", supply)
	}
}

// Devices whose type measures no medium at all collect under one node rather
// than burying the meters under ninety siblings. Devices that could have
// reported and did not stay at the root: they belong in the flow, and being
// visible there is the point.
func TestBuildCollectsDevicesWithoutAnyCarrierUnderOneNode(t *testing.T) {
	devices := []model.Device{
		device("meter", "Stromzaehler", model.Electricity),
		// A type that reads a carrier but reported nothing this window.
		device("silent", "Plug Pumpe", model.Electricity),
		// Types that read nothing at all.
		{Id: "contact", Name: "Kontakt Balkontuer"},
		{Id: "motion", Name: "PIR Treppe"},
	}
	opts := options(0.05)
	opts.NoFlowNodeName = "Ohne Energiefluss"

	graph, _, err := Build("owner", group, devices, readingsOf(
		reading{"meter", model.Electricity, 100},
	), opts)
	if err != nil {
		t.Fatal(err)
	}
	mustValid(t, graph)

	parentOf := parents(graph)
	for _, id := range []string{"contact", "motion"} {
		if got := parentOf[id]; got != model.NoFlowNodeId {
			t.Errorf("%v reads no carrier, expected the collector, got %v", id, got)
		}
	}
	if got := parentOf["silent"]; got != model.RootNodeId {
		t.Errorf("a device that could report and did not stays at the root, got %v", got)
	}
	if got := parentOf[model.NoFlowNodeId]; got != model.RootNodeId {
		t.Errorf("the collector hangs off the root, got %v", got)
	}

	var collector models.Node
	for _, node := range graph.Nodes {
		if node.Id == model.NoFlowNodeId {
			collector = node
		}
	}
	if collector.ResourceType != model.ResourceTypeCustom || collector.ResourceId != "" {
		t.Errorf("the collector is a custom node with no resource, got %#v", collector)
	}
	name := ""
	for _, attribute := range collector.Attributes {
		if attribute.Key == model.NodeAttrName {
			name = attribute.Value
		}
	}
	if name != "Ohne Energiefluss" {
		t.Errorf("collector name: got %q", name)
	}
}

// No collector where nothing needs one, and none when it is switched off - the
// root stays the only invented node in both cases.
func TestBuildCreatesNoCollectorWhenItIsNotNeeded(t *testing.T) {
	withCarrier := []model.Device{device("meter", "Stromzaehler", model.Electricity)}
	opts := options(0.05)
	opts.NoFlowNodeName = "Ohne Energiefluss"

	graph, _, err := Build("owner", group, withCarrier, readingsOf(
		reading{"meter", model.Electricity, 100},
	), opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, found := parents(graph)[model.NoFlowNodeId]; found {
		t.Error("no device needed the collector, so it must not exist")
	}

	// Switched off: the devices go to the root instead.
	off := options(0.05)
	off.NoFlowNodeName = ""
	graph, _, err = Build("owner", group, []model.Device{{Id: "contact", Name: "Kontakt"}}, nil, off)
	if err != nil {
		t.Fatal(err)
	}
	mustValid(t, graph)
	if got := parents(graph)["contact"]; got != model.RootNodeId {
		t.Errorf("with the collector off the device belongs at the root, got %v", got)
	}
}
