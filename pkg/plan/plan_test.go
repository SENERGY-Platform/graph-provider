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

package plan

import (
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/SENERGY-Platform/graph-provider/pkg/model"
	platform "github.com/SENERGY-Platform/models/go/models"
)

// entry is one drawn node: how deep it sits and what it says.
//
// Depth is derived from the order of the distinct indentations that occur, not
// from a number of spaces. The report's layout is meant to be edited - a test
// that failed on a changed indent would be one nobody dares touch, and the
// claim being made here is about nesting anyway.
type entry struct {
	depth int
	text  string
}

func draw(t *testing.T, changes []model.PlannedChange) (string, []entry) {
	t.Helper()
	out := &strings.Builder{}
	if err := Render(out, changes); err != nil {
		t.Fatalf("render failed: %v", err)
	}
	report := out.String()

	widths := []int{}
	raw := []struct {
		width int
		text  string
	}{}
	for _, line := range strings.Split(report, "\n") {
		trimmed := strings.TrimLeft(line, " \t")
		if !strings.HasPrefix(trimmed, "- ") {
			continue
		}
		width := len(line) - len(trimmed)
		if !slices.Contains(widths, width) {
			widths = append(widths, width)
		}
		raw = append(raw, struct {
			width int
			text  string
		}{width: width, text: strings.TrimSpace(strings.TrimPrefix(trimmed, "- "))})
	}
	sort.Ints(widths)

	entries := []entry{}
	for _, line := range raw {
		entries = append(entries, entry{depth: slices.Index(widths, line.width), text: line.text})
	}
	return report, entries
}

// find is the drawn node whose text contains needle.
func find(t *testing.T, entries []entry, needle string) entry {
	t.Helper()
	for _, e := range entries {
		if strings.Contains(e.text, needle) {
			return e
		}
	}
	t.Fatalf("expected a node mentioning %q in %v", needle, entries)
	return entry{}
}

// tree builds a graph from child -> parent pairs, with the root last.
func tree(id string, name string, parents map[string]string) platform.Graph {
	graph := platform.Graph{
		Id:    id,
		Owner: "owner",
		Nodes: []platform.Node{{
			Id:           model.RootNodeId,
			ResourceType: model.ResourceTypeCustom,
			Attributes:   []platform.Attribute{{Key: model.NodeAttrName, Value: name}},
		}},
	}
	children := []string{}
	for child := range parents {
		children = append(children, child)
	}
	sort.Strings(children)
	for _, child := range children {
		graph.Nodes = append(graph.Nodes, platform.Node{
			Id:           child,
			ResourceId:   child,
			ResourceType: platform.GraphResourceTypeDevice,
		})
		graph.Edges = append(graph.Edges, platform.Edge{
			Id:         "e-" + child,
			FromNodeId: child,
			ToNodeId:   parents[child],
			Weight:     model.FullWeight,
		})
	}
	return graph
}

func devices(names map[string]string) []model.Device {
	result := []model.Device{}
	ids := []string{}
	for id := range names {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		result = append(result, model.Device{Id: id, Name: names[id]})
	}
	return result
}

// meter is a device that reads one carrier, for the tests that need a figure
// to be found for it rather than only a name.
func meter(id string, name string, carrier model.Carrier) model.Device {
	return model.Device{Id: id, Name: name, Columns: []model.CarrierColumn{{Carrier: carrier}}}
}

// readingOf is one entry of a change's Readings map.
func readingOf(deviceId string, carrier model.Carrier, value float64) (model.ReadingKey, model.Reading) {
	return model.ReadingKey{DeviceId: deviceId, Carrier: carrier}, model.Reading{DeviceId: deviceId, Carrier: carrier, Value: value}
}

// A tree three levels deep has to come out three levels deep, with every node
// under the one the edges point at.
func TestRenderNestsAsTheEdgesDo(t *testing.T) {
	change := model.PlannedChange{
		Action: model.ActionCreate,
		Group:  model.Group{Id: "kc1", Path: "/acme/plant", Name: "plant"},
		Graph: tree("", "plant", map[string]string{
			"d1": model.RootNodeId,
			"d2": "d1",
			"d3": "d2",
		}),
		Devices: devices(map[string]string{
			"d1": "Hauptzaehler",
			"d2": "Halle Ost",
			"d3": "Presse",
		}),
		Note: "3 devices",
	}

	report, entries := draw(t, []model.PlannedChange{change})
	if !strings.Contains(report, "/acme/plant") {
		t.Error("expected the group path in the report")
	}
	if !strings.Contains(report, "plant") {
		t.Error("expected the graph name in the report")
	}
	if !strings.Contains(report, "3 devices") {
		t.Error("expected the note in the report")
	}
	if !strings.Contains(report, "(new)") {
		t.Error("a graph with no id is one that would be created, and the report has to say so")
	}

	// The root, then one device per level below it.
	root := find(t, entries, "plant")
	first := find(t, entries, "Hauptzaehler")
	second := find(t, entries, "Halle Ost")
	third := find(t, entries, "Presse")
	if root.depth != 0 {
		t.Errorf("expected the root at the top, got depth %v", root.depth)
	}
	if first.depth != 1 || second.depth != 2 || third.depth != 3 {
		t.Errorf("expected one level per edge, got %v, %v, %v", first.depth, second.depth, third.depth)
	}

	// Names, with the id kept so a reader can look the device up.
	if !strings.Contains(first.text, "d1") {
		t.Errorf("expected the device id beside its name, got %q", first.text)
	}
}

// A flat graph is the common shape and must not gain a level from nowhere.
func TestRenderDrawsAFlatGraphFlat(t *testing.T) {
	change := model.PlannedChange{
		Action: model.ActionUpdate,
		Group:  model.Group{Path: "/acme", Name: "acme"},
		Graph: tree("g7", "acme", map[string]string{
			"d1": model.RootNodeId,
			"d2": model.RootNodeId,
		}),
		Devices: devices(map[string]string{"d1": "Zaehler A", "d2": "Zaehler B"}),
		Note:    "2 devices added",
	}

	report, entries := draw(t, []model.PlannedChange{change})
	if !strings.Contains(report, "g7") {
		t.Error("expected the id of an existing graph in the report")
	}
	if find(t, entries, "Zaehler A").depth != 1 || find(t, entries, "Zaehler B").depth != 1 {
		t.Errorf("expected both devices directly under the root, got %v", entries)
	}
	if !strings.Contains(report, "graphs to update: 1") {
		t.Errorf("expected an update to be summarised as one, got:\n%v", report)
	}
}

// A device the pass had no reading for is why it sits at the root, and that is
// the one thing in the report a reader can act on.
func TestRenderMarksADeviceWithoutAReading(t *testing.T) {
	key, reading := readingOf("d1", model.Electricity, 1200)
	change := model.PlannedChange{
		Action: model.ActionCreate,
		Group:  model.Group{Path: "/acme", Name: "acme"},
		Graph: tree("", "acme", map[string]string{
			"d1": model.RootNodeId,
			"d2": model.RootNodeId,
		}),
		Devices:   []model.Device{meter("d1", "Hauptzaehler", model.Electricity), meter("d2", "Kompressor", model.Electricity)},
		Readings:  map[model.ReadingKey]model.Reading{key: reading},
		NoReading: []string{"d2"},
	}

	report, entries := draw(t, []model.PlannedChange{change})
	marked := find(t, entries, "Kompressor")
	if !strings.Contains(marked.text, "no reading") {
		t.Errorf("expected the device without a reading to be marked, got %q", marked.text)
	}
	if unmarked := find(t, entries, "Hauptzaehler"); strings.Contains(unmarked.text, "no reading") {
		t.Errorf("a device with a reading must not be marked, got %q", unmarked.text)
	}
	if !strings.Contains(report, "by no reading of their own: 1") {
		t.Errorf("expected the summary to count it, got:\n%v", report)
	}
}

// Nothing to do is an answer, and it has to read like one rather than like an
// empty file somebody will mistake for a crash.
func TestRenderSaysSoWhenThereIsNothingToDo(t *testing.T) {
	report, entries := draw(t, nil)
	if len(entries) != 0 {
		t.Errorf("expected no tree at all, got %v", entries)
	}
	if !strings.Contains(report, "no changes") {
		t.Errorf("expected the report to say there is nothing to do, got:\n%v", report)
	}
	for _, expected := range []string{"graphs to create: 0", "graphs to update: 0"} {
		if !strings.Contains(report, expected) {
			t.Errorf("expected %q in the summary, got:\n%v", expected, report)
		}
	}
}

// A sharing change touches no structure, so it is reported as a line and not
// as a second copy of a tree the reader has already seen.
func TestRenderReportsASharingChangeWithoutATree(t *testing.T) {
	change := model.PlannedChange{
		Action: model.ActionShare,
		Group:  model.Group{Path: "/acme", Name: "acme"},
		Graph:  tree("g1", "acme", map[string]string{"d1": model.RootNodeId}),
		Note:   "group /acme",
	}
	report, entries := draw(t, []model.PlannedChange{change})
	if len(entries) != 0 {
		t.Errorf("expected no tree for a sharing change, got %v", entries)
	}
	if !strings.Contains(report, model.ActionShare) || !strings.Contains(report, "/acme") {
		t.Errorf("expected the sharing change to be named, got:\n%v", report)
	}
}

// A graph a user has edited into a circle must not cost the report its
// termination.
func TestRenderSurvivesACircle(t *testing.T) {
	graph := tree("g9", "acme", map[string]string{"d1": model.RootNodeId, "d2": "d1"})
	graph.Edges = append(graph.Edges, platform.Edge{
		Id:         "e-loop",
		FromNodeId: "d1",
		ToNodeId:   "d2",
		Weight:     model.FullWeight,
	})
	report, _ := draw(t, []model.PlannedChange{{
		Action: model.ActionUpdate,
		Group:  model.Group{Path: "/acme", Name: "acme"},
		Graph:  graph,
	}})
	if report == "" {
		t.Error("expected a report")
	}
}

// Every device node carries the figure it was placed by, and a node with
// children says what is still unexplained - the reader has to be able to
// check the tree against the numbers that produced it, which was exactly the
// thing missing when a nine-level chain came out of a real run and nobody
// could tell whether two levels would have been right.
func TestRenderShowsTheReadingBehindEachNode(t *testing.T) {
	change := model.PlannedChange{
		Action: model.ActionCreate,
		Group:  model.Group{Path: "/acme", Name: "acme"},
		Graph: tree("", "acme", map[string]string{
			"d1": model.RootNodeId, // Stromzaehler
			"d2": "d1",             // Qubino
			"d3": "d2",             // Leiste PV
			"d4": "d2",             // Plug TV
			"d5": model.RootNodeId, // Sensor XY, no reading
		}),
		Devices: []model.Device{
			meter("d1", "Stromzaehler", model.Electricity),
			meter("d2", "Qubino", model.Electricity),
			meter("d3", "Leiste PV", model.Electricity),
			meter("d4", "Plug TV", model.Electricity),
			{Id: "d5", Name: "Sensor XY"}, // no carrier column at all
		},
		Readings: func() map[model.ReadingKey]model.Reading {
			result := map[model.ReadingKey]model.Reading{}
			for _, entry := range []struct {
				id    string
				value float64
			}{{"d1", 1669}, {"d2", 1667}, {"d3", 900}, {"d4", 355}} {
				key, reading := readingOf(entry.id, model.Electricity, entry.value)
				result[key] = reading
			}
			return result
		}(),
	}

	_, entries := draw(t, []model.PlannedChange{change})

	stromzaehler := find(t, entries, "Stromzaehler")
	if !strings.Contains(stromzaehler.text, "1669.0") || !strings.Contains(stromzaehler.text, "electricity") {
		t.Errorf("expected the reading and carrier beside the node, got %q", stromzaehler.text)
	}
	if !strings.Contains(stromzaehler.text, "unexplained 2.0") {
		t.Errorf("expected 1669 minus its child Qubino's 1667 as what is still unexplained, got %q", stromzaehler.text)
	}

	qubino := find(t, entries, "Qubino")
	if !strings.Contains(qubino.text, "1667.0") {
		t.Errorf("expected Qubino's own reading, got %q", qubino.text)
	}
	if !strings.Contains(qubino.text, "unexplained 412.0") {
		t.Errorf("expected 1667 minus its children 900 and 355, got %q", qubino.text)
	}

	// Leaves have no children, so nothing is left for them to leave
	// unexplained - the word must not appear at all.
	for _, name := range []string{"Leiste PV", "Plug TV"} {
		leaf := find(t, entries, name)
		if strings.Contains(leaf.text, "unexplained") {
			t.Errorf("expected a leaf to carry no unexplained amount, got %q", leaf.text)
		}
		if !strings.Contains(leaf.text, "electricity") {
			t.Errorf("expected the leaf's own reading, got %q", leaf.text)
		}
	}

	sensor := find(t, entries, "Sensor XY")
	if !strings.Contains(sensor.text, "no reading") {
		t.Errorf("expected a node without a figure to say so, got %q", sensor.text)
	}
}

// The candidates a device's placement weighed are shown so the edge is
// arguable instead of mysterious - but only for a device that actually had a
// choice: the largest meter of its carrier has nothing above it to weigh, and
// none of the candidates it never got to consider are printed for it either.
func TestRenderListsTheCandidatesAPlacementWeighed(t *testing.T) {
	change := model.PlannedChange{
		Action: model.ActionCreate,
		Group:  model.Group{Path: "/acme", Name: "acme"},
		Graph: tree("", "acme", map[string]string{
			"stromzaehler": model.RootNodeId,
			"qubino":       "stromzaehler",
			"plugtv":       "stromzaehler",
			"plugmgw":      "qubino",
		}),
		Devices: devices(map[string]string{
			"stromzaehler": "Stromzaehler",
			"qubino":       "Qubino",
			"plugtv":       "Plug TV",
			"plugmgw":      "Plug MGW",
		}),
		Placements: []model.Placement{
			{
				// The largest meter of its carrier: nothing was weighed for
				// it, so it must not get a block of its own below.
				DeviceId: "stromzaehler",
				Carrier:  model.Electricity,
				Value:    2200,
				ParentId: model.RootNodeId,
				Reason:   model.PlacedAtRootLargest,
			},
			{
				DeviceId: "plugmgw",
				Carrier:  model.Electricity,
				Value:    150,
				ParentId: "qubino",
				Reason:   model.PlacedByCapacity,
				Candidates: []model.Candidate{
					{DeviceId: "qubino", Remaining: 499, Fits: true, Chosen: true},
					{DeviceId: "plugtv", Remaining: 200, Fits: true, Chosen: false},
					{DeviceId: "stromzaehler", Remaining: 2, Fits: false, Chosen: false},
				},
			},
		},
	}

	report, err := renderReport(t, change)
	if err != nil {
		t.Fatalf("render failed: %v", err)
	}

	header := "how each parent was chosen"
	if !strings.Contains(report, header) {
		t.Fatalf("expected the candidate section header, got:\n%v", report)
	}
	section := report[strings.Index(report, header):]

	if !strings.Contains(section, "Plug MGW") {
		t.Errorf("expected the contested device to head its own block, got:\n%v", section)
	}
	if !strings.Contains(section, "Qubino") || !strings.Contains(section, "remaining 499.0") ||
		!strings.Contains(section, "fits") || !strings.Contains(section, "<- chosen") ||
		!strings.Contains(section, model.PlacedByCapacity) {
		t.Errorf("expected the winning candidate with its reason, got:\n%v", section)
	}
	if !strings.Contains(section, "Plug TV") || !strings.Contains(section, "remaining 200.0") {
		t.Errorf("expected a candidate that fit but did not win, got:\n%v", section)
	}
	if !strings.Contains(section, "too small") {
		t.Errorf("expected the candidate that did not fit to say so, got:\n%v", section)
	}

	// Stromzaehler's own placement (value 2200) had no candidates, so it must
	// not have a block of its own in this section - only its appearance as a
	// losing candidate above, which never mentions its own placed value.
	if strings.Contains(report, "2200.0") {
		t.Errorf("expected the uncontested placement to be left out of the candidate section, got:\n%v", report)
	}
}

// Devices with no usable reading get their own list with the reason each one
// sits at the root, so a reader has something to act on instead of only a
// mark on a branch of the tree.
func TestRenderListsDevicesWithNoReadingAndWhy(t *testing.T) {
	change := model.PlannedChange{
		Action: model.ActionCreate,
		Group:  model.Group{Path: "/acme", Name: "acme"},
		Graph: tree("", "acme", map[string]string{
			"d1": model.RootNodeId,
			"d2": model.RootNodeId,
		}),
		Devices:   devices(map[string]string{"d1": "Plug Pumpe (kaputt)", "d2": "Sensor XY"}),
		NoReading: []string{"d1", "d2"},
		Placements: []model.Placement{
			{DeviceId: "d1", ParentId: model.RootNodeId, Reason: model.PlacedAtRootNoReading},
			{DeviceId: "d2", ParentId: model.RootNodeId, Reason: model.PlacedAtRootNoReading},
		},
	}

	report, err := renderReport(t, change)
	if err != nil {
		t.Fatalf("render failed: %v", err)
	}

	header := "nothing could be concluded about these"
	treeIndex := strings.Index(report, "acme")
	headerIndex := strings.Index(report, header)
	if headerIndex < 0 {
		t.Fatalf("expected the no-reading list header, got:\n%v", report)
	}
	if headerIndex < treeIndex {
		t.Errorf("expected the no-reading list after the tree, got:\n%v", report)
	}
	section := report[headerIndex:]
	for _, want := range []string{"Plug Pumpe (kaputt)", "Sensor XY", model.PlacedAtRootNoReading} {
		if !strings.Contains(section, want) {
			t.Errorf("expected %q in the no-reading list, got:\n%v", want, section)
		}
	}
}

// renderReport is draw without the tree-entry parsing, for a test that only
// needs the raw text.
func renderReport(t *testing.T, change model.PlannedChange) (string, error) {
	t.Helper()
	out := &strings.Builder{}
	err := Render(out, []model.PlannedChange{change})
	return out.String(), err
}
