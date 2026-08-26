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

// Package plan turns the changes a reconciliation pass would make into a
// report a person can read.
//
// It is written for somebody who has never seen this code and wants to know
// what would happen to their graphs before it happens. So: names rather than
// ids wherever a name is known, the tree drawn out rather than left as a list
// of edges, and the one thing a reader can act on - a device the heuristic had
// no reading for - marked where it sits.
//
// The package is pure: it reads a slice of changes and writes text. It knows
// nothing about the pass that produced them, which is what keeps the
// dependency pointing this way and not into reconcile.
package plan

import (
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"

	"github.com/SENERGY-Platform/graph-provider/pkg/graphs"
	"github.com/SENERGY-Platform/graph-provider/pkg/model"
	platform "github.com/SENERGY-Platform/models/go/models"
)

// indent is one level of nesting in a drawn tree.
const indent = "  "

// Render writes a human-readable report of what a pass would do.
func Render(out io.Writer, changes []model.PlannedChange) error {
	report := &strings.Builder{}
	report.WriteString("what a reconciliation pass would write\n")
	report.WriteString("=====================================\n\n")

	if len(changes) == 0 {
		report.WriteString("no changes: every group's graph already matches the world\n\n")
	}
	for i, change := range changes {
		renderChange(report, i+1, change)
	}
	renderSummary(report, changes)

	_, err := io.WriteString(out, report.String())
	return err
}

func renderChange(report *strings.Builder, number int, change model.PlannedChange) {
	fmt.Fprintf(report, "%v. %v  group %v  graph %v\n",
		number, actionOf(change), groupOf(change), graphOf(change))
	if change.Note != "" {
		fmt.Fprintf(report, "%v%v\n", indent, change.Note)
	}

	// Only a change to the graph itself gets its tree drawn. A sharing change
	// leaves the structure exactly as it was, and repeating it there would
	// bury the one line that did change under a tree nobody needs to re-read.
	if change.Action == model.ActionCreate || change.Action == model.ActionUpdate {
		renderTree(report, change)
		renderCandidates(report, change)
		renderNoReading(report, change)
	}
	report.WriteString("\n")
}

// actionOf is the action, defaulting to a readable placeholder rather than an
// empty column: an unnamed action is a bug worth seeing in the report.
func actionOf(change model.PlannedChange) string {
	if change.Action == "" {
		return "(no action)"
	}
	return change.Action
}

func groupOf(change model.PlannedChange) string {
	if change.Group.Path != "" {
		return change.Group.Path
	}
	if change.Group.Name != "" {
		return change.Group.Name
	}
	return "(unknown)"
}

// graphOf names a graph the way a reader can look it up: by its display name,
// which is the name attribute of its root node, plus the id it is stored
// under. A graph about to be created has no id yet, and saying so is the
// difference between a new graph and one being changed.
func graphOf(change model.PlannedChange) string {
	name := graphName(change.Graph)
	if change.Graph.Id == "" {
		return fmt.Sprintf("%q (new)", name)
	}
	return fmt.Sprintf("%q (%v)", name, change.Graph.Id)
}

func graphName(graph platform.Graph) string {
	if root, found := graphs.RootOf(graph); found {
		if name := graphs.AttributeOf(root.Attributes, model.NodeAttrName); name != "" {
			return name
		}
	}
	if name := graphs.GeneratedNameOf(graph); name != "" {
		return name
	}
	return "unnamed"
}

// renderTree draws the graph from the root down.
//
// Edges point child to parent, so they are inverted here once into a
// parent-to-children map; the root is the node nothing points up from, the
// same rule the graph view identifies it by.
func renderTree(report *strings.Builder, change model.PlannedChange) {
	graph := change.Graph
	root, found := graphs.RootOf(graph)
	if !found {
		// Every valid graph has exactly one node without an outgoing edge, so
		// this is a graph the repository would refuse. Worth showing rather
		// than skipping: it is the interesting case in a dry run.
		fmt.Fprintf(report, "%vno root: this graph has no node without an outgoing edge\n", indent)
	}

	children := map[string][]string{}
	for _, edge := range graph.Edges {
		children[edge.ToNodeId] = append(children[edge.ToNodeId], edge.FromNodeId)
	}
	names := labels(change)
	for parent := range children {
		sort.Slice(children[parent], func(i, j int) bool {
			left, right := children[parent][i], children[parent][j]
			if names[left] != names[right] {
				return names[left] < names[right]
			}
			return left < right
		})
	}

	values := nodeReadings(change)
	deviceNodes := map[string]bool{}
	for _, node := range graph.Nodes {
		if node.ResourceType == platform.GraphResourceTypeDevice {
			deviceNodes[node.Id] = true
		}
	}

	// A user's graph can contain a cycle the model let through, and a walk
	// that trusted the edges would not come back. Visited nodes end a branch.
	visited := map[string]bool{}
	if found {
		walk(report, root.Id, 1, children, names, values, deviceNodes, visited)
	}

	unreached := []string{}
	for _, node := range graph.Nodes {
		if !visited[node.Id] {
			unreached = append(unreached, node.Id)
		}
	}
	slices.Sort(unreached)
	for _, id := range unreached {
		fmt.Fprintf(report, "%v- %v  [not reachable from the root]\n",
			strings.Repeat(indent, 1), names[id])
	}
}

func walk(report *strings.Builder, nodeId string, depth int, children map[string][]string, names map[string]string, values map[string]reading, deviceNodes map[string]bool, visited map[string]bool) {
	if visited[nodeId] {
		fmt.Fprintf(report, "%v- %v  [already shown, the edges lead in a circle]\n",
			strings.Repeat(indent, depth), names[nodeId])
		return
	}
	visited[nodeId] = true

	fmt.Fprintf(report, "%v- %v%v\n", strings.Repeat(indent, depth), names[nodeId],
		figureOf(nodeId, children, values, deviceNodes))
	for _, child := range children[nodeId] {
		walk(report, child, depth+1, children, names, values, deviceNodes, visited)
	}
}

// figureOf is what follows a node's name in the tree: the reading it was
// placed by and, for a node with children, how much of it they leave
// unexplained - or, for a device node the pass had no usable figure for, a
// short marker saying so. A node that is not a device, the root, carries no
// figure and gets nothing appended.
func figureOf(nodeId string, children map[string][]string, values map[string]reading, deviceNodes map[string]bool) string {
	if !deviceNodes[nodeId] {
		return ""
	}
	value, ok := values[nodeId]
	if !ok {
		return "  (no reading)"
	}
	result := fmt.Sprintf("  %.1f %v", value.value, value.carrier)

	kids := children[nodeId]
	if len(kids) == 0 {
		return result
	}
	// Unexplained is what the node measured minus what its children account
	// for. A child with no figure of its own contributes nothing to that sum -
	// a reading it never had cannot subtract from one that exists.
	explained := 0.0
	for _, child := range kids {
		if v, ok := values[child]; ok {
			explained += v.value
		}
	}
	return result + fmt.Sprintf("  (unexplained %.1f)", value.value-explained)
}

// reading is the figure a device was placed by: the carrier it reported the
// most of, and how much of it.
type reading struct {
	carrier model.Carrier
	value   float64
}

// deviceReading picks the figure a device would be placed by, out of what was
// measured for it.
//
// Same rule structure.primaryCarrier and reconcile.withoutReading decide it
// by, so the number shown here is the one that produced the tree and not a
// second opinion of it: the carrier read the most of, with zero and below
// counting as no reading at all - a meter that did not move over the window
// says nothing about containment.
func deviceReading(deviceId string, device model.Device, readings map[model.ReadingKey]model.Reading) (reading, bool) {
	result := reading{}
	found := false
	for _, carrier := range device.CarriersOf() {
		measured, ok := readings[model.ReadingKey{DeviceId: deviceId, Carrier: carrier}]
		if !ok {
			continue
		}
		if !found || measured.Value > result.value {
			result, found = reading{carrier: carrier, value: measured.Value}, true
		}
	}
	if !found || result.value <= 0 {
		return reading{}, false
	}
	return result, true
}

// nodeReadings is deviceReading indexed by node id, so the tree walk can look
// a node up directly instead of resolving its device on every line.
func nodeReadings(change model.PlannedChange) map[string]reading {
	devices := map[string]model.Device{}
	for _, device := range change.Devices {
		devices[device.Id] = device
	}
	result := map[string]reading{}
	for _, node := range change.Graph.Nodes {
		if node.ResourceType != platform.GraphResourceTypeDevice || node.ResourceId == "" {
			continue
		}
		device, known := devices[node.ResourceId]
		if !known {
			continue
		}
		if value, ok := deviceReading(node.ResourceId, device, change.Readings); ok {
			result[node.Id] = value
		}
	}
	return result
}

// deviceNames is a device's name by its id, for the two sections below the
// tree that name devices directly rather than through a graph node.
func deviceNames(change model.PlannedChange) map[string]string {
	result := map[string]string{}
	for _, device := range change.Devices {
		result[device.Id] = device.Name
	}
	return result
}

// deviceLabel is a device's name, falling back to its id - the same fallback
// labels uses, for a device the pass did not hand over.
func deviceLabel(names map[string]string, deviceId string) string {
	if name := names[deviceId]; name != "" {
		return name
	}
	return deviceId
}

// renderCandidates lists, for each device whose placement actually weighed
// more than one option, what was weighed and why the winner won. This is
// what makes a single edge arguable instead of mysterious, and it is long by
// nature - printed after the tree so the tree stays readable.
//
// A device with nothing to compare against - the largest meter of a carrier,
// or one with no reading at all - had no choice to show and is left out.
func renderCandidates(report *strings.Builder, change model.PlannedChange) {
	contested := []model.Placement{}
	for _, placement := range change.Placements {
		if len(placement.Candidates) > 0 {
			contested = append(contested, placement)
		}
	}
	if len(contested) == 0 {
		return
	}

	names := deviceNames(change)
	report.WriteString("\nhow each parent was chosen\n")
	for _, placement := range contested {
		fmt.Fprintf(report, "%v%v  %.1f %v\n", indent,
			deviceLabel(names, placement.DeviceId), placement.Value, placement.Carrier)
		for _, candidate := range placement.Candidates {
			fits := "too small"
			if candidate.Fits {
				fits = "fits"
			}
			chosen := ""
			if candidate.Chosen {
				chosen = fmt.Sprintf("  <- chosen (%v)", placement.Reason)
			}
			fmt.Fprintf(report, "%v%v  remaining %.1f  %v%v\n", strings.Repeat(indent, 2),
				deviceLabel(names, candidate.DeviceId), candidate.Remaining, fits, chosen)
		}
		// Every candidate considered and none of them marked chosen means the
		// device ended at the root instead - worth saying, since otherwise the
		// list of losers would look like it forgot to name a winner.
		if placement.Reason != model.PlacedByCapacity {
			fmt.Fprintf(report, "%v-> %v\n", strings.Repeat(indent, 2), placement.Reason)
		}
	}
}

// renderNoReading lists the devices a pass had no usable consumption figure
// for, with the reason each one sits at the root - the tree already marks
// them, but a list a reader can act on is more than a mark on a branch.
func renderNoReading(report *strings.Builder, change model.PlannedChange) {
	if len(change.NoReading) == 0 {
		return
	}

	reasons := map[string]string{}
	for _, placement := range change.Placements {
		reasons[placement.DeviceId] = placement.Reason
	}
	names := deviceNames(change)
	// Not "at the root": a device whose type reads no carrier collects under
	// the no-flow node, and only one that could have reported and did not
	// stays at the root. Saying "at the root" for both would send a reader
	// looking in the wrong place.
	report.WriteString("\nnothing could be concluded about these\n")
	for _, deviceId := range change.NoReading {
		reason := reasons[deviceId]
		if reason == "" {
			reason = model.PlacedAtRootNoReading
		}
		fmt.Fprintf(report, "%v%v  %v\n", indent, deviceLabel(names, deviceId), reason)
	}
}

// labels is the name to print per node id.
//
// A device node is named after its device, because that is the name the person
// reading this knows it by; its id follows in brackets so the name can be
// looked up. A device the pass did not hand over falls back to its id - that
// happens for a node in a graph a user built by hand.
func labels(change model.PlannedChange) map[string]string {
	deviceNames := map[string]string{}
	for _, device := range change.Devices {
		deviceNames[device.Id] = device.Name
	}
	result := map[string]string{}
	for _, node := range change.Graph.Nodes {
		switch {
		case node.ResourceType == platform.GraphResourceTypeDevice && node.ResourceId != "":
			if name := deviceNames[node.ResourceId]; name != "" {
				result[node.Id] = fmt.Sprintf("%v (%v)", name, node.ResourceId)
				continue
			}
			result[node.Id] = node.ResourceId
		default:
			if name := graphs.AttributeOf(node.Attributes, model.NodeAttrName); name != "" {
				result[node.Id] = name
				continue
			}
			result[node.Id] = node.Id
		}
	}
	return result
}

func renderSummary(report *strings.Builder, changes []model.PlannedChange) {
	groups := map[string]bool{}
	created, updated, devices, unplaced, contested := 0, 0, 0, 0, 0
	for _, change := range changes {
		groups[groupOf(change)] = true
		switch change.Action {
		case model.ActionCreate:
			created++
		case model.ActionUpdate:
			updated++
		}
		if change.Action != model.ActionCreate && change.Action != model.ActionUpdate {
			continue
		}
		for _, node := range change.Graph.Nodes {
			if node.ResourceType == platform.GraphResourceTypeDevice {
				devices++
			}
		}
		unplaced += len(change.NoReading)
		for _, placement := range change.Placements {
			if len(placement.Candidates) > 0 {
				contested++
			}
		}
	}

	report.WriteString("summary\n")
	fmt.Fprintf(report, "%vgroups affected: %v\n", indent, len(groups))
	fmt.Fprintf(report, "%vgraphs to create: %v\n", indent, created)
	fmt.Fprintf(report, "%vgraphs to update: %v\n", indent, updated)
	fmt.Fprintf(report, "%vdevice nodes in those graphs: %v\n", indent, devices)
	fmt.Fprintf(report, "%vplacements decided among competing candidates: %v\n", indent, contested)
	fmt.Fprintf(report, "%vdevices placed by no reading of their own: %v\n", indent, unplaced)
}
