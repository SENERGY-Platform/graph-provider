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

// Package structure turns meter readings into an energy-flow graph.
//
// The semantics it encodes come from the frontend: edges point child ->
// parent, the root is the source and the devices are the sinks, and a
// sub-meter sits *inside* its parent's reading rather than on top of it. A
// supply meter reading 1000 with a strip metering 900 behind it has passed
// 1000, and the strip says where 900 of that went. That is the whole of the
// containment test - a device fits under a parent while the parent still has
// a reading left that nothing else explains.
//
// The package is pure: no I/O, no clock, no globals. The only id it does not
// derive from its input comes through Options.NewId.
package structure

import (
	"errors"
	"fmt"
	"sort"

	"github.com/SENERGY-Platform/graph-provider/pkg/model"
	"github.com/SENERGY-Platform/models/go/models"
	"github.com/google/uuid"
)

// ErrInvalidInput marks a graph that was already invalid when this package
// was handed it, as opposed to one this package broke. The two need different
// reactions - the first is a graph a user has to repair, the second a bug
// here - and a caller can only tell them apart if the error says so.
var ErrInvalidInput = errors.New("input graph is invalid")

// Options are the knobs of the heuristic.
type Options struct {
	// Tolerance is how much a child may exceed a parent's remaining
	// capacity and still be placed under it, as a fraction (0.05 = 5 %).
	//
	// Readings are taken at different moments and rounded on the way, so a
	// sub-meter can measure slightly more than its parent has left without
	// the structure being wrong.
	Tolerance float64

	// NewId produces edge ids. Nil means uuid. Tests inject a counter.
	//
	// Edge ids are derived from the pair of nodes they connect (see edgeId),
	// which is the shape the frontend produces and is unique per pair, so
	// this is only reached when an id is already taken by a foreign edge of
	// a graph a user has edited.
	NewId func() string

	// NoFlowNodeName labels the collector that devices reading no carrier
	// hang under. Empty means no collector is created and those devices go to
	// the root, which is also what happens for a site that has none of them.
	NoFlowNodeName string
}

// tolerance clamps the configured value into the range the test is defined
// on. A negative tolerance would tighten the test past exact containment,
// and one above 1 would make every candidate fit regardless of its reading.
func (this Options) tolerance() float64 {
	if this.Tolerance < 0 {
		return 0
	}
	if this.Tolerance > 1 {
		return 1
	}
	return this.Tolerance
}

func (this Options) newId() string {
	if this.NewId != nil {
		return this.NewId()
	}
	return uuid.NewString()
}

// Build creates the initial graph for a group.
//
// Graph.Id is left empty: whether this becomes a create or an update is the
// caller's decision, not the heuristic's.
func Build(owner string, group model.Group, devices []model.Device, readings map[model.ReadingKey]model.Reading, opts Options) (models.Graph, []model.Placement, error) {
	graph := models.Graph{
		Owner: owner,
		Attributes: []models.Attribute{
			{Key: model.AttrKeycloakGroupId, Value: group.Id, Origin: model.AttrOrigin},
			{Key: model.AttrKeycloakGroup, Value: group.Path, Origin: model.AttrOrigin},
			{Key: model.AttrName, Value: group.Name, Origin: model.AttrOrigin},

			// Every edge below is a guess, which is the one case where the
			// marker belongs on the graph instead of on single edges: there
			// is no edge to point at when the whole structure is derived.
			// The graph view reads it the same way either way - have the user
			// re-check the shares.
			{Key: models.GraphEdgeAttrSystemChanged, Value: "true", Origin: model.AttrOrigin},
		},
		Nodes: []models.Node{{
			Id:           model.RootNodeId,
			ResourceType: model.ResourceTypeCustom,
			Attributes: []models.Attribute{
				{Key: model.NodeAttrName, Value: group.Name, Origin: model.AttrOrigin},
			},
		}},
		Edges: []models.Edge{},
	}

	// Devices arrive in whatever order the caller assembled them in; every
	// step from here on has to be a function of the set, not of the order.
	sorted := make([]model.Device, len(devices))
	copy(sorted, devices)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Id < sorted[j].Id })

	// A device is placed exactly once. A combined heat and power unit reads
	// the gas going in and the electricity coming out; both are real, but a
	// second node would be a duplicate resource id and Graph.Valid rejects
	// it. The larger reading decides which pass the device takes part in.
	perCarrier := map[model.Carrier][]placement{}
	parentOf := map[string]string{}
	placements := []model.Placement{}
	noFlow := false
	for _, device := range sorted {
		graph.Nodes = append(graph.Nodes, models.Node{
			// id == resource_id on device nodes is a frontend convention,
			// not a formality: the graph view looks a node's device up by
			// its node id.
			Id:           device.Id,
			ResourceId:   device.Id,
			ResourceType: models.GraphResourceTypeDevice,
			Attributes:   []models.Attribute{},
		})

		carrier, value, ok := primaryCarrier(device.Id, device.CarriersOf(), readings)
		if !ok {
			// No carrier columns at all, or columns but no usable number for
			// any of them - zero and below count as none, see
			// primaryCarrier. Nothing is concluded about such a device, and it
			// is not offered as a parent either.
			//
			// Where it hangs depends on WHY there is no reading, and the two
			// cases are not alike. A device whose type reads no carrier never
			// will, and a real site has many of them - contacts, motion
			// sensors, remotes - so they collect under one node rather than
			// burying the two or three meters under ninety siblings. A device
			// that could have reported and did not belongs in the flow and
			// stays at the root, where it stands out as something to look
			// into.
			parent := model.RootNodeId
			if !device.HasEnergyFlow() && opts.NoFlowNodeName != "" {
				parent = model.NoFlowNodeId
				noFlow = true
			}
			parentOf[device.Id] = parent
			placements = append(placements, model.Placement{
				DeviceId: device.Id,
				ParentId: parent,
				Reason:   model.PlacedAtRootNoReading,
			})
			continue
		}
		perCarrier[carrier] = append(perCarrier[carrier], placement{
			deviceId: device.Id,
			name:     device.Name,
			value:    value,
			carrier:  carrier,
		})
	}

	// One pass per carrier: a gas meter can never be the sub-meter of an
	// electricity meter, however well the numbers would fit.
	for _, carrier := range model.Carriers {
		placements = append(placements, place(perCarrier[carrier], opts.tolerance(), parentOf)...)
	}

	// The collector, and the one place this service invents a node other than
	// the root. Created only when something needs it.
	takenEdgeIds := map[string]bool{}
	if noFlow {
		graph.Nodes = append(graph.Nodes, models.Node{
			Id:           model.NoFlowNodeId,
			ResourceType: model.ResourceTypeCustom,
			Attributes: []models.Attribute{
				{Key: model.NodeAttrName, Value: opts.NoFlowNodeName, Origin: model.AttrOrigin},
				{Key: model.NoFlowTranslationKey, Value: "noEnergyFlow", Origin: model.AttrOrigin},
			},
		})
		graph.Edges = append(graph.Edges, models.Edge{
			Id:         uniqueEdgeId(takenEdgeIds, model.NoFlowNodeId, model.RootNodeId, opts),
			FromNodeId: model.NoFlowNodeId,
			ToNodeId:   model.RootNodeId,
			Weight:     model.FullWeight,
			Attributes: []models.Attribute{},
		})
	}
	for _, device := range sorted {
		graph.Edges = append(graph.Edges, models.Edge{
			Id:         uniqueEdgeId(takenEdgeIds, device.Id, parentOf[device.Id], opts),
			FromNodeId: device.Id,
			ToNodeId:   parentOf[device.Id],
			// In a tree every node has exactly one parent, so there is no
			// share to estimate and every edge carries the whole node.
			Weight:     model.FullWeight,
			Attributes: []models.Attribute{},
		})
	}
	sort.Slice(graph.Edges, func(i, j int) bool { return graph.Edges[i].Id < graph.Edges[j].Id })

	// The device repository answers an invalid graph with a 400. Checking
	// here separates a bug in the heuristic from a broken request.
	if err := validate(&graph); err != nil {
		return graph, placements, fmt.Errorf("generated graph is invalid: %w", err)
	}

	// Placements in the order the devices were weighed, so a report can be
	// read top down against the tree it explains.
	sort.SliceStable(placements, func(i, j int) bool {
		return placements[i].Value > placements[j].Value
	})
	return graph, placements, nil
}

// Attach places one device into an existing graph without restructuring
// anything already there.
//
// The graph may have been corrected by hand since it was generated. Undoing
// that silently is worse than leaving a suboptimal guess in place, so this
// only ever adds a node and an edge.
func Attach(graph *models.Graph, device model.Device, readings map[model.ReadingKey]model.Reading, opts Options) ([]model.Placement, error) {
	if graph == nil {
		return nil, errors.New("attach: graph is nil")
	}
	if device.Id == "" {
		return nil, errors.New("attach: device has no id")
	}
	if findDeviceNode(graph, device.Id) != "" {
		return nil, nil
	}
	// Checked before anything is changed, so a graph a user broke elsewhere -
	// two sinks after a deleted edge, say - is not reported as damage this
	// attach did. The reader of the log is otherwise sent after the wrong
	// cause on every pass.
	if err := validate(graph); err != nil {
		return nil, fmt.Errorf("%w: cannot attach %v: %w", ErrInvalidInput, device.Id, err)
	}

	// Everything happens on a copy: a graph that fails validation must not
	// reach the caller in a half-changed state.
	working := clone(*graph)
	rootId, err := rootNodeId(&working)
	if err != nil {
		return nil, fmt.Errorf("attach %v: %w", device.Id, err)
	}

	parentId := rootId
	// A device reading no carrier joins the collector when the graph already
	// has one, so Attach and Build put the same kind of device in the same
	// place. Attach never creates the collector: adding a node to a graph a
	// user may have rearranged is a restructuring, and Attach does not do
	// those.
	if !device.HasEnergyFlow() && findNode(&working, model.NoFlowNodeId) {
		parentId = model.NoFlowNodeId
	}
	placement := model.Placement{
		DeviceId: device.Id,
		ParentId: parentId,
		Reason:   model.PlacedAtRootNoReading,
	}
	if carrier, value, ok := primaryCarrier(device.Id, device.CarriersOf(), readings); ok {
		placement.Carrier = carrier
		placement.Value = value
		placement.ParentId = rootId
		placement.Reason = model.PlacedAtRootNothingFits
		candidate, candidates, found := bestExistingParent(&working, carrier, value, readings, opts.tolerance())
		placement.Candidates = candidates
		if found {
			parentId = candidate
			placement.ParentId = candidate
			placement.Reason = model.PlacedByCapacity
		}
	}

	working.Nodes = append(working.Nodes, models.Node{
		Id:           device.Id,
		ResourceId:   device.Id,
		ResourceType: models.GraphResourceTypeDevice,
		Attributes:   []models.Attribute{},
	})
	taken := map[string]bool{}
	for _, edge := range working.Edges {
		taken[edge.Id] = true
	}
	working.Edges = append(working.Edges, models.Edge{
		Id:         uniqueEdgeId(taken, device.Id, parentId, opts),
		FromNodeId: device.Id,
		ToNodeId:   parentId,
		Weight:     model.FullWeight,
		Attributes: []models.Attribute{
			// The frontend's signal to have the user re-check the shares:
			// this edge is a guess the service made, not something the user
			// entered.
			{Key: models.GraphEdgeAttrSystemChanged, Value: "true", Origin: model.AttrOrigin},
		},
	})

	if err := validate(&working); err != nil {
		return nil, fmt.Errorf("attaching %v produced an invalid graph: %w", device.Id, err)
	}
	*graph = working
	return []model.Placement{placement}, nil
}

// Detach removes a device's node, letting the model reroute the edges.
//
// The three outcomes are distinguished on purpose. (false, nil) is a device
// that had no node: nothing to do, and nothing to report. (false, err) is a
// removal that could not be carried out - the graph is handed back untouched,
// because a stale node is better than a graph the device repository refuses,
// but the caller has to hear about it: every following pass will refuse the
// same removal, and without the error the device stays in the graph forever
// with nobody being told.
func Detach(graph *models.Graph, deviceId string) (removed bool, err error) {
	if graph == nil {
		return false, errors.New("detach: graph is nil")
	}
	if deviceId == "" {
		return false, errors.New("detach: device has no id")
	}
	nodeId := findDeviceNode(graph, deviceId)
	if nodeId == "" {
		return false, nil
	}
	// Everything happens on a copy: a rerouting that ends up invalid, or that
	// gives up halfway through a panic, must not reach the caller.
	working := clone(*graph)
	if err := guarded("removing node "+nodeId, func() { working.DeleteNode(nodeId) }); err != nil {
		return false, err
	}
	if err := validate(&working); err != nil {
		return false, fmt.Errorf("removing %v would leave the graph invalid: %w", deviceId, err)
	}
	*graph = working
	return true, nil
}

// guarded runs a call into the platform model and turns a panic into an error.
//
// models.Graph.ensureValidEdgeWeights, which DeleteNode reaches through
// rerouteEdge, computes the index of an edge's system_changed attribute and
// then writes edge.Attributes[i] with i, the index of the *edge*. An edge that
// already carries the attribute and sits further along Edges than it has
// attributes therefore takes the process down; six of the 99 two-way splits a
// user can enter on a node reproduce it, 29/71 among them, and integer
// truncation of the rerouted weights is what gets there. The bug belongs to
// the models module, which this repository cannot write to, so the guard is
// the local defence: without it the panic kills the reconcile goroutine, and
// the startup sweep meets the same graph again, which turns one bad graph into
// a crash loop. It becomes an error rather than a silence because the caller
// is the only one who can name the graph that does this.
func guarded(what string, call func()) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("%v panicked in the platform model: %v", what, recovered)
		}
	}()
	call()
	return nil
}

// validate is models.Graph.Valid behind the same guard. Every call into the
// model goes through one; see guarded.
func validate(graph *models.Graph) error {
	var result error
	if err := guarded("validating the graph", func() { result = graph.Valid() }); err != nil {
		return err
	}
	return result
}

// placement is one device competing for a place under a parent of its own
// carrier.
type placement struct {
	deviceId string
	name     string
	value    float64
	carrier  model.Carrier
}

// place assigns every device of one carrier to a parent and records it in
// parentOf, keyed by device id.
//
// Descending by reading, because a sub-meter is never larger than the meter
// it sits inside of. The largest device has no candidate above it and
// becomes a child of the root, which falls out of the loop rather than
// needing a case of its own.
func place(devices []placement, tolerance float64, parentOf map[string]string) []model.Placement {
	sort.Slice(devices, func(i, j int) bool {
		if devices[i].value != devices[j].value {
			return devices[i].value > devices[j].value
		}
		return devices[i].deviceId < devices[j].deviceId
	})

	// remaining[i] is what device i has measured that no child of it
	// explains. Placing a child reduces its parent's remaining only -
	// capacity is about what a meter itself still has unaccounted for, and
	// the grandparent's reading already contains the child either way.
	remaining := make([]float64, len(devices))
	depth := make([]int, len(devices))
	placements := make([]model.Placement, 0, len(devices))

	for i, device := range devices {
		best := -1
		candidates := []model.Candidate{}
		for j := 0; j < i; j++ {
			// A meter whose remainder has all but vanished against its own
			// reading is accounted for by the children it already has, and a
			// further child would have to fit into a gap that is measurement
			// noise. Without this the shallowest-first rule pulls small
			// consumers up to the top meter of a site whose sub-main explains
			// it entirely - measured: a 156 kWh supply meter with 0.1 left
			// still collected four devices that belong under the 155.9 kWh
			// meter below it.
			explained := remaining[j] < devices[j].value*tolerance
			fits := !explained && remaining[j] >= device.value*(1-tolerance)
			candidates = append(candidates, model.Candidate{
				DeviceId:  devices[j].deviceId,
				Remaining: remaining[j],
				Fits:      fits,
			})
			if !fits {
				continue
			}
			if best == -1 || shallower(devices[j], remaining[j], depth[j], devices[best], remaining[best], depth[best], device.name) {
				best = j
			}
		}

		reason := model.PlacedByCapacity
		parent := model.RootNodeId
		switch {
		case best == -1 && i == 0:
			reason = model.PlacedAtRootLargest
		case best == -1:
			reason = model.PlacedAtRootNothingFits
		default:
			parent = devices[best].deviceId
			candidates[best].Chosen = true
			remaining[best] -= device.value
			depth[i] = depth[best] + 1
		}
		if best == -1 {
			depth[i] = 1
		}
		parentOf[device.deviceId] = parent
		remaining[i] = device.value

		placements = append(placements, model.Placement{
			DeviceId:   device.deviceId,
			Carrier:    device.carrier,
			Value:      device.value,
			ParentId:   parent,
			Reason:     reason,
			Candidates: candidates,
		})
	}
	return placements
}

// shallower reports whether a is the better parent than b.
//
// The shallowest parent that can contain the device wins. Remaining capacity
// decides only *whether* a candidate is eligible; it is a constraint, not
// evidence. From totals alone the depth of a metering hierarchy is not
// determinable except in the near-identity case, so the rule has to encode a
// prior - and the prior in SPEC.md is that a guessed level is work a user has
// to undo, so a level has to be forced before it is invented.
//
// Both capacity-ordered rules were tried against a real site and both chained:
//
//   - Tightest fit (smallest remaining) chains always. A device's remaining is
//     its full reading the moment it is placed, and the list is walked
//     descending, so the device placed last has the smallest full reading among
//     those placed and wins whenever it fits at all. Nine levels where two were
//     right.
//   - Roomiest fit (largest remaining) chains less, but still: placing a child
//     lowers the parent's remaining by the child's value while the child keeps
//     its full value, so a child taking more than half of what its parent had
//     left becomes roomier than that parent and catches the next device.
//
// Depth-first has neither failure mode, because depth only ever grows when
// every shallower candidate is out of capacity - which is exactly the evidence
// that the shallower meter is already accounted for.
//
// A consequence worth knowing: at the time device i is weighed, its predecessor
// still has its full reading as remaining (its own children can only come
// later) and that reading is >= device i's. So a device with a reading always
// has some eligible parent, and in a generated graph only the largest meter of
// each carrier and the devices without readings sit at the root.
//
// Ties break towards the roomier parent - among equally shallow candidates,
// leaving the most room unspent postpones the next forced level - then by the
// longest common name prefix, then by id so the outcome does not depend on
// input order.
func shallower(a placement, remainingA float64, depthA int, b placement, remainingB float64, depthB int, name string) bool {
	if depthA != depthB {
		return depthA < depthB
	}
	if remainingA != remainingB {
		return remainingA > remainingB
	}
	prefixA, prefixB := commonPrefixLength(a.name, name), commonPrefixLength(b.name, name)
	if prefixA != prefixB {
		return prefixA > prefixB
	}
	return a.deviceId < b.deviceId
}

func bestExistingParent(graph *models.Graph, carrier model.Carrier, value float64, readings map[model.ReadingKey]model.Reading, tolerance float64) (string, []model.Candidate, bool) {
	childrenOf := map[string][]string{}
	for _, edge := range graph.Edges {
		childrenOf[edge.ToNodeId] = append(childrenOf[edge.ToNodeId], edge.FromNodeId)
	}
	resourceIdOf := map[string]string{}
	for _, node := range graph.Nodes {
		resourceIdOf[node.Id] = node.ResourceId
	}

	// Depth per node, so the same shallowest-first rule as Build can be
	// applied here. Measured by walking up the parent edges; a graph a user
	// has rebuilt is still a tree, so the walk terminates.
	parentOf := map[string]string{}
	for _, edge := range graph.Edges {
		parentOf[edge.FromNodeId] = edge.ToNodeId
	}
	depthOf := func(nodeId string) int {
		depth := 0
		for step := 0; step < len(graph.Nodes)+1; step++ {
			parent, has := parentOf[nodeId]
			if !has {
				return depth
			}
			depth++
			nodeId = parent
		}
		return depth
	}

	bestId := ""
	bestRemaining := 0.0
	bestDepth := 0
	found := false
	candidates := []model.Candidate{}
	for _, node := range graph.Nodes {
		if node.ResourceType != models.GraphResourceTypeDevice || node.ResourceId == "" {
			continue
		}
		// The carriers a node measures are not stored on the graph, so they
		// are read back out of the readings. A node whose largest reading is
		// for another carrier belongs to that carrier's tree and is no
		// candidate here.
		nodeCarrier, nodeValue, ok := primaryCarrier(node.ResourceId, model.Carriers, readings)
		if !ok || nodeCarrier != carrier {
			continue
		}
		remaining := nodeValue
		for _, childNodeId := range childrenOf[node.Id] {
			if reading, ok := readings[model.ReadingKey{DeviceId: resourceIdOf[childNodeId], Carrier: carrier}]; ok {
				remaining -= reading.Value
			}
		}
		// Same rule as Build's: a meter its own children already explain is
		// not a candidate, however shallow it sits.
		explained := remaining < nodeValue*tolerance
		fits := !explained && remaining >= value*(1-tolerance)
		candidates = append(candidates, model.Candidate{
			DeviceId:  node.ResourceId,
			Remaining: remaining,
			Fits:      fits,
		})
		if !fits {
			continue
		}
		// Shallowest first, then roomiest - the same rule as Build's
		// shallower, and it has to be the same or the two would answer one
		// case two ways. Ties fall through to the node id; Attach is handed
		// one device and cannot see the other nodes' names, so it has no
		// prefix to weigh.
		depth := depthOf(node.Id)
		better := !found ||
			depth < bestDepth ||
			(depth == bestDepth && remaining > bestRemaining) ||
			(depth == bestDepth && remaining == bestRemaining && node.Id < bestId)
		if better {
			bestId, bestRemaining, bestDepth, found = node.Id, remaining, depth, true
		}
	}
	for i := range candidates {
		candidates[i].Chosen = found && candidates[i].DeviceId == bestId
	}
	return bestId, candidates, found
}

// edgeId is the shape the frontend produces. It is unique per pair, which is
// what makes a generated graph comparable to a stored one edge by edge.
func edgeId(fromNodeId string, toNodeId string) string {
	return "edge-" + fromNodeId + "-" + toNodeId
}

// uniqueEdgeId keeps edgeId's shape unless a graph a user has edited already
// carries that id on some other edge; Graph.Valid rejects a duplicate.
func uniqueEdgeId(taken map[string]bool, fromNodeId string, toNodeId string, opts Options) string {
	id := edgeId(fromNodeId, toNodeId)
	for taken[id] {
		id = opts.newId()
	}
	taken[id] = true
	return id
}

// primaryCarrier is the carrier a device is placed by: the one it read the
// most of.
//
// carriers is expected in model.Carriers order, which is what makes the
// comparison decide ties by that order.
//
// A figure of zero or less counts as no reading at all. Zero is not a small
// consumption here: a meter that stands at the same value at both ends of the
// window - gas in summer, water in an empty building - contains nothing, so
// containment says nothing about it, while the test itself would happily read
// "0 fits into 0" and hang every silent meter under the first of them. A
// negative figure, from a meter exchange or a counter rollover, is no better
// grounded and falls under the same rule.
func primaryCarrier(deviceId string, carriers []model.Carrier, readings map[model.ReadingKey]model.Reading) (model.Carrier, float64, bool) {
	result := model.Carrier("")
	value := 0.0
	found := false
	for _, carrier := range carriers {
		reading, ok := readings[model.ReadingKey{DeviceId: deviceId, Carrier: carrier}]
		if !ok {
			continue
		}
		if !found || reading.Value > value {
			result, value, found = carrier, reading.Value, true
		}
	}
	if !found || value <= 0 {
		return model.Carrier(""), 0, false
	}
	return result, value, true
}

// rootNodeId finds the node every other node ultimately points at.
//
// By convention that is the node called root. A graph whose root a user
// replaced still has exactly one node without an outgoing edge, because
// Graph.Valid demands it, so that is the fallback.
func rootNodeId(graph *models.Graph) (string, error) {
	for _, node := range graph.Nodes {
		if node.Id == model.RootNodeId {
			return node.Id, nil
		}
	}
	hasOutgoing := map[string]bool{}
	for _, edge := range graph.Edges {
		hasOutgoing[edge.FromNodeId] = true
	}
	ends := []string{}
	for _, node := range graph.Nodes {
		if !hasOutgoing[node.Id] {
			ends = append(ends, node.Id)
		}
	}
	if len(ends) != 1 {
		return "", fmt.Errorf("graph has no usable root: %v nodes without an outgoing edge", len(ends))
	}
	return ends[0], nil
}

// findDeviceNode returns the id of the node standing for a device, or the
// empty string. The resource id is the authoritative link; the node id
// matching it is the convention this service writes but not one a
// user-edited graph is bound by.
func findDeviceNode(graph *models.Graph, deviceId string) string {
	for _, node := range graph.Nodes {
		if node.ResourceId == deviceId {
			return node.Id
		}
	}
	for _, node := range graph.Nodes {
		if node.Id == deviceId {
			return node.Id
		}
	}
	return ""
}

func commonPrefixLength(a string, b string) int {
	runesA, runesB := []rune(a), []rune(b)
	count := 0
	for count < len(runesA) && count < len(runesB) && runesA[count] == runesB[count] {
		count++
	}
	return count
}

// clone copies a graph down to its attribute slices. The model's own
// mutations write through those slices, so a shallow copy would change the
// original while a change is still being tried out.
func clone(graph models.Graph) models.Graph {
	result := graph
	result.Attributes = cloneAttributes(graph.Attributes)
	result.Nodes = make([]models.Node, len(graph.Nodes))
	for i, node := range graph.Nodes {
		node.Attributes = cloneAttributes(node.Attributes)
		result.Nodes[i] = node
	}
	result.Edges = make([]models.Edge, len(graph.Edges))
	for i, edge := range graph.Edges {
		edge.Attributes = cloneAttributes(edge.Attributes)
		result.Edges[i] = edge
	}
	return result
}

func cloneAttributes(attributes []models.Attribute) []models.Attribute {
	if attributes == nil {
		return nil
	}
	result := make([]models.Attribute, len(attributes))
	copy(result, attributes)
	return result
}

// findNode reports whether a node with that id is in the graph.
func findNode(graph *models.Graph, nodeId string) bool {
	for _, node := range graph.Nodes {
		if node.Id == nodeId {
			return true
		}
	}
	return false
}
