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

// Package model holds the types every other package of this service speaks in.
//
// It depends on nothing but the platform models, so it can be imported from
// anywhere without creating a cycle. Anything that two packages have to agree
// about belongs here; anything only one package needs does not.
package model

import (
	"strings"
	"time"

	platform "github.com/SENERGY-Platform/models/go/models"
)

// Carrier is a medium a device measures.
//
// Not all of them are energy - water is metered by volume - but on a flow
// diagram each of them is a carrier, which is where the name comes from.
type Carrier string

const (
	Electricity Carrier = "electricity"
	Gas         Carrier = "gas"
	Oil         Carrier = "oil"
	Water       Carrier = "water"
)

// Carriers lists every carrier this service recognises, in a stable order.
var Carriers = []Carrier{Electricity, Gas, Oil, Water}

// CarrierFunctionId maps a carrier to the measuring function that identifies it.
//
// Ids rather than names: a function's display name is editable and localised,
// its id is not. Nothing resolves these against the device repository - the id
// is the identity, and a lookup would turn a stable key into a request that can
// fail.
var CarrierFunctionId = map[Carrier]string{
	Electricity: "urn:infai:ses:measuring-function:57dfd369-92db-462c-aca4-a767b52c972e",
	Gas:         "urn:infai:ses:measuring-function:0bab7253-5e8a-4e7c-9005-39724d6a2b4f",
	Oil:         "urn:infai:ses:measuring-function:a75687f5-8bca-421f-8e57-a11176c31591",
	Water:       "urn:infai:ses:measuring-function:81fbff32-c4a4-4f13-96ca-cfc7a4cfa4f0",
}

// CarrierByFunctionId is the reverse of CarrierFunctionId.
var CarrierByFunctionId = func() map[string]Carrier {
	result := map[string]Carrier{}
	for carrier, functionId := range CarrierFunctionId {
		result[functionId] = carrier
	}
	return result
}()

// ColumnPathSeparator joins the names of nested content variables into the
// column name the timescale wrapper knows a value by.
const ColumnPathSeparator = "."

// CarrierColumn is one readable value on one device: which service answers it,
// what the column is called, and which carrier it belongs to.
type CarrierColumn struct {
	ServiceId string  `json:"service_id"`
	Name      string  `json:"name"`
	Carrier   Carrier `json:"carrier"`
}

// Device is what this service needs to know about a device.
//
// Assembled from the device repository (identity, type, columns) and from
// permissions-v2 (groups). Never from a Kafka message body - see the note on
// triggers in SPEC.md.
type Device struct {
	Id           string          `json:"id"`
	Name         string          `json:"name"`
	DeviceTypeId string          `json:"device_type_id"`
	Columns      []CarrierColumn `json:"columns"`

	// Groups are the Keycloak group paths that hold at least read on this
	// device, leading slash included.
	Groups []string `json:"groups"`
}

// HasEnergyFlow reports whether the device measures any carrier at all.
//
// A device that reads none carries no flow, so nothing about its place in a
// structure can be derived from readings. Those devices are attached to the
// root rather than guessed at.
func (this Device) HasEnergyFlow() bool {
	return len(this.Columns) > 0
}

// CarriersOf lists the carriers a device measures, in Carriers order.
func (this Device) CarriersOf() []Carrier {
	found := map[Carrier]bool{}
	for _, column := range this.Columns {
		found[column.Carrier] = true
	}
	result := []Carrier{}
	for _, carrier := range Carriers {
		if found[carrier] {
			result = append(result, carrier)
		}
	}
	return result
}

// Group is a Keycloak group.
type Group struct {
	// Id is the Keycloak group id: stable across a rename, and therefore the
	// identity of the group's default graph. Not a permission key.
	Id string `json:"id"`

	// Path is the full group path with a leading slash. This is the key
	// permissions-v2 stores group rights under and the value the groups claim
	// carries, so it is what sharing is written against - but not an identity:
	// Keycloak rewrites it on a rename. See AttrKeycloakGroupId.
	Path string `json:"path"`

	// Name is what the graph is called: the leaf of Path. See GroupName.
	Name string `json:"name"`
}

// GroupName derives a graph name from a group path.
//
// The leaf, without the leading slash - a nested group is named after itself
// and not after its ancestry. A group whose own name contains a slash is the
// exception the rule has to allow for: if the path does not start with a
// slash it is not a path but a name, and it is used whole.
func GroupName(path string) string {
	if !strings.HasPrefix(path, "/") {
		return path
	}
	trimmed := strings.TrimSuffix(path, "/")
	index := strings.LastIndex(trimmed, "/")
	if index < 0 {
		return trimmed
	}
	return trimmed[index+1:]
}

// PlannedChange is one write a reconciliation pass would make.
//
// Reported whether or not writing is enabled, which is what lets a run against
// a real cluster show its work before it is allowed to do any. Nothing derived
// from it may feed back into the pass: it is an observation of a decision that
// has already been taken.
type PlannedChange struct {
	// Action is one of the Action constants below.
	Action string

	Group Group

	// Graph is the graph as it would be written. For a sharing change it is
	// the graph the sharing belongs to, unchanged.
	Graph platform.Graph

	// Devices are the devices the graph was built from, so a reader can be
	// shown names instead of ids.
	Devices []Device

	// NoReading are the ids of devices the pass had no consumption figure for.
	// They are the ones hanging off the root for a reason a reader can act on,
	// and it cannot be derived from the graph: the largest meter of a carrier
	// sits on the root too, with a reading.
	NoReading []string

	// Readings are what the pass measured, so a reader can check every edge of
	// the tree against the numbers that produced it. Keyed as usual.
	Readings map[ReadingKey]Reading

	// Placements records how each device got its parent, in the order they
	// were decided. The report shows the candidates that lost, which is the
	// only way to tell a structure that the numbers forced from one the
	// heuristic guessed.
	Placements []Placement

	// Note says what specifically changed, in words, e.g. "3 devices added".
	Note string
}

// Reasons a device ended up where it did.
const (
	// PlacedByCapacity means a parent was chosen from the candidates that
	// could still contain the device.
	PlacedByCapacity = "largest unexplained capacity"

	// PlacedAtRootNothingFits means no placed device had room for it.
	PlacedAtRootNothingFits = "no placed meter had room, so the root"

	// PlacedAtRootNoReading means the device had no usable figure, so nothing
	// could be concluded about where it belongs.
	PlacedAtRootNoReading = "no usable reading, so the root"

	// PlacedAtRootLargest means it is the biggest meter of its carrier and
	// therefore has nothing above it.
	PlacedAtRootLargest = "largest meter of its carrier, so the root"
)

// Placement is how one device got its parent.
type Placement struct {
	DeviceId string
	Carrier  Carrier

	// Value is the figure it was placed by. Zero when there was none.
	Value float64

	// ParentId is the node it was attached to - a device id, or RootNodeId.
	ParentId string

	// Reason is one of the PlacedBy/PlacedAtRoot constants.
	Reason string

	// Candidates are the meters that were considered, including the winner and
	// including those that did not fit. Empty for a device with no reading:
	// nothing was considered, which is the point.
	Candidates []Candidate
}

// Candidate is one meter weighed as a possible parent.
type Candidate struct {
	DeviceId string

	// Remaining is what it had left unexplained when this device was weighed.
	Remaining float64

	// Fits is whether Remaining could hold the device, tolerance included.
	Fits bool

	// Chosen is whether it won.
	Chosen bool
}

// The actions a PlannedChange can carry. Constants rather than literals
// because the renderer counts creations against updates, which makes the
// spelling a contract between two packages.
const (
	ActionCreate      = "create"
	ActionUpdate      = "update"
	ActionShare       = "share"
	ActionUnshare     = "unshare"
	ActionDetachGroup = "detach-group"
)

// Window is the stretch a consumption figure was measured over.
type Window struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

// Reading is what one device used of one carrier over one window.
//
// Absent is not zero: a window in which a meter never reported is not a window
// in which it used nothing, and the difference decides whether a device may be
// placed under a parent at all. A missing reading is a missing map entry, never
// a zero value.
type Reading struct {
	DeviceId  string    `json:"device_id"`
	Carrier   Carrier   `json:"carrier"`
	Value     float64   `json:"value"`
	Window    Window    `json:"window"`
	FetchedAt time.Time `json:"fetched_at"`
}

// ReadingKey identifies a reading in a map.
type ReadingKey struct {
	DeviceId string
	Carrier  Carrier
}

// Key returns the map key of a reading.
func (this Reading) Key() ReadingKey {
	return ReadingKey{DeviceId: this.DeviceId, Carrier: this.Carrier}
}

// Attribute keys and node conventions.
//
// The first two are this service's own provenance, written onto the graph
// because there is no database to keep it in. The rest are the graph view's
// conventions, which a generated graph has to match or it renders wrongly.
const (
	// AttrKeycloakGroupId is the identity of a group's default graph: the
	// Keycloak group id, which survives a rename.
	//
	// The path cannot carry this. Keycloak rewrites a group's path when it is
	// renamed, so a graph identified by path would be orphaned on a rename and
	// a second one created beside it - and the first carries whatever the user
	// had built. The id changes for nothing short of deleting the group.
	AttrKeycloakGroupId = "graph-provider/keycloak-group-id"

	// AttrKeycloakGroup is the group path the graph is currently shared with.
	//
	// Kept alongside the id because sharing needs the path: permissions-v2
	// keys group rights by the value in the token's groups claim, which is the
	// full path. On a rename the id still matches, this attribute is rewritten,
	// the old path is unshared and the new one shared.
	AttrKeycloakGroup = "graph-provider/keycloak-group"

	// AttrName records the root name this service last wrote, so a later run
	// can tell its own name from one a user chose.
	AttrName = "graph-provider/name"

	// AttrOrigin is the origin this service stamps on every attribute it
	// writes.
	AttrOrigin = "graph-provider"

	// NodeAttrName is the attribute the graph view reads a node's name from.
	// A graph's display name is the name of its root node.
	NodeAttrName = "name"

	// RootNodeId is the id the graph view expects the root to carry.
	RootNodeId = "root"

	// ResourceTypeCustom is the resource type of a node that is not a device.
	// The root is the only such node this service creates.
	ResourceTypeCustom = "dashboard-custom"

	// FullWeight is the weight of an edge whose node hangs off a single
	// parent. In a tree that is every edge.
	FullWeight = 100

	// NoFlowNodeId is the collector a device whose type reads no carrier at
	// all hangs under.
	//
	// The second synthetic node, and the only exception to "the root is the
	// only invented node". A site of a hundred devices is mostly contacts,
	// motion sensors and remotes - things that measure no medium and never
	// will - and hanging them off the root buries the two or three meters the
	// flow view is about under ninety siblings. They still have to appear:
	// every device must be in its company's graph.
	//
	// This is only for a device whose TYPE reads no carrier. A device that
	// could report and did not is a different case and stays at the root: it
	// belongs in the flow, and the reason it is not placed is missing data
	// rather than missing capability.
	NoFlowNodeId = "no-energy-flow"

	// NoFlowTranslationKey lets the graph view localise the collector's label
	// instead of showing whatever this service wrote. The view prefers the
	// name attribute, so the configured name wins until somebody adds the key.
	NoFlowTranslationKey = "fallback_translation_string"
)
