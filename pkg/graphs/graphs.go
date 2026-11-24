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

// Package graphs is the service's access to stored graphs and their sharing.
//
// It is the only writer in this service, which is also where the kill switch
// lives: with writing disabled everything else still runs, reads and computes,
// and nothing leaves the process.
package graphs

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"time"

	devicerepomodel "github.com/SENERGY-Platform/device-repository/lib/model"
	"github.com/SENERGY-Platform/graph-provider/pkg/model"
	"github.com/SENERGY-Platform/graph-provider/pkg/things"
	platform "github.com/SENERGY-Platform/models/go/models"
	permissions "github.com/SENERGY-Platform/permissions-v2/pkg/client"
)

// pageSize is how many graphs are asked for per request.
const pageSize = 500

// Repository is the part of the device repository client this package uses.
//
// ReadGraph and DeleteGraph are absent because neither is needed, not because
// they could not be called: a single graph is read through ListGraphs with an
// id filter, and a graph is never deleted - a group that disappears has its
// sharing withdrawn, not its user's work removed. (An earlier version of this
// comment claimed those two lacked an admin bypass. They do not:
// permissions-v2 returns early for a token with the admin role in
// CheckTopicDefaultPermissionContext, so every check passes for this service's
// token. The conclusion stood for the wrong reason.)
type Repository interface {
	ListGraphs(token string, options devicerepomodel.GraphListOptions) (result []platform.Graph, total int64, err error, errCode int)
	SetGraph(token string, graph platform.Graph) (result platform.Graph, err error, code int)
}

// Permissions is the part of the permissions-v2 client this package uses.
type Permissions interface {
	GetResource(token string, topicId string, id string) (result permissions.Resource, err error, code int)
	SetPermission(token string, topicId string, id string, perms permissions.ResourcePermissions) (result permissions.ResourcePermissions, err error, code int)
}

// Things is the part of the repository's thing endpoints this package uses.
type Things interface {
	List(ctx context.Context, token string, ids []string, deviceIds []string) ([]things.Thing, error, int)
	Create(ctx context.Context, token string, thing things.Thing) (things.Thing, error, int)
	AddDeviceLink(ctx context.Context, token string, thingId string, link things.LinkCreate) (things.Thing, error, int)
	Delete(ctx context.Context, token string, thingId string) (error, int)

	EndDeviceLink(ctx context.Context, token string, thingId string, linkId string, until time.Time) (things.Thing, error, int)

	// ChangeGraph writes a graph as a change at an instant, see things.Client.
	// Here because it is the same foreign client; used for writes that remove.
	ChangeGraph(ctx context.Context, token string, graph platform.Graph, at time.Time) (platform.Graph, error, int)
}

type Store struct {
	repository  Repository
	permissions Permissions
	things      Things

	token         string
	graphTopic    string
	thingTopic    string
	serviceUserId string

	// writable false is the kill switch. Reads still happen, writes are
	// reported as skipped.
	writable bool
}

func New(repository Repository, perms Permissions, thingClient Things, token string, graphTopic string, thingTopic string, serviceUserId string, writable bool) *Store {
	return &Store{
		repository:    repository,
		permissions:   perms,
		things:        thingClient,
		token:         token,
		graphTopic:    graphTopic,
		thingTopic:    thingTopic,
		serviceUserId: serviceUserId,
		writable:      writable,
	}
}

// ErrReadOnly is returned instead of writing when the kill switch is set.
var ErrReadOnly = errors.New("writing is disabled by configuration")

// ErrConflict marks a write the repository answered with 409: its
// compare-and-set lost the race against another writer five times in a row. The
// edit is a diff against the stored graph, so nothing is wrong with it - the
// next pass reads the graph again and applies it again. Handled like any other
// transient write failure, only named, so the log says what happened.
var ErrConflict = errors.New("the repository's compare-and-set lost against another writer")

func (this *Store) Writable() bool {
	return this.writable
}

// Defaults is this service's own graphs, indexed the two ways they can be
// recognised.
//
// ById is the normal case. ByPath holds only graphs that carry the path
// attribute without an id - graphs written before the id was recorded, or a
// write that was interrupted between the two. The caller adopts those and
// writes the id, so the second map empties itself over time.
type Defaults struct {
	ById   map[string]platform.Graph
	ByPath map[string]platform.Graph
}

// Defaults returns this service's own graphs.
//
// Filtered by the presence of either provenance attribute rather than by their
// values. A value would have to travel through the client's attributes_json
// parameter, which double-escapes it - the client percent-encodes the JSON and
// the query encoder escapes it again, so the repository is handed something
// that is no longer JSON and answers 400. Confirmed against the real
// repository in pkg/tests. The number of default graphs is the number of
// groups, so matching the values here costs nothing.
//
// One request per key, not one request naming both. The repository's plain
// attributes filter is documented as matching an element that carries an
// attribute "that is in the given list", but it builds one $elemMatch per
// listed attribute and ANDs them: a two-key filter returns only graphs
// carrying BOTH keys, and the graph that carries the path alone - written
// before the id was recorded, or a write interrupted between the two - would
// be invisible, so the service would create a second graph beside the one the
// user had built. pkg/tests pins this.
//
// Leaving Ids nil is what earns the admin bypass in the repository's graph
// controller: an id filter would send the call through a per-id permission
// check instead.
func (this *Store) Defaults(ctx context.Context) (Defaults, error) {
	result := Defaults{
		ById:   map[string]platform.Graph{},
		ByPath: map[string]platform.Graph{},
	}
	for _, key := range []string{model.AttrKeycloakGroupId, model.AttrKeycloakGroup} {
		if err := this.collect(ctx, key, result); err != nil {
			return result, err
		}
	}
	return result, nil
}

// collect adds every graph carrying one attribute key to result.
//
// A graph carrying both keys arrives twice, once per call. Indexing it is
// idempotent: keepLower is handed the same id both times.
func (this *Store) collect(ctx context.Context, attributeKey string, result Defaults) error {
	for offset := int64(0); ; offset += pageSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, _, err, _ := this.repository.ListGraphs(this.token, devicerepomodel.GraphListOptions{
			Limit:      pageSize,
			Offset:     offset,
			SortBy:     "id.asc",
			Attributes: []platform.Attribute{{Key: attributeKey}},
		})
		if err != nil {
			return fmt.Errorf("unable to list graphs by %v: %w", attributeKey, err)
		}
		for _, graph := range page {
			if groupId := GroupIdOf(graph); groupId != "" {
				keepLower(result.ById, groupId, graph)
				continue
			}
			if path := GroupPathOf(graph); path != "" {
				keepLower(result.ByPath, path, graph)
			}
		}
		if int64(len(page)) < pageSize {
			return nil
		}
	}
}

// keepLower stores graph under key, keeping the lower id when two graphs claim
// the same group.
//
// Two graphs claiming one group is a state this service cannot have produced.
// Choosing the lower id makes the outcome stable instead of dependent on paging
// order, and leaves the duplicate untouched for someone to look at.
func keepLower(into map[string]platform.Graph, key string, graph platform.Graph) {
	if previous, taken := into[key]; taken && previous.Id < graph.Id {
		return
	}
	into[key] = graph
}

// Get reads one graph back by id.
func (this *Store) Get(ctx context.Context, id string) (platform.Graph, bool, error) {
	if err := ctx.Err(); err != nil {
		return platform.Graph{}, false, err
	}
	page, _, err, _ := this.repository.ListGraphs(this.token, devicerepomodel.GraphListOptions{
		Ids: []string{id},
	})
	if err != nil {
		return platform.Graph{}, false, fmt.Errorf("unable to read graph %v: %w", id, err)
	}
	if len(page) == 0 {
		return platform.Graph{}, false, nil
	}
	return page[0], true, nil
}

// Save creates or updates a graph.
//
// Validated locally first: the repository would answer an invalid graph with a
// 400, and failing here separates a heuristic bug from a transport problem.
func (this *Store) Save(ctx context.Context, graph platform.Graph) (platform.Graph, error) {
	if err := graph.Valid(); err != nil {
		return graph, fmt.Errorf("refusing to write an invalid graph: %w", err)
	}
	if !this.writable {
		return graph, ErrReadOnly
	}
	if err := ctx.Err(); err != nil {
		return graph, err
	}
	result, err, code := this.repository.SetGraph(this.token, graph)
	return this.written(graph, result, err, code)
}

// SaveRemoval writes a graph from which something was removed.
//
// As a change at the given instant, not as a correction: a correction deletes
// the removed node from the stored document, history included, and the
// repository refuses it (400, naming the instant) while an ended version of an
// edge still points at the node. A change ends the node and its edges at the
// instant and keeps everything before it. Writes that only add stay a
// correction without an instant, see Save.
func (this *Store) SaveRemoval(ctx context.Context, graph platform.Graph, at time.Time) (platform.Graph, error) {
	if err := graph.Valid(); err != nil {
		return graph, fmt.Errorf("refusing to write an invalid graph: %w", err)
	}
	if !this.writable {
		return graph, ErrReadOnly
	}
	if err := ctx.Err(); err != nil {
		return graph, err
	}
	result, err, code := this.things.ChangeGraph(ctx, this.token, graph, at)
	return this.written(graph, result, err, code)
}

func (this *Store) written(graph platform.Graph, result platform.Graph, err error, code int) (platform.Graph, error) {
	if err != nil {
		if code == http.StatusConflict {
			return graph, fmt.Errorf("unable to write graph %v (%v): %w: %w", graph.Id, code, ErrConflict, err)
		}
		return graph, fmt.Errorf("unable to write graph %v (%v): %w", graph.Id, code, err)
	}
	return result, nil
}

// Share gives a group read, write and execute on a graph, and nothing more.
//
// Not administrate: members are meant to edit the company graph, not to delete
// it or hand it on. Administrate stays with the owner.
//
// The current state is read first and the write skipped when it already
// agrees. Every write here produces a rights command on the graph topic that
// this service consumes itself; without the comparison that echo would ask for
// another write and the loop would not end.
func (this *Store) Share(ctx context.Context, graphId string, groupPath string) (changed bool, err error) {
	return this.setGroupPermission(ctx, this.graphTopic, "graph", graphId, groupPath, &permissions.PermissionsMap{
		Read:    true,
		Write:   true,
		Execute: true,
	})
}

// Unshare withdraws a group's access and leaves everything else alone.
//
// This is what a disappeared group gets. The graph is not deleted: removing a
// user's work in response to a directory event is the harsher of the two
// actions, and the graph stays usable to its owner.
func (this *Store) Unshare(ctx context.Context, graphId string, groupPath string) (changed bool, err error) {
	return this.setGroupPermission(ctx, this.graphTopic, "graph", graphId, groupPath, nil)
}

// ShareThing gives a group the same rights on a thing as on its graph: read,
// write and execute, administrate stays with the owner. A member who may read
// the company graph has to be able to read the meter things it points at, or
// the graph shows nodes the member cannot resolve.
func (this *Store) ShareThing(ctx context.Context, thingId string, groupPath string) (changed bool, err error) {
	return this.setGroupPermission(ctx, this.thingTopic, "thing", thingId, groupPath, &permissions.PermissionsMap{
		Read:    true,
		Write:   true,
		Execute: true,
	})
}

// UnshareThing withdraws a group's access to a thing.
func (this *Store) UnshareThing(ctx context.Context, thingId string, groupPath string) (changed bool, err error) {
	return this.setGroupPermission(ctx, this.thingTopic, "thing", thingId, groupPath, nil)
}

// setGroupPermission writes one group's entry, or removes it when want is nil.
func (this *Store) setGroupPermission(ctx context.Context, topic string, noun string, graphId string, groupPath string, want *permissions.PermissionsMap) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	resource, err, code := this.permissions.GetResource(this.token, topic, graphId)
	if err != nil && code != http.StatusNotFound {
		return false, fmt.Errorf("unable to read permissions of %v %v: %w", noun, graphId, err)
	}
	if code == http.StatusNotFound {
		// The graph has no permission resource, so there is nothing to share
		// and no safe way to invent one: writing permissions requires at least
		// one administrating user, and guessing who that is would be worse
		// than reporting that the graph is not ready yet.
		return false, fmt.Errorf("%v %v has no permission resource", noun, graphId)
	}

	next := clonePermissions(resource.ResourcePermissions)
	current, held := next.GroupPermissions[groupPath]
	switch {
	case want == nil && !held:
		return false, nil
	case want != nil && held && current == *want:
		return false, nil
	case want == nil:
		delete(next.GroupPermissions, groupPath)
	default:
		next.GroupPermissions[groupPath] = *want
	}

	// The owner has to keep administrate, or permissions-v2 rejects the whole
	// write: a resource with no administrating user is invalid by its model.
	owner := next.UserPermissions[this.serviceUserId]
	if !owner.Administrate {
		next.UserPermissions[this.serviceUserId] = permissions.PermissionsMap{
			Read: true, Write: true, Execute: true, Administrate: true,
		}
	}
	if !next.Valid() {
		return false, fmt.Errorf("%v %v would be left without an administrating user", noun, graphId)
	}

	if !this.writable {
		return false, ErrReadOnly
	}
	if _, err, code = this.permissions.SetPermission(this.token, topic, graphId, next); err != nil {
		return false, fmt.Errorf("unable to write permissions of %v %v (%v): %w", noun, graphId, code, err)
	}
	return true, nil
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

// GroupIdOf is the Keycloak group id a graph is the default graph of, or "".
//
// The identity, because it survives a rename. See model.AttrKeycloakGroupId.
func GroupIdOf(graph platform.Graph) string {
	return AttributeOf(graph.Attributes, model.AttrKeycloakGroupId)
}

// GroupPathOf is the group path a graph is currently shared with, or "".
func GroupPathOf(graph platform.Graph) string {
	return AttributeOf(graph.Attributes, model.AttrKeycloakGroup)
}

// GeneratedNameOf is the root name this service last wrote onto a graph.
//
// The one piece of history the service needs about its own work: comparing it
// to the root's current name is how a group rename is distinguished from a
// name a user chose. Provenance travels with the object rather than sitting in
// a store this service would have to keep in step.
func GeneratedNameOf(graph platform.Graph) string {
	return AttributeOf(graph.Attributes, model.AttrName)
}

// AttributeOf returns the value of the named attribute, or "".
func AttributeOf(attributes []platform.Attribute, key string) string {
	for _, attribute := range attributes {
		if attribute.Key == key {
			return attribute.Value
		}
	}
	return ""
}

// SetAttribute writes an attribute, replacing any existing one with that key.
func SetAttribute(attributes []platform.Attribute, key string, value string) []platform.Attribute {
	result := slices.DeleteFunc(slices.Clone(attributes), func(attribute platform.Attribute) bool {
		return attribute.Key == key
	})
	return append(result, platform.Attribute{Key: key, Value: value, Origin: model.AttrOrigin})
}

// RootOf is the node nothing points up from - the top of the graph.
//
// Found by structure rather than by id: a user may have rebuilt the graph, and
// the graph view itself identifies the root this way.
//
// A device or a thing is never the root: the model refuses either as the end
// node, so a graph with one at the top is not one the repository would store.
// Skipping them matters for a projection, which can be read in the middle of
// an edit - a thing node whose parent edge has not started yet would otherwise
// be taken for the top.
func RootOf(graph platform.Graph) (platform.Node, bool) {
	for _, node := range graph.Nodes {
		if node.ResourceType == platform.GraphResourceTypeDevice || node.ResourceType == model.ResourceTypeThing {
			continue
		}
		isSource := true
		for _, edge := range graph.Edges {
			if edge.FromNodeId == node.Id {
				isSource = false
				break
			}
		}
		if isSource {
			return node, true
		}
	}
	return platform.Node{}, false
}

// DeviceIdsIn is every device a graph contains as a device node, by the id it
// points at. Devices a graph reaches through meter things are not in it, they
// need the things: see Store.Placed.
func DeviceIdsIn(graph platform.Graph) []string {
	result := []string{}
	for _, node := range graph.Nodes {
		if node.ResourceType == platform.GraphResourceTypeDevice && node.ResourceId != "" {
			result = append(result, node.ResourceId)
		}
	}
	slices.Sort(result)
	return result
}

// ThingIdsIn is every thing a graph contains, by the id it points at.
func ThingIdsIn(graph platform.Graph) []string {
	result := []string{}
	for _, node := range graph.Nodes {
		if node.ResourceType == model.ResourceTypeThing && node.ResourceId != "" {
			result = append(result, node.ResourceId)
		}
	}
	slices.Sort(result)
	return result
}

// Placed is where the devices of a graph are, however they got there.
type Placed struct {
	// DeviceIds are the devices the graph accounts for: those with a device
	// node, and those linked through an open link to a thing with a node in the
	// graph. Sorted.
	DeviceIds []string

	// Things are the things the graph's thing nodes point at, by id. A thing
	// node whose thing the repository does not return is absent.
	Things map[string]things.Thing

	// ThingDevices maps a thing in the graph to the device that measures it
	// through an open link. A thing measured by several devices at once maps to
	// the lowest id, which is enough to read a figure from.
	ThingDevices map[string]string
}

// Placed reads the things of a graph and answers which devices it accounts for.
//
// The graph read is the projection at now, so a thing node that ended is not in
// it, and a link that ended is not counted: only a link that is open now or
// still ahead keeps a device placed. A device with a link that ended and no
// other place is missing and is placed again, as a new meter.
//
// Every kind of thing counts, not only meters. The repository refuses a graph
// holding a thing together with a device linked to it at the same time, so a
// device linked to a consumer thing of the graph could not be added as a device
// node either, and trying would fail every pass.
//
// A graph without thing nodes costs no request.
func (this *Store) Placed(ctx context.Context, graph platform.Graph, now time.Time) (Placed, error) {
	result := Placed{
		DeviceIds:    DeviceIdsIn(graph),
		Things:       map[string]things.Thing{},
		ThingDevices: map[string]string{},
	}
	thingIds := ThingIdsIn(graph)
	if len(thingIds) == 0 {
		return result, nil
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	found, err, code := this.things.List(ctx, this.token, thingIds, nil)
	if err != nil {
		return result, fmt.Errorf("unable to read the things of graph %v (%v): %w", graph.Id, code, err)
	}
	seen := map[string]bool{}
	for _, id := range result.DeviceIds {
		seen[id] = true
	}
	for _, thing := range found {
		result.Things[thing.Id] = thing
		for _, link := range thing.DeviceLinks {
			if !link.Open(now) {
				continue
			}
			if !seen[link.DeviceId] {
				seen[link.DeviceId] = true
				result.DeviceIds = append(result.DeviceIds, link.DeviceId)
			}
			if previous, taken := result.ThingDevices[thing.Id]; !taken || link.DeviceId < previous {
				result.ThingDevices[thing.Id] = link.DeviceId
			}
		}
	}
	slices.Sort(result.DeviceIds)
	return result, nil
}

// Meters is how a pass placed the devices it is about to add.
type Meters struct {
	// ThingIds maps a device to the meter thing it is placed as. Adopted,
	// created, or - while writing is off - a placeholder.
	ThingIds map[string]string

	// Created are the ids of the things this call created.
	Created []string

	// Plain are the devices that cannot become a meter: they are already
	// linked to a thing this service did not make for this group, and a second
	// open link for one device is refused. They are placed as device nodes.
	Plain []string
}

// PlaceholderThingId is the id a planned meter carries in a dry run, where no
// thing is created. Recognisable on purpose: nothing in a report should be
// mistaken for an id the repository handed out.
func PlaceholderThingId(deviceId string) string {
	return "planned-meter-of-" + deviceId
}

// PlaceMeters gives each device a meter thing, creating what is missing.
//
// A device with an open link to a thing this service made for this group - a
// meter thing, owned by the service user, carrying the group's id - is placed
// as that thing: it is what an earlier pass left behind when it created the
// thing and the link and then failed before the graph was written, and a
// second thing for the same device would be refused anyway, since the periods
// of one device may not overlap across things. A thing found this way is
// shared again, because the earlier pass may have stopped before sharing it.
//
// A device with an open link to any other thing is not touched; it is
// reported in Plain.
//
// Otherwise a meter thing is created, named like the device, owned like the
// graph by the service user and marked with the group's id, and the device is
// linked to it. The link is created with no start, which the repository reads
// as now, no readings - they are not known when a device is first seen - and
// no factor, which the repository reads as 1. A thing whose link cannot be
// created is deleted again, so a failure does not leave a thing nobody links.
//
// With writing off nothing is created and nothing is shared: the devices get a
// placeholder id, so a dry run can still show the graph it would write.
func (this *Store) PlaceMeters(ctx context.Context, group model.Group, devices []model.Device, now time.Time) (Meters, error) {
	result := Meters{ThingIds: map[string]string{}}
	if len(devices) == 0 {
		return result, nil
	}
	sorted := slices.Clone(devices)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Id < sorted[j].Id })
	deviceIds := make([]string, len(sorted))
	for i, device := range sorted {
		deviceIds[i] = device.Id
	}

	if err := ctx.Err(); err != nil {
		return result, err
	}
	linked, err, code := this.things.List(ctx, this.token, nil, deviceIds)
	if err != nil {
		return result, fmt.Errorf("unable to read the things of the new devices (%v): %w", code, err)
	}
	// Per device: the things it has an open link to, in id order.
	openLinks := map[string][]things.Thing{}
	for _, thing := range linked {
		for _, link := range thing.DeviceLinks {
			if link.Open(now) {
				openLinks[link.DeviceId] = append(openLinks[link.DeviceId], thing)
			}
		}
	}

	for _, device := range sorted {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		existing := openLinks[device.Id]
		if len(existing) > 0 {
			if adopt, ok := this.ownMeter(existing, group); ok {
				result.ThingIds[device.Id] = adopt.Id
				if this.writable {
					if _, err := this.ShareThing(ctx, adopt.Id, group.Path); err != nil {
						return result, fmt.Errorf("unable to share meter %v of device %v: %w", adopt.Id, device.Id, err)
					}
				}
				continue
			}
			result.Plain = append(result.Plain, device.Id)
			continue
		}
		if !this.writable {
			result.ThingIds[device.Id] = PlaceholderThingId(device.Id)
			continue
		}
		thingId, err := this.createMeter(ctx, group, device)
		if thingId != "" {
			result.Created = append(result.Created, thingId)
			result.ThingIds[device.Id] = thingId
		}
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

// IsOwnMeter reports whether a thing is a meter this service made for the group:
// kind meter, owned by the service user, carrying the group's id. Only such
// things are ever changed by a pass; a link on any other thing is a person's.
func (this *Store) IsOwnMeter(thing things.Thing, group model.Group) bool {
	return thing.Kind == things.KindMeter &&
		thing.OwnerId == this.serviceUserId &&
		AttributeOf(thing.Attributes, model.AttrKeycloakGroupId) == group.Id
}

// EndLink ends a device's open link on a meter at an instant. The thing, its
// node and everything measured before stay. Callers check IsOwnMeter first.
func (this *Store) EndLink(ctx context.Context, thingId string, linkId string, at time.Time) error {
	if !this.writable {
		return ErrReadOnly
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err, code := this.things.EndDeviceLink(ctx, this.token, thingId, linkId, at); err != nil {
		return fmt.Errorf("unable to end link %v of meter %v (%v): %w", linkId, thingId, code, err)
	}
	return nil
}

// ownMeter picks the meter thing this service made for the group from a
// device's open links.
func (this *Store) ownMeter(candidates []things.Thing, group model.Group) (things.Thing, bool) {
	for _, thing := range candidates {
		if this.IsOwnMeter(thing, group) {
			return thing, true
		}
	}
	return things.Thing{}, false
}

// createMeter creates one meter thing, links the device to it and shares it.
// The id is returned as soon as the thing exists, even with an error after it,
// because the next pass finds the thing by its link and adopts it.
func (this *Store) createMeter(ctx context.Context, group model.Group, device model.Device) (string, error) {
	name := device.Name
	if name == "" {
		name = device.Id
	}
	thing, err, code := this.things.Create(ctx, this.token, things.Thing{
		Kind: things.KindMeter,
		Name: name,
		// Owned like the graph. permissions-v2 gives an owner full rights and
		// needs at least one administrating user, which this is.
		OwnerId: this.serviceUserId,
		Attributes: []platform.Attribute{
			{Key: model.AttrKeycloakGroupId, Value: group.Id, Origin: model.AttrOrigin},
		},
	})
	if err != nil {
		return "", fmt.Errorf("unable to create a meter for device %v (%v): %w", device.Id, code, err)
	}
	if _, err, code = this.things.AddDeviceLink(ctx, this.token, thing.Id, things.LinkCreate{DeviceId: device.Id}); err != nil {
		linkErr := fmt.Errorf("unable to link device %v to its meter %v (%v): %w", device.Id, thing.Id, code, err)
		// Best effort. A thing without a link is of no use to anybody; if the
		// delete fails too it stays, and a later pass cannot find it by a link
		// - which is the lesser harm against a delete retried until it works.
		if delErr, _ := this.things.Delete(ctx, this.token, thing.Id); delErr != nil {
			return "", errors.Join(linkErr, fmt.Errorf("the meter %v could not be removed again: %w", thing.Id, delErr))
		}
		return "", linkErr
	}
	if _, err = this.ShareThing(ctx, thing.Id, group.Path); err != nil {
		return thing.Id, fmt.Errorf("unable to share meter %v of device %v: %w", thing.Id, device.Id, err)
	}
	return thing.Id, nil
}

// NodeIdentityKept checks that no node of before changed what it stands for in
// after. The repository answers a changed resource_type or resource_id of an
// existing node id with 400: a different resource is a different node. Nothing
// here edits a node in place, so this is the guard that keeps it so.
func NodeIdentityKept(before platform.Graph, after platform.Graph) error {
	now := map[string]platform.Node{}
	for _, node := range after.Nodes {
		now[node.Id] = node
	}
	for _, node := range before.Nodes {
		if kept, ok := now[node.Id]; ok && (kept.ResourceType != node.ResourceType || kept.ResourceId != node.ResourceId) {
			return fmt.Errorf("node %v would change from %v %v to %v %v", node.Id,
				node.ResourceType, node.ResourceId, kept.ResourceType, kept.ResourceId)
		}
	}
	return nil
}
