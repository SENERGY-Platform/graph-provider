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

// Package reconcile is the pass that brings a group's graph in line with the
// world.
//
// Every trigger - a Kafka message, the group poll, the safety-net ticker, a
// request over HTTP - ends here, and the pass itself is the same code in every
// case. That is what lets a backlog after a restart and a live event behave
// alike, and it is why the pass has to be idempotent: running it twice must
// write once.
//
// It runs in one goroutine. Reconciliation is the only writer to a graph, and
// with no database there is nowhere to hold a lease, so serial is not a
// simplification but the correctness argument.
package reconcile

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/SENERGY-Platform/graph-provider/pkg/cache"
	"github.com/SENERGY-Platform/graph-provider/pkg/events"
	"github.com/SENERGY-Platform/graph-provider/pkg/graphs"
	"github.com/SENERGY-Platform/graph-provider/pkg/model"
	"github.com/SENERGY-Platform/graph-provider/pkg/structure"
	platform "github.com/SENERGY-Platform/models/go/models"
)

// Readings is the consumption source, as far as this package is concerned.
type Readings interface {
	Fetch(ctx context.Context, devices []model.Device, window model.Window) (map[model.ReadingKey]model.Reading, error)
}

type Config struct {
	ServiceUserId     string
	Window            time.Duration
	Tolerance         float64
	NoFlowNodeName    string
	ReadingTtl        time.Duration
	GroupPollInterval time.Duration
	ReconcileInterval time.Duration

	// Planned, when set, is called with every change a pass would make,
	// whether or not writing is enabled. Nil in normal operation.
	//
	// Called from the goroutine that runs the pass, before the write for a
	// graph and after the store's answer for a sharing change - see plan and
	// permission. It must not write to the systems the pass reads, and what it
	// does with a change cannot influence the pass.
	Planned func(model.PlannedChange)
}

type Reconciler struct {
	cache       *cache.Cache
	store       *graphs.Store
	consumption Readings
	pending     *events.Pending
	config      Config
	logger      *slog.Logger

	now func() time.Time

	// requested holds the group paths a pass has been asked for, and whether
	// somebody asked for all of them. Kept apart from events.Pending because
	// these are wishes about groups, not about resource ids.
	mux       sync.Mutex
	requested map[string]bool
	all       bool
	wake      chan struct{}
}

func New(c *cache.Cache, store *graphs.Store, consumption Readings, pending *events.Pending, cfg Config, logger *slog.Logger) *Reconciler {
	return &Reconciler{
		cache:       c,
		store:       store,
		consumption: consumption,
		pending:     pending,
		config:      cfg,
		logger:      logger,
		now:         time.Now,
		requested:   map[string]bool{},
		wake:        make(chan struct{}, 1),
	}
}

// RequestAll asks for a full pass. It does not wait for one.
func (this *Reconciler) RequestAll() {
	this.mux.Lock()
	this.all = true
	this.mux.Unlock()
	this.signal()
}

// RequestGroup asks for a pass over one group. It does not wait for one.
func (this *Reconciler) RequestGroup(path string) {
	this.mux.Lock()
	this.requested[path] = true
	this.mux.Unlock()
	this.signal()
}

func (this *Reconciler) signal() {
	select {
	case this.wake <- struct{}{}:
	default:
		// A wakeup is already queued and the loop will see the request when
		// it next looks, so a second one would only be dropped.
	}
}

// Pending is how much work is queued, for the info endpoint.
func (this *Reconciler) Pending() int {
	this.mux.Lock()
	defer this.mux.Unlock()
	count := len(this.requested)
	if this.all {
		count++
	}
	return count + this.pending.Len()
}

// Run drives the service until ctx is done.
//
// A full pass first: whatever happened while the service was down is covered
// by it, which is what allows the Kafka consumers to be a pure accelerator and
// no single message to be load-bearing.
func (this *Reconciler) Run(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		if err := this.Full(ctx); err != nil && !errors.Is(err, context.Canceled) {
			this.logger.Error("initial reconciliation failed", "error", err)
		}

		// One pump, started once. A Take per loop iteration would leave a
		// goroutine blocked in Take on every pass through another branch, and
		// whichever of them later won a trigger would push it into a channel
		// nobody selects on any more - a silently dropped trigger.
		triggers := make(chan []events.Trigger)
		go func() {
			defer close(triggers)
			for {
				batch := this.pending.Take(ctx)
				if len(batch) == 0 {
					return // ctx is done
				}
				select {
				case triggers <- batch:
				case <-ctx.Done():
					return
				}
			}
		}()

		groupPoll := time.NewTicker(this.config.GroupPollInterval)
		defer groupPoll.Stop()
		safetyNet := time.NewTicker(this.config.ReconcileInterval)
		defer safetyNet.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-safetyNet.C:
				if err := this.Full(ctx); err != nil && !errors.Is(err, context.Canceled) {
					this.logger.Error("scheduled reconciliation failed", "error", err)
				}
			case <-groupPoll.C:
				if err := this.pollGroups(ctx); err != nil && !errors.Is(err, context.Canceled) {
					this.logger.Error("group poll failed", "error", err)
				}
			case <-this.wake:
				this.drain(ctx)
			case batch, ok := <-triggers:
				if !ok {
					return
				}
				this.resolve(ctx, batch)
				this.drain(ctx)
			}
		}
	}()
}

// resolve turns Kafka triggers into group work.
//
// A trigger carries an id and nothing else. What that id now looks like is
// asked of the authoritative source here, because a stream that keeps a few
// days cannot be trusted to have told the whole story.
func (this *Reconciler) resolve(ctx context.Context, triggers []events.Trigger) {
	for _, trigger := range triggers {
		if ctx.Err() != nil {
			return
		}
		switch trigger.Kind {
		case events.KindDevice:
			change, err := this.cache.RefreshDevice(ctx, trigger.Id)
			if err != nil {
				this.logger.Warn("unable to refresh device", "device", trigger.Id, "error", err)
				continue
			}
			for _, path := range change.AffectedGroups() {
				this.RequestGroup(path)
			}
		case events.KindDeviceType:
			// The trigger names a type, not the devices. Every device of that
			// type may have gained or lost a column, so each is re-read.
			for _, deviceId := range this.cache.DevicesOfDeviceType(trigger.Id) {
				change, err := this.cache.RefreshDevice(ctx, deviceId)
				if err != nil {
					this.logger.Warn("unable to refresh device", "device", deviceId, "error", err)
					continue
				}
				for _, path := range change.AffectedGroups() {
					this.RequestGroup(path)
				}
			}
		case events.KindGraph:
			// A graph changed. Which group it belongs to is written on the
			// graph itself, so a lookup answers it without any local state.
			graph, found, err := this.store.Get(ctx, trigger.Id)
			if err != nil {
				this.logger.Warn("unable to read graph", "graph", trigger.Id, "error", err)
				continue
			}
			if !found {
				// Deleted. Which group it belonged to was written on the graph,
				// so with the graph gone the id says nothing - and there is no
				// local record to look it up in, by design.
				//
				// A full pass is the answer: it finds the group that now has no
				// default graph and creates one. Graphs are deleted rarely
				// enough that paying for a pass is cheaper than waiting out the
				// safety-net interval with a company that has no graph.
				this.logger.Info("a graph was deleted, reconciling everything to find out whose", "graph", trigger.Id)
				this.RequestAll()
				continue
			}
			if path := graphs.GroupPathOf(graph); path != "" {
				this.RequestGroup(path)
			}
		}
	}
}

// drain works off everything that has been asked for.
func (this *Reconciler) drain(ctx context.Context) {
	for {
		this.mux.Lock()
		all := this.all
		paths := make([]string, 0, len(this.requested))
		for path := range this.requested {
			paths = append(paths, path)
		}
		this.all = false
		this.requested = map[string]bool{}
		this.mux.Unlock()

		if all {
			if err := this.Full(ctx); err != nil && !errors.Is(err, context.Canceled) {
				this.logger.Error("requested reconciliation failed", "error", err)
			}
			continue
		}
		if len(paths) == 0 {
			return
		}
		slices.Sort(paths)
		defaults, err := this.store.Defaults(ctx)
		if err != nil {
			this.logger.Error("unable to list default graphs", "error", err)
			return
		}
		for _, path := range paths {
			if ctx.Err() != nil {
				return
			}
			if err := this.safeGroup(ctx, path, existingFor(defaults, this.cache, path)); err != nil && !errors.Is(err, context.Canceled) {
				this.logger.Error("unable to reconcile group", "group", path, "error", err)
			}
		}
	}
}

// Full re-reads everything and reconciles every group.
func (this *Reconciler) Full(ctx context.Context) error {
	if _, _, err := this.cache.RefreshGroups(ctx); err != nil {
		return fmt.Errorf("unable to read groups: %w", err)
	}
	if err := this.cache.RefreshDevices(ctx); err != nil {
		return fmt.Errorf("unable to read devices: %w", err)
	}
	defaults, err := this.store.Defaults(ctx)
	if err != nil {
		return err
	}

	groups := this.cache.Groups()
	known := map[string]bool{}
	var errs []error
	for _, group := range groups {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		known[group.Path] = true
		if err := this.safeGroup(ctx, group.Path, existing(defaults, group)); err != nil {
			errs = append(errs, fmt.Errorf("group %v: %w", group.Path, err))
		}
	}

	// An empty group list is not the same as every group having disappeared.
	// A directory that answers successfully with nothing - a misconfigured
	// realm, a filter that matches nothing, a client that lost its roles -
	// would otherwise cost every company its access in a single pass. Nothing
	// legitimate needs that, so no group at all means no orphaning.
	if len(groups) == 0 {
		// Always said, not only when graphs already exist. A realm with no
		// groups at all is rare; a service account that cannot see them is
		// common, and Keycloak does not distinguish the two - with
		// query-groups but not view-groups it answers the listing with an
		// empty array and HTTP 200. Silence here reads as "nothing to do".
		this.logger.Warn("no groups visible - if that is unexpected, the service"+
			" account is probably missing view-groups from the client that"+
			" administers this realm; Keycloak answers an empty list rather than"+
			" a 403 when only query-groups is granted",
			"existing graphs", len(defaults.ById)+len(defaults.ByPath))
		return errors.Join(errs...)
	}

	// A graph whose group is gone. Its sharing is withdrawn and its group
	// attributes cleared; the graph itself stays. Deleting a user's work in
	// response to a directory event is the harsher of the two actions, and
	// the graph remains usable to its owner.
	//
	// Keyed by group id, so a renamed group is not mistaken for a vanished
	// one - that mistake would cost the user everything they had built.
	knownIds := map[string]bool{}
	for _, group := range groups {
		knownIds[group.Id] = true
	}
	for groupId, graph := range defaults.ById {
		if knownIds[groupId] || ctx.Err() != nil {
			continue
		}
		if err := this.orphan(ctx, GroupPathOfGraph(graph), graph); err != nil {
			errs = append(errs, fmt.Errorf("orphaned graph %v: %w", graph.Id, err))
		}
	}
	for path, graph := range defaults.ByPath {
		if known[path] || ctx.Err() != nil {
			continue
		}
		if err := this.orphan(ctx, path, graph); err != nil {
			errs = append(errs, fmt.Errorf("orphaned graph %v: %w", graph.Id, err))
		}
	}
	return errors.Join(errs...)
}

// pollGroups re-reads the group tree and asks for a pass over what moved.
func (this *Reconciler) pollGroups(ctx context.Context) error {
	added, removed, err := this.cache.RefreshGroups(ctx)
	if err != nil {
		return err
	}
	for _, group := range append(added, removed...) {
		this.RequestGroup(group.Path)
	}
	if len(added) > 0 || len(removed) > 0 {
		this.logger.Info("group tree changed", "added", len(added), "removed", len(removed))
		this.signal()
	}
	return nil
}

// existing finds a group's default graph: by the stable group id, and failing
// that by path, which adopts a graph written before the id was recorded.
func existing(defaults graphs.Defaults, group model.Group) platform.Graph {
	if graph, found := defaults.ById[group.Id]; found {
		return graph
	}
	return defaults.ByPath[group.Path]
}

// existingFor is existing for a caller that has only a path.
func existingFor(defaults graphs.Defaults, c *cache.Cache, path string) platform.Graph {
	if group, known := c.Group(path); known {
		return existing(defaults, group)
	}
	return defaults.ByPath[path]
}

// GroupPathOfGraph is the path a graph is currently shared with.
func GroupPathOfGraph(graph platform.Graph) string {
	return graphs.GroupPathOf(graph)
}

// safeGroup runs one group's pass and contains a panic to that group.
//
// The graph model mutates a graph in place while rerouting edges and has been
// seen to index out of range on a graph a user had edited. A panic there would
// otherwise travel up this goroutine and end the process - and the startup
// sweep would meet the same graph again, so the service would not come back.
// One unreconcilable group must not cost every other group its updates.
func (this *Reconciler) safeGroup(ctx context.Context, path string, existing platform.Graph) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("panic while reconciling group %v (graph %v): %v",
				path, existing.Id, recovered)
			this.logger.Error("recovered from a panic while reconciling a group",
				"group", path, "graph", existing.Id, "panic", recovered,
				"stack", string(debug.Stack()))
		}
	}()
	return this.group(ctx, path, existing)
}

// group brings one group's default graph in line.
func (this *Reconciler) group(ctx context.Context, path string, existing platform.Graph) error {
	group, known := this.cache.Group(path)
	if !known {
		// The group is gone. Full handles the orphan; a single-group pass has
		// nothing to reconcile against.
		return nil
	}
	devices := this.cache.DevicesOfGroup(path)

	if existing.Id == "" {
		return this.create(ctx, group, devices)
	}
	return this.update(ctx, group, devices, existing)
}

// create generates a group's first graph.
//
// This is the only time the full heuristic runs. Afterwards the graph may
// carry a user's decisions, and re-deriving a structure over them would undo
// work silently.
func (this *Reconciler) create(ctx context.Context, group model.Group, devices []model.Device) error {
	readings, err := this.readings(ctx, devices)
	if err != nil {
		return err
	}
	graph, placements, err := structure.Build(this.config.ServiceUserId, group, devices, readings, structure.Options{
		Tolerance:      this.config.Tolerance,
		NoFlowNodeName: this.config.NoFlowNodeName,
	})
	if err != nil {
		return fmt.Errorf("unable to build graph: %w", err)
	}
	unplaced := withoutReading(devices, readings)
	this.plan(model.PlannedChange{
		Action:     model.ActionCreate,
		Group:      group,
		Graph:      graph,
		Devices:    devices,
		NoReading:  unplaced,
		Readings:   readings,
		Placements: placements,
		Note:       fmt.Sprintf("%v, %v of them without a reading", plural(len(devices), "device"), len(unplaced)),
	})

	saved, err := this.store.Save(ctx, graph)
	if err != nil {
		// A kill switch somebody configured is an expected condition, not a
		// failure: reported at info, and the pass carries on over the remaining
		// groups. Sharing is not attempted - the graph has no id and no
		// permission resource, so there is nothing to share.
		if errors.Is(err, graphs.ErrReadOnly) {
			this.logger.Info("writing is disabled, default graph not created",
				"group", group.Path, "devices", len(devices))
			return nil
		}
		return err
	}
	this.logger.Info("created default graph", "group", group.Path, "graph", saved.Id, "devices", len(devices))

	// Sharing comes after the write, not with it: the graph has no permission
	// resource until the repository has created one for it.
	if err = this.permission(model.ActionShare, group, saved, group.Path, func() (bool, error) {
		return this.store.Share(ctx, saved.Id, group.Path)
	}); err != nil {
		return fmt.Errorf("unable to share new graph %v: %w", saved.Id, err)
	}
	return nil
}

// update adds what is missing and removes what left. It never restructures.
func (this *Reconciler) update(ctx context.Context, group model.Group, devices []model.Device, existing platform.Graph) error {
	next := existing
	changed := false
	removedCount := 0
	notes := []string{}
	unplaced := []string{}

	// A path change means the group was renamed, or a parent above it was.
	// Read before anything else, because it decides whether a device that is
	// no longer in the group counts as having left.
	previousPath := graphs.GroupPathOf(existing)
	moved := previousPath != "" && previousPath != group.Path

	inGraph := map[string]bool{}
	for _, id := range graphs.DeviceIdsIn(existing) {
		inGraph[id] = true
	}
	wanted := map[string]bool{}
	for _, device := range devices {
		wanted[device.Id] = true
	}

	// Gone first: removing frees capacity that an arrival might use, and the
	// model reroutes the edges of a removed node itself.
	// Sorted, not in map order. Removing a node reroutes its children, so with
	// several departures the resulting graph depends on the order they are
	// removed in - and a graph that differs between two runs over the same
	// input is one nobody can reason about.
	departed := []string{}
	if moved {
		// Not this pass. Keycloak rewrites a group's path on a rename, but
		// permissions-v2 keys a device's group rights by the old path and is
		// not told - so straight after a rename every device of the company
		// looks like one that left. Emptying the graph on that reading would
		// destroy the structure a user built, and it would be wrong: the
		// devices are where they were, only the name of the group changed.
		// Real departures are picked up by the next pass.
		this.logger.Info("group moved, leaving the graph's devices in place this pass",
			"graph", existing.Id, "from", previousPath, "to", group.Path)
	} else {
		for id := range inGraph {
			if !wanted[id] {
				departed = append(departed, id)
			}
		}
	}
	slices.Sort(departed)
	for _, id := range departed {
		removed, err := structure.Detach(&next, id)
		if err != nil {
			// A refusal is not a no-op: the device stays in the graph and the
			// same futile removal is attempted every pass. Reported at warn
			// rather than error because the pass is still useful and the
			// condition needs a person to edit the graph, not to be paged.
			this.logger.Warn("unable to remove a departed device from the default graph",
				"group", group.Path, "graph", existing.Id, "device", id, "error", err)
			continue
		}
		if removed {
			changed = true
			removedCount++
			this.logger.Info("removed device from default graph", "group", group.Path, "graph", existing.Id, "device", id)
		}
	}

	missing := []model.Device{}
	for _, device := range devices {
		if !inGraph[device.Id] {
			missing = append(missing, device)
		}
	}
	var readings map[model.ReadingKey]model.Reading
	var placements []model.Placement
	if len(missing) > 0 {
		// Readings for the whole group, not only the arrivals: placing a new
		// device needs the capacity of the meters already in the graph. This
		// also means the report can show a figure for devices that were
		// already in the graph, not only for the ones this pass attaches.
		var err error
		readings, err = this.readings(ctx, devices)
		if err != nil {
			return err
		}
		unplaced = withoutReading(missing, readings)
		for _, device := range missing {
			// TODO(structure-placements): structure.Attach is expected to
			// also return the []model.Placement it decided for this device,
			// once pkg/structure exposes the placement diagnostics the report
			// renderer needs (see pkg/plan). Written against that target
			// signature; will not compile until pkg/structure lands it.
			attached, err := structure.Attach(&next, device, readings, structure.Options{Tolerance: this.config.Tolerance, NoFlowNodeName: this.config.NoFlowNodeName})
			if err != nil {
				return fmt.Errorf("unable to attach device %v: %w", device.Id, err)
			}
			placements = append(placements, attached...)
			changed = true
			this.logger.Info("added device to default graph", "group", group.Path, "graph", existing.Id, "device", device.Id)
		}
	}

	if this.rename(&next, group) {
		changed = true
		notes = append(notes, "renamed to "+group.Name)
		this.logger.Info("renamed default graph", "group", group.Path, "graph", existing.Id, "name", group.Name)
	}

	// Adoption: a graph written before the id was recorded is claimed by
	// writing it, so the next pass finds it the cheap way.
	if graphs.GroupIdOf(next) != group.Id {
		next.Attributes = graphs.SetAttribute(next.Attributes, model.AttrKeycloakGroupId, group.Id)
		changed = true
		notes = append(notes, "group id recorded")
	}

	// The group has moved: the id still matches, so this is the same graph and
	// the same user's work - only the path it is shared under has changed.
	if previousPath != group.Path {
		next.Attributes = graphs.SetAttribute(next.Attributes, model.AttrKeycloakGroup, group.Path)
		changed = true
		notes = append(notes, "group path now "+group.Path)
	}

	if changed {
		if removedCount > 0 {
			notes = append([]string{plural(removedCount, "device") + " removed"}, notes...)
		}
		if len(missing) > 0 {
			notes = append([]string{plural(len(missing), "device") + " added"}, notes...)
		}
		this.plan(model.PlannedChange{
			Action:     model.ActionUpdate,
			Group:      group,
			Graph:      next,
			Devices:    devices,
			NoReading:  unplaced,
			Readings:   readings,
			Placements: placements,
			Note:       strings.Join(notes, ", "),
		})
		if _, err := this.store.Save(ctx, next); err != nil {
			// See create: a configured kill switch does not fail the pass. The
			// sharing below is still checked, because it is a separate write and
			// a reader of the report wants to know about both.
			if !errors.Is(err, graphs.ErrReadOnly) {
				return err
			}
			this.logger.Info("writing is disabled, default graph not updated",
				"group", group.Path, "graph", existing.Id, "change", strings.Join(notes, ", "))
		}
	}

	// Sharing is checked every pass whether or not the graph changed: a user
	// or another service may have taken the group's access away.
	if err := this.permission(model.ActionShare, group, existing, group.Path, func() (bool, error) {
		return this.store.Share(ctx, existing.Id, group.Path)
	}); err != nil {
		return err
	}

	// The old path last, and only once the new one holds. Withdrawing first
	// would leave the company with no access to its own graph if the second
	// call failed.
	if moved {
		if err := this.permission(model.ActionUnshare, group, existing, previousPath, func() (bool, error) {
			return this.store.Unshare(ctx, existing.Id, previousPath)
		}); err != nil {
			return err
		}
		this.logger.Info("group moved, sharing followed",
			"graph", existing.Id, "from", previousPath, "to", group.Path)
	}
	return nil
}

// rename follows a group rename, but only into a name this service wrote.
//
// The comparison is against the name recorded on the graph rather than against
// anything held locally: provenance travels with the object, which is what
// lets this service keep no database and still tell its own work from a user's.
// A user's name stands; the record is still brought up to date, so the next
// rename does not argue with it again.
func (this *Reconciler) rename(graph *platform.Graph, group model.Group) bool {
	root, found := graphs.RootOf(*graph)
	if !found {
		return false
	}
	generated := graphs.GeneratedNameOf(*graph)
	current := graphs.AttributeOf(root.Attributes, model.NodeAttrName)

	if generated == group.Name && current == group.Name {
		return false
	}

	changed := false
	if current == generated {
		for i, node := range graph.Nodes {
			if node.Id != root.Id {
				continue
			}
			graph.Nodes[i].Attributes = graphs.SetAttribute(node.Attributes, model.NodeAttrName, group.Name)
			changed = true
			break
		}
	}
	if generated != group.Name {
		graph.Attributes = graphs.SetAttribute(graph.Attributes, model.AttrName, group.Name)
		changed = true
	}
	return changed
}

// orphan withdraws a vanished group's access and forgets the group.
func (this *Reconciler) orphan(ctx context.Context, path string, graph platform.Graph) error {
	// The group is gone, so there is no model.Group to name: the path is all
	// that is left of it, and it is what the withdrawal is written against.
	group := model.Group{Path: path, Name: model.GroupName(path)}
	if err := this.permission(model.ActionUnshare, group, graph, path, func() (bool, error) {
		return this.store.Unshare(ctx, graph.Id, path)
	}); err != nil {
		return err
	}
	next := graph
	next.Attributes = slices.DeleteFunc(slices.Clone(graph.Attributes), func(attribute platform.Attribute) bool {
		return attribute.Key == model.AttrKeycloakGroup || attribute.Key == model.AttrKeycloakGroupId
	})
	if reflect.DeepEqual(next.Attributes, graph.Attributes) {
		return nil
	}
	this.plan(model.PlannedChange{
		Action: model.ActionDetachGroup,
		Group:  group,
		Graph:  next,
		Note:   "group attributes cleared, the graph itself stays",
	})
	if _, err := this.store.Save(ctx, next); err != nil {
		// Same as create and update: the kill switch is what somebody asked
		// for, so it does not fail the pass.
		if !errors.Is(err, graphs.ErrReadOnly) {
			return err
		}
		this.logger.Info("writing is disabled, graph not detached", "group", path, "graph", graph.Id)
		return nil
	}
	this.logger.Info("group is gone, graph detached", "group", path, "graph", graph.Id)
	return nil
}

// readings serves what is cached and fetches only the rest.
func (this *Reconciler) readings(ctx context.Context, devices []model.Device) (map[model.ReadingKey]model.Reading, error) {
	// The window is recomputed per pass but has to be identical for every
	// device in one structure: figures read over different stretches are not
	// comparable, and comparing them is the whole of the heuristic.
	end := this.now()
	window := model.Window{Start: end.Add(-this.config.Window), End: end}

	cached := this.cache.Readings(devices, window, this.config.ReadingTtl)
	missing := []model.Device{}
	for _, device := range devices {
		for _, carrier := range device.CarriersOf() {
			if _, known := cached[model.ReadingKey{DeviceId: device.Id, Carrier: carrier}]; !known {
				missing = append(missing, device)
				break
			}
		}
	}
	if len(missing) == 0 {
		return cached, nil
	}

	fetched, err := this.consumption.Fetch(ctx, missing, window)
	if err != nil {
		return nil, fmt.Errorf("unable to read consumption: %w", err)
	}
	this.cache.PutReadings(fetched)
	for key, reading := range fetched {
		cached[key] = reading
	}
	return cached, nil
}

// plan reports one intended change, if anybody is listening.
func (this *Reconciler) plan(change model.PlannedChange) {
	if this.config.Planned == nil {
		return
	}
	this.config.Planned(change)
}

// permission performs one sharing write and reports it as a planned change.
//
// Reported on the store's answer rather than before the call, which is the one
// place this differs from a graph write. Whether a group's rights already agree
// is known only to permissions-v2, and the store asks it on every pass whether
// or not the graph changed - so a report written before the call would name
// every group's sharing every time and say nothing. ErrReadOnly is the
// interesting case: the store raises it only once it has established that the
// rights do not agree, so it means "this write was needed and skipped".
func (this *Reconciler) permission(action string, group model.Group, graph platform.Graph, path string, write func() (bool, error)) error {
	changed, err := write()
	switch {
	case errors.Is(err, graphs.ErrReadOnly):
		// A configured kill switch is not a failure. Logged at info and swallowed
		// so the pass carries on over the remaining groups; an error here would
		// page somebody for doing what they configured.
		this.plan(model.PlannedChange{Action: action, Group: group, Graph: graph, Note: "group " + path})
		this.logger.Info("writing is disabled, sharing not changed",
			"action", action, "group", group.Path, "graph", graph.Id, "path", path)
		return nil
	case err != nil:
		return err
	case changed:
		this.plan(model.PlannedChange{Action: action, Group: group, Graph: graph, Note: "group " + path})
	}
	return nil
}

// withoutReading is the devices a pass has no usable consumption figure for.
//
// The same rule the heuristic places by, so the report explains the tree it
// describes rather than a different one: a device with no carrier at all reads
// nothing, and a figure of zero or less - a meter that did not move, one that
// was exchanged - says nothing about containment and is not a reading either.
func withoutReading(devices []model.Device, readings map[model.ReadingKey]model.Reading) []string {
	result := []string{}
	for _, device := range devices {
		best := 0.0
		for _, carrier := range device.CarriersOf() {
			if reading, found := readings[model.ReadingKey{DeviceId: device.Id, Carrier: carrier}]; found && reading.Value > best {
				best = reading.Value
			}
		}
		if best <= 0 {
			result = append(result, device.Id)
		}
	}
	slices.Sort(result)
	return result
}

// plural counts a noun for a line somebody reads.
//
// "1 devices added" in a report meant for people is a distraction from what
// the report says, and the report is the whole point of the hook.
func plural(count int, noun string) string {
	if count == 1 {
		return "1 " + noun
	}
	return fmt.Sprintf("%v %vs", count, noun)
}
