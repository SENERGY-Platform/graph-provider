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

// Package cache holds the service's view of devices, groups and readings.
//
// Everything in here is derived and disposable: the device repository,
// permissions-v2 and Keycloak are the truth, and a restart rebuilds the lot.
// That is the whole reason this service has no database - a second copy of
// state that is already authoritative elsewhere would buy a migration and a
// backup story for data whose loss costs one refresh.
//
// Nothing here is ever built from a Kafka message body. A trigger carries an
// id; this package answers it by asking the source again.
package cache

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	devicerepomodel "github.com/SENERGY-Platform/device-repository/lib/model"
	"github.com/SENERGY-Platform/graph-provider/pkg/carrier"
	"github.com/SENERGY-Platform/graph-provider/pkg/model"
	platform "github.com/SENERGY-Platform/models/go/models"
	permissions "github.com/SENERGY-Platform/permissions-v2/pkg/client"
)

// pageSize is how many devices are asked for per request during a full sweep.
// Large enough that a site of a few thousand meters is a handful of round
// trips, small enough that one response stays a reasonable size.
const pageSize = 500

// DeviceRepository is the part of the device repository client this package
// uses. An interface so tests need no server.
type DeviceRepository interface {
	ListExtendedDevices(token string, options devicerepomodel.ExtendedDeviceListOptions) (result []platform.ExtendedDevice, total int64, err error, errCode int)
}

// Permissions is the part of the permissions-v2 client this package uses.
type Permissions interface {
	ListResourcesWithAdminPermission(token string, topicId string, options permissions.ListOptions) (result []permissions.Resource, err error, code int)
	GetResource(token string, topicId string, id string) (result permissions.Resource, err error, code int)
}

// GroupSource is the Keycloak group tree.
type GroupSource interface {
	Groups(ctx context.Context) ([]model.Group, error)
}

// Cache is safe for concurrent use.
type Cache struct {
	devices     DeviceRepository
	permissions Permissions
	groups      GroupSource

	token       string
	deviceTopic string

	// allowGroup filters the realm's groups. Supplied by the caller so this
	// package does not depend on the configuration.
	allowGroup func(path string) bool

	now func() time.Time

	mux        sync.RWMutex
	deviceById map[string]model.Device
	groupByPat map[string]model.Group
	readings   map[model.ReadingKey]model.Reading
}

func New(devices DeviceRepository, perms Permissions, groups GroupSource, token string, deviceTopic string, allowGroup func(string) bool) *Cache {
	if allowGroup == nil {
		allowGroup = func(string) bool { return true }
	}
	return &Cache{
		devices:     devices,
		permissions: perms,
		groups:      groups,
		token:       token,
		deviceTopic: deviceTopic,
		allowGroup:  allowGroup,
		now:         time.Now,
		deviceById:  map[string]model.Device{},
		groupByPat:  map[string]model.Group{},
		readings:    map[model.ReadingKey]model.Reading{},
	}
}

// RefreshGroups replaces the group tree.
//
// Replaces rather than merges: a group that has disappeared from Keycloak has
// to disappear here too, and a merge would keep it alive forever.
func (this *Cache) RefreshGroups(ctx context.Context) (added []model.Group, removed []model.Group, err error) {
	found, err := this.groups.Groups(ctx)
	if err != nil {
		return nil, nil, err
	}

	next := map[string]model.Group{}
	for _, group := range found {
		if !this.allowGroup(group.Path) {
			continue
		}
		next[group.Path] = group
	}

	this.mux.Lock()
	defer this.mux.Unlock()
	for path, group := range next {
		previous, existed := this.groupByPat[path]
		if !existed || previous != group {
			added = append(added, group)
		}
	}
	for path, group := range this.groupByPat {
		if _, stillThere := next[path]; !stillThere {
			removed = append(removed, group)
		}
	}
	this.groupByPat = next

	sortGroups(added)
	sortGroups(removed)
	return added, removed, nil
}

// RefreshDevices rebuilds the whole device view: identity and carrier columns
// from the device repository, group membership from permissions-v2.
//
// Two sweeps rather than one call per device. The permission listing carries
// each device's group rights directly, so a site of two thousand meters costs
// a handful of pages instead of two thousand round trips.
func (this *Cache) RefreshDevices(ctx context.Context) error {
	groupsByDevice, err := this.listDeviceGroups(ctx)
	if err != nil {
		return fmt.Errorf("unable to list device permissions: %w", err)
	}

	next := map[string]model.Device{}
	for offset := int64(0); ; offset += pageSize {
		if err = ctx.Err(); err != nil {
			return err
		}
		page, _, err, _ := this.devices.ListExtendedDevices(this.token, devicerepomodel.ExtendedDeviceListOptions{
			Limit:  pageSize,
			Offset: offset,
			FullDt: true,
			SortBy: "id.asc",
		})
		if err != nil {
			return fmt.Errorf("unable to list devices: %w", err)
		}
		for _, extended := range page {
			device := toDevice(extended, groupsByDevice[extended.Id])
			next[device.Id] = device
		}
		if int64(len(page)) < pageSize {
			break
		}
	}

	this.mux.Lock()
	defer this.mux.Unlock()
	this.deviceById = next
	// Readings of devices that are gone are dead weight, and keeping them
	// would let a deleted-and-recreated id inherit a stale figure.
	for key := range this.readings {
		if _, known := next[key.DeviceId]; !known {
			delete(this.readings, key)
		}
	}
	return nil
}

// DeviceChange is what a single-device refresh found.
//
// PreviousGroups matters as much as the new state: a device that left a group
// changes two graphs, and the group it left is only knowable from what was
// held before.
type DeviceChange struct {
	Id             string
	Device         model.Device
	Exists         bool
	PreviousGroups []string
}

// AffectedGroups is every group path whose graph may need to change.
func (this DeviceChange) AffectedGroups() []string {
	seen := map[string]bool{}
	result := []string{}
	for _, path := range append(append([]string{}, this.PreviousGroups...), this.Device.Groups...) {
		if !seen[path] {
			seen[path] = true
			result = append(result, path)
		}
	}
	sort.Strings(result)
	return result
}

// RefreshDevice re-reads one device from the authoritative sources.
//
// This is what a Kafka trigger resolves to. The message said an id changed;
// what it changed to is asked here rather than read from the message, because a
// view assembled from a stream is a second opinion about state this service
// does not own - and the stale one is the one nobody notices.
func (this *Cache) RefreshDevice(ctx context.Context, id string) (DeviceChange, error) {
	change := DeviceChange{Id: id}

	this.mux.RLock()
	if previous, known := this.deviceById[id]; known {
		change.PreviousGroups = append([]string{}, previous.Groups...)
	}
	this.mux.RUnlock()

	page, _, err, _ := this.devices.ListExtendedDevices(this.token, devicerepomodel.ExtendedDeviceListOptions{
		Ids:    []string{id},
		FullDt: true,
	})
	if err != nil {
		return change, fmt.Errorf("unable to read device %v: %w", id, err)
	}
	if len(page) == 0 {
		// Gone, or no longer visible. Either way it belongs in no graph.
		this.mux.Lock()
		delete(this.deviceById, id)
		for _, carrierName := range model.Carriers {
			delete(this.readings, model.ReadingKey{DeviceId: id, Carrier: carrierName})
		}
		this.mux.Unlock()
		return change, nil
	}

	resource, err, code := this.permissions.GetResource(this.token, this.deviceTopic, id)
	if err != nil && code != 404 {
		return change, fmt.Errorf("unable to read permissions of device %v: %w", id, err)
	}

	device := toDevice(page[0], readableGroups(resource))
	change.Device = device
	change.Exists = true

	this.mux.Lock()
	previous, known := this.deviceById[id]
	// A changed device type means changed columns, which means the cached
	// readings were read from columns that may no longer exist.
	if known && previous.DeviceTypeId != device.DeviceTypeId {
		for _, carrierName := range model.Carriers {
			delete(this.readings, model.ReadingKey{DeviceId: id, Carrier: carrierName})
		}
	}
	this.deviceById[id] = device
	this.mux.Unlock()

	return change, nil
}

// DevicesOfDeviceType lists the ids this service holds for a device type.
//
// A device-type trigger names the type, not the devices; this is how it is
// turned into work.
func (this *Cache) DevicesOfDeviceType(deviceTypeId string) []string {
	this.mux.RLock()
	defer this.mux.RUnlock()
	result := []string{}
	for id, device := range this.deviceById {
		if device.DeviceTypeId == deviceTypeId {
			result = append(result, id)
		}
	}
	sort.Strings(result)
	return result
}

// DevicesOfGroup lists the devices a group holds a permission on, ordered by
// id so a structure built from them does not depend on map iteration.
func (this *Cache) DevicesOfGroup(path string) []model.Device {
	this.mux.RLock()
	defer this.mux.RUnlock()
	result := []model.Device{}
	for _, device := range this.deviceById {
		for _, groupPath := range device.Groups {
			if groupPath == path {
				result = append(result, device)
				break
			}
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Id < result[j].Id })
	return result
}

func (this *Cache) Group(path string) (model.Group, bool) {
	this.mux.RLock()
	defer this.mux.RUnlock()
	group, ok := this.groupByPat[path]
	return group, ok
}

func (this *Cache) Groups() []model.Group {
	this.mux.RLock()
	defer this.mux.RUnlock()
	result := make([]model.Group, 0, len(this.groupByPat))
	for _, group := range this.groupByPat {
		result = append(result, group)
	}
	sortGroups(result)
	return result
}

// DeviceCount is how many devices the service holds a view of, for the info
// endpoint. Zero after a failed sweep is worth being able to see.
func (this *Cache) DeviceCount() int {
	this.mux.RLock()
	defer this.mux.RUnlock()
	return len(this.deviceById)
}

func (this *Cache) Device(id string) (model.Device, bool) {
	this.mux.RLock()
	defer this.mux.RUnlock()
	device, ok := this.deviceById[id]
	return device, ok
}

// Readings returns the cached readings for the given devices that are still
// fresh and were measured over the given window.
//
// Both conditions matter. A figure read over a different stretch answers a
// different question, and a figure older than the ttl may predate a meter
// being rewired.
func (this *Cache) Readings(devices []model.Device, window model.Window, ttl time.Duration) map[model.ReadingKey]model.Reading {
	this.mux.RLock()
	defer this.mux.RUnlock()
	cutoff := this.now().Add(-ttl)
	result := map[model.ReadingKey]model.Reading{}
	for _, device := range devices {
		for _, carrierName := range device.CarriersOf() {
			key := model.ReadingKey{DeviceId: device.Id, Carrier: carrierName}
			reading, known := this.readings[key]
			if !known || reading.Window != window || reading.FetchedAt.Before(cutoff) {
				continue
			}
			result[key] = reading
		}
	}
	return result
}

// PutReadings stores freshly fetched readings.
func (this *Cache) PutReadings(readings map[model.ReadingKey]model.Reading) {
	this.mux.Lock()
	defer this.mux.Unlock()
	for key, reading := range readings {
		this.readings[key] = reading
	}
}

// listDeviceGroups pages the permission listing and returns, per device, the
// group paths that hold at least read.
func (this *Cache) listDeviceGroups(ctx context.Context) (map[string][]string, error) {
	result := map[string][]string{}
	for offset := int64(0); ; offset += pageSize {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		page, err, _ := this.permissions.ListResourcesWithAdminPermission(this.token, this.deviceTopic, permissions.ListOptions{
			Limit:  pageSize,
			Offset: offset,
		})
		if err != nil {
			return nil, err
		}
		for _, resource := range page {
			result[resource.Id] = readableGroups(resource)
		}
		if int64(len(page)) < pageSize {
			break
		}
	}
	return result, nil
}

// readableGroups is the group paths of a resource that hold at least read.
//
// Read and not execute: what makes a device part of a company is that the
// company can see it, and read is the weakest right that means so.
func readableGroups(resource permissions.Resource) []string {
	result := []string{}
	for path, perm := range resource.GroupPermissions {
		if perm.Read {
			result = append(result, path)
		}
	}
	sort.Strings(result)
	return result
}

func toDevice(extended platform.ExtendedDevice, groups []string) model.Device {
	name := extended.DisplayName
	if name == "" {
		name = extended.Name
	}
	device := model.Device{
		Id:           extended.Id,
		Name:         name,
		DeviceTypeId: extended.DeviceTypeId,
		Groups:       groups,
		Columns:      []model.CarrierColumn{},
	}
	// The full device type ships with the device, so naming a carrier costs
	// no extra request. Without it the columns are simply unknown - better an
	// empty list than a guess, since a device with no columns is attached to
	// the root rather than placed.
	if extended.DeviceType != nil {
		device.Columns = carrier.Columns(*extended.DeviceType)
	}
	if device.Groups == nil {
		device.Groups = []string{}
	}
	return device
}

func sortGroups(groups []model.Group) {
	sort.Slice(groups, func(i, j int) bool { return groups[i].Path < groups[j].Path })
}
