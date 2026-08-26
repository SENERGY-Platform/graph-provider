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

package cache

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	devicerepomodel "github.com/SENERGY-Platform/device-repository/lib/model"
	"github.com/SENERGY-Platform/graph-provider/pkg/model"
	platform "github.com/SENERGY-Platform/models/go/models"
	permissions "github.com/SENERGY-Platform/permissions-v2/pkg/client"
)

const testTopic = "devices"

type fakeDevices struct {
	all   []platform.ExtendedDevice
	calls []devicerepomodel.ExtendedDeviceListOptions
	err   error
}

func (this *fakeDevices) ListExtendedDevices(_ string, options devicerepomodel.ExtendedDeviceListOptions) ([]platform.ExtendedDevice, int64, error, int) {
	this.calls = append(this.calls, options)
	if this.err != nil {
		return nil, 0, this.err, 500
	}
	if options.Ids != nil {
		wanted := map[string]bool{}
		for _, id := range options.Ids {
			wanted[id] = true
		}
		result := []platform.ExtendedDevice{}
		for _, device := range this.all {
			if wanted[device.Id] {
				result = append(result, device)
			}
		}
		return result, int64(len(result)), nil, 200
	}
	start := int(options.Offset)
	if start > len(this.all) {
		start = len(this.all)
	}
	end := start + int(options.Limit)
	if end > len(this.all) {
		end = len(this.all)
	}
	return this.all[start:end], int64(len(this.all)), nil, 200
}

type fakePermissions struct {
	resources map[string]permissions.Resource
	order     []string
	listCalls int
}

func (this *fakePermissions) ListResourcesWithAdminPermission(_ string, _ string, options permissions.ListOptions) ([]permissions.Resource, error, int) {
	this.listCalls++
	start := int(options.Offset)
	if start > len(this.order) {
		start = len(this.order)
	}
	end := start + int(options.Limit)
	if end > len(this.order) {
		end = len(this.order)
	}
	result := []permissions.Resource{}
	for _, id := range this.order[start:end] {
		result = append(result, this.resources[id])
	}
	return result, nil, 200
}

func (this *fakePermissions) GetResource(_ string, _ string, id string) (permissions.Resource, error, int) {
	resource, ok := this.resources[id]
	if !ok {
		return permissions.Resource{}, errors.New("not found"), 404
	}
	return resource, nil, 200
}

type fakeGroups struct {
	groups []model.Group
	err    error
}

func (this *fakeGroups) Groups(context.Context) ([]model.Group, error) {
	return this.groups, this.err
}

func electricityType(id string) *platform.DeviceType {
	return &platform.DeviceType{
		Id: id,
		Services: []platform.Service{{
			Id: id + ":svc",
			Outputs: []platform.Content{{
				ContentVariable: platform.ContentVariable{
					Name: "reading",
					SubContentVariables: []platform.ContentVariable{{
						Name:       "energy",
						FunctionId: model.CarrierFunctionId[model.Electricity],
					}},
				},
			}},
		}},
	}
}

func device(id string, name string, typeId string, dt *platform.DeviceType) platform.ExtendedDevice {
	return platform.ExtendedDevice{
		Device:      platform.Device{Id: id, Name: name, DeviceTypeId: typeId},
		DisplayName: name,
		DeviceType:  dt,
	}
}

func resource(id string, groups map[string]bool) permissions.Resource {
	perms := map[string]permissions.PermissionsMap{}
	for path, read := range groups {
		perms[path] = permissions.PermissionsMap{Read: read}
	}
	return permissions.Resource{
		Id:                  id,
		TopicId:             testTopic,
		ResourcePermissions: permissions.ResourcePermissions{GroupPermissions: perms},
	}
}

func newTestCache(devices *fakeDevices, perms *fakePermissions, groups *fakeGroups, allow func(string) bool) *Cache {
	return New(devices, perms, groups, "token", testTopic, allow)
}

func TestRefreshDevicesMergesGroupsAndColumns(t *testing.T) {
	devices := &fakeDevices{all: []platform.ExtendedDevice{
		device("d1", "Hauptzähler", "dt1", electricityType("dt1")),
		device("d2", "Nebenzähler", "dt1", electricityType("dt1")),
		// No device type shipped: the columns are unknown, which is not the
		// same as a device that reads nothing.
		device("d3", "Sensor", "dt2", nil),
	}}
	perms := &fakePermissions{
		order: []string{"d1", "d2", "d3"},
		resources: map[string]permissions.Resource{
			"d1": resource("d1", map[string]bool{"/acme": true}),
			"d2": resource("d2", map[string]bool{"/acme": true, "/beta": true}),
			// Present but without read: not part of any company.
			"d3": resource("d3", map[string]bool{"/acme": false}),
		},
	}
	c := newTestCache(devices, perms, &fakeGroups{}, nil)

	if err := c.RefreshDevices(context.Background()); err != nil {
		t.Fatal(err)
	}

	d1, ok := c.Device("d1")
	if !ok {
		t.Fatal("d1 missing")
	}
	if len(d1.Columns) != 1 || d1.Columns[0].Carrier != model.Electricity {
		t.Errorf("expected one electricity column, got %+v", d1.Columns)
	}
	if d1.Columns[0].Name != "reading.energy" {
		t.Errorf("expected dotted column path, got %v", d1.Columns[0].Name)
	}

	acme := c.DevicesOfGroup("/acme")
	if len(acme) != 2 || acme[0].Id != "d1" || acme[1].Id != "d2" {
		t.Errorf("expected d1 and d2 in /acme in id order, got %+v", ids(acme))
	}
	if got := c.DevicesOfGroup("/beta"); len(got) != 1 || got[0].Id != "d2" {
		t.Errorf("expected only d2 in /beta, got %+v", ids(got))
	}

	d3, _ := c.Device("d3")
	if len(d3.Groups) != 0 {
		t.Errorf("a group without read must not count as membership, got %+v", d3.Groups)
	}
	if d3.HasEnergyFlow() {
		t.Error("a device without a device type has no known columns")
	}
}

func TestRefreshDevicesPagesUntilShortPage(t *testing.T) {
	all := []platform.ExtendedDevice{}
	order := []string{}
	resources := map[string]permissions.Resource{}
	for i := 0; i < pageSize+7; i++ {
		id := "d" + string(rune('a'+i%26)) + itoa(i)
		all = append(all, device(id, id, "dt1", electricityType("dt1")))
		order = append(order, id)
		resources[id] = resource(id, map[string]bool{"/acme": true})
	}
	devices := &fakeDevices{all: all}
	c := newTestCache(devices, &fakePermissions{order: order, resources: resources}, &fakeGroups{}, nil)

	if err := c.RefreshDevices(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(c.DevicesOfGroup("/acme")); got != len(all) {
		t.Errorf("expected every device, got %v of %v", got, len(all))
	}
	if len(devices.calls) != 2 {
		t.Errorf("expected two device pages, got %v", len(devices.calls))
	}
}

func TestRefreshDeviceReportsGroupsBeforeAndAfter(t *testing.T) {
	devices := &fakeDevices{all: []platform.ExtendedDevice{
		device("d1", "Zähler", "dt1", electricityType("dt1")),
	}}
	perms := &fakePermissions{
		order:     []string{"d1"},
		resources: map[string]permissions.Resource{"d1": resource("d1", map[string]bool{"/acme": true})},
	}
	c := newTestCache(devices, perms, &fakeGroups{}, nil)
	if err := c.RefreshDevices(context.Background()); err != nil {
		t.Fatal(err)
	}

	// The device is handed over to another company.
	perms.resources["d1"] = resource("d1", map[string]bool{"/beta": true})

	change, err := c.RefreshDevice(context.Background(), "d1")
	if err != nil {
		t.Fatal(err)
	}
	if !change.Exists {
		t.Fatal("device should still exist")
	}
	want := []string{"/acme", "/beta"}
	if got := change.AffectedGroups(); !reflect.DeepEqual(got, want) {
		t.Errorf("both the old and the new group must be affected, got %+v", got)
	}
	if got := c.DevicesOfGroup("/acme"); len(got) != 0 {
		t.Errorf("expected /acme to be empty, got %+v", ids(got))
	}
}

func TestRefreshDeviceForgetsADeletedDevice(t *testing.T) {
	devices := &fakeDevices{all: []platform.ExtendedDevice{
		device("d1", "Zähler", "dt1", electricityType("dt1")),
	}}
	perms := &fakePermissions{
		order:     []string{"d1"},
		resources: map[string]permissions.Resource{"d1": resource("d1", map[string]bool{"/acme": true})},
	}
	c := newTestCache(devices, perms, &fakeGroups{}, nil)
	if err := c.RefreshDevices(context.Background()); err != nil {
		t.Fatal(err)
	}
	c.PutReadings(map[model.ReadingKey]model.Reading{
		{DeviceId: "d1", Carrier: model.Electricity}: {DeviceId: "d1", Carrier: model.Electricity, Value: 5},
	})

	devices.all = nil
	change, err := c.RefreshDevice(context.Background(), "d1")
	if err != nil {
		t.Fatal(err)
	}
	if change.Exists {
		t.Error("a device the repository no longer returns must not be reported as existing")
	}
	if got := change.AffectedGroups(); !reflect.DeepEqual(got, []string{"/acme"}) {
		t.Errorf("the group it belonged to must still be affected, got %+v", got)
	}
	if _, ok := c.Device("d1"); ok {
		t.Error("device should be forgotten")
	}
}

func TestRefreshDeviceDropsReadingsOnDeviceTypeChange(t *testing.T) {
	devices := &fakeDevices{all: []platform.ExtendedDevice{
		device("d1", "Zähler", "dt1", electricityType("dt1")),
	}}
	perms := &fakePermissions{
		order:     []string{"d1"},
		resources: map[string]permissions.Resource{"d1": resource("d1", map[string]bool{"/acme": true})},
	}
	c := newTestCache(devices, perms, &fakeGroups{}, nil)
	if err := c.RefreshDevices(context.Background()); err != nil {
		t.Fatal(err)
	}

	window := model.Window{Start: time.Unix(0, 0), End: time.Unix(100, 0)}
	c.PutReadings(map[model.ReadingKey]model.Reading{
		{DeviceId: "d1", Carrier: model.Electricity}: {
			DeviceId: "d1", Carrier: model.Electricity, Value: 5,
			Window: window, FetchedAt: time.Unix(50, 0),
		},
	})
	c.now = func() time.Time { return time.Unix(60, 0) }

	d1, _ := c.Device("d1")
	if got := c.Readings([]model.Device{d1}, window, time.Hour); len(got) != 1 {
		t.Fatalf("expected the reading to be served, got %v", len(got))
	}

	// The device gets a new type: the old columns may not exist any more, so
	// the figure read from them is not answerable for.
	devices.all = []platform.ExtendedDevice{device("d1", "Zähler", "dt2", electricityType("dt2"))}
	if _, err := c.RefreshDevice(context.Background(), "d1"); err != nil {
		t.Fatal(err)
	}
	d1, _ = c.Device("d1")
	if got := c.Readings([]model.Device{d1}, window, time.Hour); len(got) != 0 {
		t.Errorf("readings must be dropped when the device type changes, got %v", len(got))
	}
}

func TestReadingsRejectStaleAndForeignWindows(t *testing.T) {
	c := newTestCache(&fakeDevices{}, &fakePermissions{}, &fakeGroups{}, nil)
	c.now = func() time.Time { return time.Unix(1000, 0) }

	window := model.Window{Start: time.Unix(0, 0), End: time.Unix(100, 0)}
	other := model.Window{Start: time.Unix(0, 0), End: time.Unix(200, 0)}
	dev := model.Device{
		Id:      "d1",
		Columns: []model.CarrierColumn{{ServiceId: "s", Name: "n", Carrier: model.Electricity}},
	}

	c.PutReadings(map[model.ReadingKey]model.Reading{
		{DeviceId: "d1", Carrier: model.Electricity}: {
			DeviceId: "d1", Carrier: model.Electricity, Value: 5,
			Window: window, FetchedAt: time.Unix(100, 0),
		},
	})

	if got := c.Readings([]model.Device{dev}, other, time.Hour); len(got) != 0 {
		t.Error("a figure measured over another stretch answers another question")
	}
	if got := c.Readings([]model.Device{dev}, window, 10*time.Second); len(got) != 0 {
		t.Error("a figure older than the ttl must not be served")
	}
	if got := c.Readings([]model.Device{dev}, window, time.Hour); len(got) != 1 {
		t.Error("a fresh figure for the right window must be served")
	}
}

func TestRefreshGroupsReportsAddedAndRemovedAndFilters(t *testing.T) {
	groups := &fakeGroups{groups: []model.Group{
		{Id: "1", Path: "/acme", Name: "acme"},
		{Id: "2", Path: "/acme/werk-nord", Name: "werk-nord"},
		{Id: "3", Path: "/internal/ops", Name: "ops"},
	}}
	allow := func(path string) bool { return path != "/internal/ops" }
	c := newTestCache(&fakeDevices{}, &fakePermissions{}, groups, allow)

	added, removed, err := c.RefreshGroups(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(added) != 2 || len(removed) != 0 {
		t.Fatalf("expected two added and none removed, got %v/%v", len(added), len(removed))
	}
	if _, ok := c.Group("/internal/ops"); ok {
		t.Error("a filtered group must not enter the cache")
	}

	// A group disappears, another is renamed.
	groups.groups = []model.Group{{Id: "1", Path: "/acme", Name: "acme-neu"}}
	added, removed, err = c.RefreshGroups(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(added) != 1 || added[0].Name != "acme-neu" {
		t.Errorf("a renamed group counts as changed, got %+v", added)
	}
	if len(removed) != 1 || removed[0].Path != "/acme/werk-nord" {
		t.Errorf("expected the vanished group to be reported, got %+v", removed)
	}
	if got := len(c.Groups()); got != 1 {
		t.Errorf("the tree is replaced, not merged, got %v groups", got)
	}
}

func TestDevicesOfDeviceType(t *testing.T) {
	devices := &fakeDevices{all: []platform.ExtendedDevice{
		device("d1", "a", "dt1", electricityType("dt1")),
		device("d2", "b", "dt2", electricityType("dt2")),
		device("d3", "c", "dt1", electricityType("dt1")),
	}}
	perms := &fakePermissions{order: []string{"d1", "d2", "d3"}, resources: map[string]permissions.Resource{
		"d1": resource("d1", nil), "d2": resource("d2", nil), "d3": resource("d3", nil),
	}}
	c := newTestCache(devices, perms, &fakeGroups{}, nil)
	if err := c.RefreshDevices(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := c.DevicesOfDeviceType("dt1"); !reflect.DeepEqual(got, []string{"d1", "d3"}) {
		t.Errorf("expected d1 and d3, got %+v", got)
	}
}

func TestRefreshDevicesPropagatesErrors(t *testing.T) {
	devices := &fakeDevices{err: errors.New("boom")}
	c := newTestCache(devices, &fakePermissions{}, &fakeGroups{}, nil)
	if err := c.RefreshDevices(context.Background()); err == nil {
		t.Error("an error from the repository must not be swallowed")
	}
}

func ids(devices []model.Device) []string {
	result := []string{}
	for _, device := range devices {
		result = append(result, device.Id)
	}
	return result
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	digits := []byte{}
	for i > 0 {
		digits = append([]byte{byte('0' + i%10)}, digits...)
		i /= 10
	}
	return string(digits)
}
