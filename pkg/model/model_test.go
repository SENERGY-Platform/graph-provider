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

package model

import (
	"reflect"
	"testing"
)

// The naming rule: a graph is named after the leaf of its group's path, without
// the leading slash. A nested group is named after itself, not after its
// ancestry.
func TestGroupName(t *testing.T) {
	cases := map[string]string{
		"/acme":                   "acme",
		"/acme/werk-nord":         "werk-nord",
		"/acme/werk-nord/halle-1": "halle-1",
		"/":                       "",
		"":                        "",
		// Not a path but a name: no leading slash, so it is used whole. This is
		// the exception the rule has to allow for - a group may be called "a/b".
		"a/b":  "a/b",
		"acme": "acme",
		// A trailing slash is Keycloak being untidy, not a nameless child.
		"/acme/": "acme",
	}
	for path, want := range cases {
		if got := GroupName(path); got != want {
			t.Errorf("%q: want %q, got %q", path, want, got)
		}
	}
}

// Every carrier needs both halves of the rule, and the medium is the half that
// tells carriers apart: two carriers on one aspect would be one carrier read
// twice. A medium aspect that is also a direction or an unread aspect would
// make its carrier unreadable.
func TestEveryCarrierHasAFunctionAndItsOwnMedium(t *testing.T) {
	if len(CarrierFunctionId) != len(Carriers) || len(MediumAspectId) != len(Carriers) {
		t.Fatalf("expected one function and one medium per carrier, got %v and %v for %v carriers",
			len(CarrierFunctionId), len(MediumAspectId), len(Carriers))
	}
	carrierOfMedium := map[string]Carrier{}
	for _, carrier := range Carriers {
		if functionId := CarrierFunctionId[carrier]; functionId != EnergyFunctionId && functionId != VolumeFunctionId {
			t.Errorf("%v is counted with %q, which is neither Get-Energy nor Get-Volume", carrier, functionId)
		}
		medium := MediumAspectId[carrier]
		if medium == "" {
			t.Errorf("%v has no medium aspect", carrier)
			continue
		}
		if other, taken := carrierOfMedium[medium]; taken {
			t.Errorf("%v and %v share the medium aspect %v", other, carrier, medium)
		}
		carrierOfMedium[medium] = carrier
		if medium == GenerationAspectId || UnreadAspectIds[medium] {
			t.Errorf("the medium aspect of %v keeps every column of it out", carrier)
		}
	}
}

func TestDeviceCarriers(t *testing.T) {
	// Deliberately out of Carriers order, to show the result is not the input
	// order: two views printing the same device must name its media alike.
	device := Device{
		Id: "d1",
		Columns: []CarrierColumn{
			{ServiceId: "s1", Name: "water", Carrier: Water},
			{ServiceId: "s1", Name: "l1", Carrier: Electricity},
			{ServiceId: "s1", Name: "l2", Carrier: Electricity},
		},
	}
	if !device.HasEnergyFlow() {
		t.Error("a device with columns carries a flow")
	}
	if got := device.CarriersOf(); !reflect.DeepEqual(got, []Carrier{Electricity, Water}) {
		t.Errorf("expected Carriers order without duplicates, got %+v", got)
	}

	// A device whose type reads nothing has no place a reading could argue for,
	// which is why it is attached to the root rather than placed.
	bare := Device{Id: "d2"}
	if bare.HasEnergyFlow() {
		t.Error("a device without columns carries no flow")
	}
	if got := bare.CarriersOf(); len(got) != 0 {
		t.Errorf("expected no carriers, got %+v", got)
	}
}

func TestReadingKey(t *testing.T) {
	reading := Reading{DeviceId: "d1", Carrier: Gas, Value: 12}
	want := ReadingKey{DeviceId: "d1", Carrier: Gas}
	if got := reading.Key(); got != want {
		t.Errorf("want %+v, got %+v", got, want)
	}
	// The key must separate carriers: a device reading gas and electricity has
	// two figures, and collapsing them would silently pick one.
	other := Reading{DeviceId: "d1", Carrier: Electricity}
	if reading.Key() == other.Key() {
		t.Error("two carriers of one device must not share a key")
	}
}
