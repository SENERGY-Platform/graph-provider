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

package carrier

import (
	"reflect"
	"testing"

	"github.com/SENERGY-Platform/graph-provider/pkg/model"
	"github.com/SENERGY-Platform/models/go/models"
)

const unrelatedFunctionId = "urn:infai:ses:controlling-function:deadbeef-0000-0000-0000-000000000000"

// meter is a value annotated the way a counter of carrier is: the carrier's
// measuring function and its medium aspect.
func meter(name string, carrier model.Carrier) models.ContentVariable {
	return models.ContentVariable{
		Name:       name,
		FunctionId: model.CarrierFunctionId[carrier],
		AspectIds:  []string{model.MediumAspectId[carrier]},
	}
}

// content wraps a content variable into a models.Content, the shape Inputs
// and Outputs are lists of.
func content(variable models.ContentVariable) models.Content {
	return models.Content{ContentVariable: variable}
}

func TestColumns(t *testing.T) {
	tests := []struct {
		name       string
		deviceType models.DeviceType
		want       []model.CarrierColumn
	}{
		{
			name: "flat electricity meter",
			deviceType: models.DeviceType{
				Services: []models.Service{
					{
						Id: "service-1",
						Outputs: []models.Content{
							content(meter("value", model.Electricity)),
						},
					},
				},
			},
			want: []model.CarrierColumn{
				{ServiceId: "service-1", Name: "value", Carrier: model.Electricity},
			},
		},
		{
			name: "annotation two levels down produces a dotted path",
			deviceType: models.DeviceType{
				Services: []models.Service{
					{
						Id: "service-1",
						Outputs: []models.Content{
							content(models.ContentVariable{
								Name: "energy",
								SubContentVariables: []models.ContentVariable{
									{
										Name: "phase",
										SubContentVariables: []models.ContentVariable{
											meter("value", model.Electricity),
										},
									},
								},
							}),
						},
					},
				},
			},
			want: []model.CarrierColumn{
				{ServiceId: "service-1", Name: "energy.phase.value", Carrier: model.Electricity},
			},
		},
		{
			name: "chp reads gas in and electricity out",
			deviceType: models.DeviceType{
				Services: []models.Service{
					{
						Id: "service-1",
						Outputs: []models.Content{
							content(meter("gas_in", model.Gas)),
							content(meter("electricity_out", model.Electricity)),
						},
					},
				},
			},
			want: []model.CarrierColumn{
				{ServiceId: "service-1", Name: "electricity_out", Carrier: model.Electricity},
				{ServiceId: "service-1", Name: "gas_in", Carrier: model.Gas},
			},
		},
		{
			name: "three phases annotate the same function three times",
			deviceType: models.DeviceType{
				Services: []models.Service{
					{
						Id: "service-1",
						Outputs: []models.Content{
							content(models.ContentVariable{
								Name: "power",
								SubContentVariables: []models.ContentVariable{
									meter("l1", model.Electricity),
									meter("l2", model.Electricity),
									meter("l3", model.Electricity),
								},
							}),
						},
					},
				},
			},
			want: []model.CarrierColumn{
				{ServiceId: "service-1", Name: "power.l1", Carrier: model.Electricity},
				{ServiceId: "service-1", Name: "power.l2", Carrier: model.Electricity},
				{ServiceId: "service-1", Name: "power.l3", Carrier: model.Electricity},
			},
		},
		{
			name: "an input annotated like a meter is ignored",
			deviceType: models.DeviceType{
				Services: []models.Service{
					{
						Id: "service-1",
						Inputs: []models.Content{
							content(meter("setpoint", model.Electricity)),
						},
					},
				},
			},
			want: []model.CarrierColumn{},
		},
		{
			name: "an empty name mid-path drops that branch but not its siblings",
			deviceType: models.DeviceType{
				Services: []models.Service{
					{
						Id: "service-1",
						Outputs: []models.Content{
							content(models.ContentVariable{
								Name: "root",
								SubContentVariables: []models.ContentVariable{
									{
										// No name: this branch cannot be addressed as a
										// column, even though a descendant is annotated.
										Name: "",
										SubContentVariables: []models.ContentVariable{
											meter("value", model.Electricity),
										},
									},
									meter("sibling", model.Gas),
								},
							}),
						},
					},
				},
			},
			want: []model.CarrierColumn{
				{ServiceId: "service-1", Name: "root.sibling", Carrier: model.Gas},
			},
		},
		{
			name:       "no services at all",
			deviceType: models.DeviceType{},
			want:       []model.CarrierColumn{},
		},
		{
			name: "services and outputs but no meter annotation",
			deviceType: models.DeviceType{
				Services: []models.Service{
					{
						Id: "service-1",
						Outputs: []models.Content{
							content(models.ContentVariable{Name: "value", FunctionId: unrelatedFunctionId}),
							content(models.ContentVariable{Name: "other"}),
						},
					},
				},
			},
			want: []model.CarrierColumn{},
		},
		{
			name: "result is sorted by service id then name regardless of input order",
			deviceType: models.DeviceType{
				Services: []models.Service{
					{
						Id: "service-b",
						Outputs: []models.Content{
							content(meter("z", model.Electricity)),
							content(meter("a", model.Gas)),
						},
					},
					{
						Id: "service-a",
						Outputs: []models.Content{
							content(meter("m", model.Electricity)),
						},
					},
				},
			},
			want: []model.CarrierColumn{
				{ServiceId: "service-a", Name: "m", Carrier: model.Electricity},
				{ServiceId: "service-b", Name: "a", Carrier: model.Gas},
				{ServiceId: "service-b", Name: "z", Carrier: model.Electricity},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := Columns(test.deviceType)
			if got == nil {
				t.Fatalf("Columns() returned nil, want a non-nil slice")
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Errorf("Columns() = %#v, want %#v", got, test.want)
			}
		})
	}
}

// A list of variable length is modelled with a single sub-variable named "*".
// The device repository accepts that name explicitly; the timescale wrapper's
// column validation does not, and it rejects the entire request when one
// element fails. Left in, a single such device type would cost every device
// batched with it its readings - and the group would never get a graph at all.
func TestWildcardListNameIsNotAColumn(t *testing.T) {
	deviceType := models.DeviceType{
		Id: "dt",
		Services: []models.Service{{
			Id: "svc",
			Outputs: []models.Content{{
				ContentVariable: models.ContentVariable{
					Name: "phases",
					Type: models.List,
					SubContentVariables: []models.ContentVariable{{
						Name: "*",
						SubContentVariables: []models.ContentVariable{{
							Name:       "energy",
							FunctionId: model.CarrierFunctionId[model.Electricity],
							AspectIds:  []string{model.MediumAspectId[model.Electricity]},
						}},
					}},
				},
			}},
		}},
	}
	if got := Columns(deviceType); len(got) != 0 {
		t.Errorf("expected no addressable column, got %+v", got)
	}
}

// A numeric index is also accepted by the device repository, and the wrapper's
// character class does contain digits - so those paths stay.
func TestNumericIndexIsAColumn(t *testing.T) {
	deviceType := models.DeviceType{
		Id: "dt",
		Services: []models.Service{{
			Id: "svc",
			Outputs: []models.Content{{
				ContentVariable: models.ContentVariable{
					Name: "phases",
					SubContentVariables: []models.ContentVariable{{
						Name: "0",
						SubContentVariables: []models.ContentVariable{{
							Name:       "energy",
							FunctionId: model.CarrierFunctionId[model.Electricity],
							AspectIds:  []string{model.MediumAspectId[model.Electricity]},
						}},
					}},
				},
			}},
		}},
	}
	got := Columns(deviceType)
	if len(got) != 1 || got[0].Name != "phases.0.energy" {
		t.Errorf("expected phases.0.energy, got %+v", got)
	}
}

// A sibling of an unaddressable branch is still found: the branch ends, the
// walk does not.
func TestASiblingOfAWildcardBranchSurvives(t *testing.T) {
	deviceType := models.DeviceType{
		Id: "dt",
		Services: []models.Service{{
			Id: "svc",
			Outputs: []models.Content{{
				ContentVariable: models.ContentVariable{
					Name: "reading",
					SubContentVariables: []models.ContentVariable{
						{
							Name: "*",
							SubContentVariables: []models.ContentVariable{{
								Name:       "lost",
								FunctionId: model.CarrierFunctionId[model.Electricity],
								AspectIds:  []string{model.MediumAspectId[model.Electricity]},
							}},
						},
						{
							Name:       "total",
							FunctionId: model.CarrierFunctionId[model.Electricity],
							AspectIds:  []string{model.MediumAspectId[model.Electricity]},
						},
					},
				},
			}},
		}},
	}
	got := Columns(deviceType)
	if len(got) != 1 || got[0].Name != "reading.total" {
		t.Errorf("expected only reading.total, got %+v", got)
	}
}

// The recognition rule, case by case. It has to give the dashboard's answer on
// every value, so the cases follow carrier.ts there: a column only one side
// recognised would place a device the other side calls one that measures nothing.
func TestCarrierOf(t *testing.T) {
	const (
		consumption     = "urn:infai:ses:aspect:74a7b913-73ac-42b7-9b35-573f2c1e97cf"
		storage         = "urn:infai:ses:aspect:9a02ed7f-2304-47a1-92bd-54998959351a"
		charging        = "urn:infai:ses:aspect:a47386f2-3160-404e-a700-ae4ce06f49bd"
		discharging     = "urn:infai:ses:aspect:69db99ab-5fb5-4375-b580-792833f9fef6"
		target          = "urn:infai:ses:aspect:4f4188b6-f944-45fa-9429-15655f691397"
		total           = "urn:infai:ses:aspect:fdc999eb-d366-44e8-9d24-bfd48d5fece1"
		powerFunctionId = "urn:infai:ses:measuring-function:1c7c90fb-73b6-4690-aac2-72e9735e68d0" // Get Power
		// Get Electricity Energy Consumption, the function the medium used to be in.
		oldElectricityFunctionId = "urn:infai:ses:measuring-function:57dfd369-92db-462c-aca4-a767b52c972e"
	)
	electricity := model.MediumAspectId[model.Electricity]
	gas := model.MediumAspectId[model.Gas]
	oil := model.MediumAspectId[model.Oil]
	heat := model.MediumAspectId[model.Heat]
	water := model.MediumAspectId[model.Water]

	tests := []struct {
		name       string
		functionId string
		aspectIds  []string
		want       model.Carrier
	}{
		{"electricity on Get-Energy", model.EnergyFunctionId, []string{electricity}, model.Electricity},
		{"gas on Get-Energy", model.EnergyFunctionId, []string{gas}, model.Gas},
		{"heating oil on Get-Energy", model.EnergyFunctionId, []string{oil}, model.Oil},
		{"heat on Get-Energy", model.EnergyFunctionId, []string{heat}, model.Heat},
		{"water on Get-Volume", model.VolumeFunctionId, []string{water}, model.Water},
		{"consumption is a draw", model.EnergyFunctionId, []string{electricity, consumption}, model.Electricity},
		{"aspects beside the medium do not matter", model.EnergyFunctionId, []string{total, electricity}, model.Electricity},
		{"gas and Heating is gas", model.EnergyFunctionId, []string{heat, gas}, model.Gas},
		{"heating oil and Heating is oil", model.EnergyFunctionId, []string{heat, oil}, model.Oil},
		{"two media are read as the first in Carriers order", model.EnergyFunctionId, []string{gas, electricity}, model.Electricity},

		{"generation is not read", model.EnergyFunctionId, []string{electricity, model.GenerationAspectId}, ""},
		{"storage is not read", model.EnergyFunctionId, []string{electricity, storage}, ""},
		{"charging is not read", model.EnergyFunctionId, []string{electricity, charging}, ""},
		{"discharging is not read", model.EnergyFunctionId, []string{electricity, discharging}, ""},
		{"a target is not read", model.EnergyFunctionId, []string{electricity, target}, ""},
		{"the medium on another quantity is not read", powerFunctionId, []string{electricity}, ""},
		{"Get-Energy without a medium is not read", model.EnergyFunctionId, []string{consumption}, ""},
		{"no function is not read", "", []string{electricity}, ""},
		// Gas counted in cubic metres becomes energy only through a calorific value
		// no device type states. Read as gas it would be compared against meters
		// counting kilowatt-hours in the same tree.
		{"gas on Get-Volume is not read", model.VolumeFunctionId, []string{gas}, ""},
		{"water on Get-Energy is not read", model.EnergyFunctionId, []string{water}, ""},
		{"the old medium-specific function is not read", oldElectricityFunctionId, nil, ""},
		{"the old function with a medium aspect is not read", oldElectricityFunctionId, []string{electricity}, ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := carrierOf(test.functionId, test.aspectIds)
			if ok != (test.want != "") || got != test.want {
				t.Errorf("carrierOf() = %q, %v, want %q", got, ok, test.want)
			}
		})
	}
}

// A bidirectional grid meter declares its import and export register on the
// same function and medium. Only the import is a draw.
func TestColumnsOfABidirectionalMeterReadTheImportOnly(t *testing.T) {
	export := meter("export", model.Electricity)
	export.AspectIds = append(export.AspectIds, model.GenerationAspectId)
	deviceType := models.DeviceType{
		Services: []models.Service{{
			Id: "svc",
			Outputs: []models.Content{
				content(models.ContentVariable{
					Name:                "register",
					SubContentVariables: []models.ContentVariable{meter("import", model.Electricity), export},
				}),
			},
		}},
	}
	want := []model.CarrierColumn{{ServiceId: "svc", Name: "register.import", Carrier: model.Electricity}}
	if got := Columns(deviceType); !reflect.DeepEqual(got, want) {
		t.Errorf("Columns() = %#v, want %#v", got, want)
	}
}

// The order decides a value naming two media and the tie in primaryCarrier, and
// it has to be the dashboard's ENERGY_CARRIERS. Pinned literally so that a
// reordering here fails rather than silently disagreeing with the view.
func TestCarriersFollowTheDashboardOrder(t *testing.T) {
	want := []model.Carrier{"electricity", "gas", "oil", "heat", "water"}
	if !reflect.DeepEqual(model.Carriers, want) {
		t.Errorf("model.Carriers = %v, want the dashboard's order %v", model.Carriers, want)
	}
}
