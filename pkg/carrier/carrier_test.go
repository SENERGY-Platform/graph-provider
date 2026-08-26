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

const (
	electricityFunctionId = "urn:infai:ses:measuring-function:57dfd369-92db-462c-aca4-a767b52c972e"
	gasFunctionId         = "urn:infai:ses:measuring-function:0bab7253-5e8a-4e7c-9005-39724d6a2b4f"
	unrelatedFunctionId   = "urn:infai:ses:controlling-function:deadbeef-0000-0000-0000-000000000000"
)

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
							content(models.ContentVariable{
								Name:       "value",
								FunctionId: electricityFunctionId,
							}),
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
											{Name: "value", FunctionId: electricityFunctionId},
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
							content(models.ContentVariable{Name: "gas_in", FunctionId: gasFunctionId}),
							content(models.ContentVariable{Name: "electricity_out", FunctionId: electricityFunctionId}),
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
									{Name: "l1", FunctionId: electricityFunctionId},
									{Name: "l2", FunctionId: electricityFunctionId},
									{Name: "l3", FunctionId: electricityFunctionId},
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
			name: "an input annotated with a carrier function is ignored",
			deviceType: models.DeviceType{
				Services: []models.Service{
					{
						Id: "service-1",
						Inputs: []models.Content{
							content(models.ContentVariable{Name: "setpoint", FunctionId: electricityFunctionId}),
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
											{Name: "value", FunctionId: electricityFunctionId},
										},
									},
									{Name: "sibling", FunctionId: gasFunctionId},
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
			name: "services and outputs but no matching function",
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
							content(models.ContentVariable{Name: "z", FunctionId: electricityFunctionId}),
							content(models.ContentVariable{Name: "a", FunctionId: gasFunctionId}),
						},
					},
					{
						Id: "service-a",
						Outputs: []models.Content{
							content(models.ContentVariable{Name: "m", FunctionId: electricityFunctionId}),
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
							}},
						},
						{
							Name:       "total",
							FunctionId: model.CarrierFunctionId[model.Electricity],
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
