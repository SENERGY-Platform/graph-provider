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

// Package carrier derives the carrier columns of a device type.
//
// The device repository has no field saying "this is a gas meter". What it
// has is the annotation on each output content variable: a measuring function
// saying what quantity was counted, and a set of aspects saying of what and
// which way. carrierOf turns that annotation into a carrier, by the same rule
// the dashboard applies, and this package walks a device type's outputs and
// produces the columns the structure heuristic needs - see
// docs/structure-heuristic.md.
package carrier

import (
	"slices"
	"sort"
	"strings"

	"github.com/SENERGY-Platform/graph-provider/pkg/model"
	"github.com/SENERGY-Platform/models/go/models"
)

// Columns returns every column of a device type that reads one of the carriers.
//
// Outputs only - an input carrying a measuring function is a device being
// *told* a value, not a reading, and has no series behind it.
//
// More than one column is normal rather than exceptional: a combined heat
// and power unit reads the gas going in and the electricity coming out, and
// a three-phase supply annotates the same function once per phase. Neither
// case is deduplicated.
func Columns(deviceType models.DeviceType) []model.CarrierColumn {
	columns := []model.CarrierColumn{}
	for _, service := range deviceType.Services {
		for _, output := range service.Outputs {
			columns = collect(output.ContentVariable, service.Id, nil, columns)
		}
	}

	sort.Slice(columns, func(i, j int) bool {
		if columns[i].ServiceId != columns[j].ServiceId {
			return columns[i].ServiceId < columns[j].ServiceId
		}
		return columns[i].Name < columns[j].Name
	})
	return columns
}

// collect walks one content variable tree, appending the paths that carry a
// carrier to into and returning the extended slice.
//
// A structured output nests: a service returning an object with a "power"
// and an "energy" field carries the annotation on the leaves, not on the
// structure around them, so the path names have to be accumulated on the
// way down rather than known up front.
//
// A variable whose name cannot be addressed as a column is abandoned rather
// than turned into a path nothing can ask about - its siblings are still
// walked by the caller. See addressable for what that excludes and why.
func collect(variable models.ContentVariable, serviceId string, prefix []string, into []model.CarrierColumn) []model.CarrierColumn {
	if !addressable(variable.Name) {
		return into
	}
	path := append(append([]string{}, prefix...), variable.Name)

	if carrier, ok := carrierOf(variable.FunctionId, variable.AspectIds); ok {
		into = append(into, model.CarrierColumn{
			ServiceId: serviceId,
			Name:      strings.Join(path, model.ColumnPathSeparator),
			Carrier:   carrier,
		})
	}

	for _, child := range variable.SubContentVariables {
		into = collect(child, serviceId, path, into)
	}
	return into
}

// carrierOf is the carrier a value counts what was drawn of, if any.
//
// A value reads a carrier when it carries the carrier's medium aspect and is
// counted with the carrier's function - see model.MediumAspectId. Where it names
// two media, the first in model.Carriers order wins; the model forbids two
// aspects of one class, but the device repository does not enforce that, so the
// answer has to be stable.
//
// AspectIds only, never the deprecated AspectId. The device repository fills
// the plural field from the singular one on every read, and sets the singular
// one to the alphabetically first of the set - on a value naming a medium and a
// direction, a coin flip between the two.
//
// There is no fallback to the medium-specific functions that came before
// Get-Energy and Get-Volume. The catalog no longer carries them, and the
// dashboard does not read them either: a column only one side recognised would
// be a contradiction between the graph and the view drawing it.
func carrierOf(functionId string, aspectIds []string) (model.Carrier, bool) {
	if functionId == "" {
		return "", false
	}
	for _, aspectId := range aspectIds {
		if aspectId == model.GenerationAspectId || model.UnreadAspectIds[aspectId] {
			return "", false
		}
	}
	for _, carrier := range model.Carriers {
		if functionId == model.CarrierFunctionId[carrier] && slices.Contains(aspectIds, model.MediumAspectId[carrier]) {
			return carrier, true
		}
	}
	return "", false
}

// addressable reports whether a content variable's name can appear in a column
// path the timescale wrapper will accept.
//
// The two ends of this disagree, and a device type may legitimately sit in the
// gap. The device repository accepts "*" as the name of a list's single
// sub-variable - that is how a list of variable length is modelled, not a
// mistake - and it accepts a bare number as an array index. The wrapper
// validates a column name against [A-Za-z0-9._-] and rejects the whole request
// when one element fails, so a single "*" in a path would cost the readings of
// every device batched with it. Those devices then look like devices that
// reported nothing and are attached to the root, which is the harmless
// outcome; the group getting no graph at all, forever, is not.
//
// So a name outside the addressable set ends that branch here. The device is
// then simply one whose type reads no carrier, which is a case the heuristic
// already handles. Numbers stay: the wrapper's class contains digits.
func addressable(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '.', r == '-', r == '_':
		default:
			return false
		}
	}
	return true
}
