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

package consumption

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/SENERGY-Platform/graph-provider/pkg/model"
	wrapper "github.com/SENERGY-Platform/timescale-wrapper/pkg/model"
)

// Service ids have to be shaped like real ones: the wrapper's own Valid()
// rejects anything else, and the request tests assert against it.
const (
	serviceA = "urn:infai:ses:service:11111111-1111-1111-1111-111111111111"
	serviceB = "urn:infai:ses:service:22222222-2222-2222-2222-222222222222"
)

const testToken = "Bearer test-token"

var testWindow = model.Window{
	Start: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
	End:   time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC),
}

var fixedNow = time.Date(2026, 8, 25, 9, 14, 0, 0, time.UTC)

// meter is what a fake wrapper answers, keyed by the question it was asked:
// device, column and sort direction. One entry per end of the window, so a
// meter that reported at only one end is expressed by leaving the other out.
type meter map[string]float64

func meterKey(deviceId string, column string, direction wrapper.Direction) string {
	return deviceId + "|" + column + "|" + string(direction)
}

func keyOf(element RequestElement) string {
	deviceId := ""
	if element.DeviceId != nil {
		deviceId = *element.DeviceId
	}
	column := ""
	if len(element.Columns) > 0 {
		column = element.Columns[0].Name
	}
	direction := wrapper.Direction("")
	if element.OrderDirection != nil {
		direction = *element.OrderDirection
	}
	return meterKey(deviceId, column, direction)
}

type fakeClient struct {
	// calls holds the request elements of every call, in the order they came.
	calls [][]RequestElement
	// tokens holds the token of every call.
	tokens []string
	// options holds the options of every call.
	options []*Options
	// answer produces one call's response. nil answers with nothing.
	answer func(elements []RequestElement) ([]ResponseElement, error)
}

func (this *fakeClient) GetQueriesV2(token string, requestElements []RequestElement, options *Options) ([]ResponseElement, int, error) {
	this.calls = append(this.calls, append([]RequestElement{}, requestElements...))
	this.tokens = append(this.tokens, token)
	this.options = append(this.options, options)
	if this.answer == nil {
		return []ResponseElement{}, 200, nil
	}
	result, err := this.answer(requestElements)
	if err != nil {
		return nil, 500, err
	}
	return result, 200, nil
}

// row is one answered row: a time and a value, the shape an un-aggregated
// query selects.
func row(value interface{}) []interface{} {
	return []interface{}{"2026-07-01T00:00:00Z", value}
}

func responseOf(requestIndex int, rows ...[]interface{}) ResponseElement {
	return ResponseElement{RequestIndex: requestIndex, Data: [][][]interface{}{rows}}
}

// fromMeter answers each request element from readings, and omits the element
// entirely when there is nothing for it - which is what the wrapper does: it
// appends no response element for a query that returned no rows.
func fromMeter(readings meter) func([]RequestElement) ([]ResponseElement, error) {
	return func(elements []RequestElement) ([]ResponseElement, error) {
		result := []ResponseElement{}
		for index, element := range elements {
			value, ok := readings[keyOf(element)]
			if !ok {
				continue
			}
			result = append(result, responseOf(index, row(value)))
		}
		return result, nil
	}
}

func fetcherFor(client Client) *Fetcher {
	fetcher := New(client, testToken)
	fetcher.Now = func() time.Time { return fixedNow }
	return fetcher
}

func device(id string, columns ...model.CarrierColumn) model.Device {
	return model.Device{Id: id, Name: id, Columns: columns}
}

func column(serviceId string, name string, carrier model.Carrier) model.CarrierColumn {
	return model.CarrierColumn{ServiceId: serviceId, Name: name, Carrier: carrier}
}

func fetch(t *testing.T, client *fakeClient, devices []model.Device) map[model.ReadingKey]model.Reading {
	t.Helper()
	result, err := fetcherFor(client).Fetch(context.Background(), devices, testWindow)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return result
}

func wantValue(t *testing.T, result map[model.ReadingKey]model.Reading, deviceId string, carrier model.Carrier, want float64) {
	t.Helper()
	reading, ok := result[model.ReadingKey{DeviceId: deviceId, Carrier: carrier}]
	if !ok {
		t.Fatalf("no reading for %v/%v, have %v", deviceId, carrier, result)
	}
	if reading.Value != want {
		t.Errorf("%v/%v: got %v, want %v", deviceId, carrier, reading.Value, want)
	}
}

// wantAbsent asserts a missing entry rather than a zero one. The distinction is
// the point: the structure heuristic places a device it has a number for and
// hangs one it has no number for off the root.
func wantAbsent(t *testing.T, result map[model.ReadingKey]model.Reading, deviceId string, carrier model.Carrier) {
	t.Helper()
	reading, ok := result[model.ReadingKey{DeviceId: deviceId, Carrier: carrier}]
	if ok {
		t.Errorf("expected no entry for %v/%v, got %v", deviceId, carrier, reading)
	}
}

// TestRequestShape pins every part of the recipe that is invisible in the
// answer: get one of them wrong and the wrapper still replies, with figures
// that look like readings.
func TestRequestShape(t *testing.T) {
	client := &fakeClient{}
	devices := []model.Device{
		device("device-1",
			column(serviceA, "energy.value", model.Electricity),
			column(serviceB, "gas.volume", model.Gas)),
	}

	_, err := fetcherFor(client).Fetch(context.Background(), devices, testWindow)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(client.calls) != 1 {
		t.Fatalf("got %v calls, want 1", len(client.calls))
	}
	elements := client.calls[0]

	// Two elements per column, not one per column and not one per device.
	if len(elements) != 4 {
		t.Fatalf("got %v request elements, want 4", len(elements))
	}
	if client.tokens[0] != testToken {
		t.Errorf("token: got %q, want %q", client.tokens[0], testToken)
	}
	// The per-element order fields must decide, so no query-parameter override
	// may be sent alongside them.
	if client.options[0] != nil {
		t.Errorf("options: got %v, want nil", client.options[0])
	}

	wantColumns := []string{"energy.value", "energy.value", "gas.volume", "gas.volume"}
	wantServices := []string{serviceA, serviceA, serviceB, serviceB}
	wantDirections := []wrapper.Direction{wrapper.Asc, wrapper.Desc, wrapper.Asc, wrapper.Desc}

	for index := range elements {
		element := elements[index]

		if !element.Valid() {
			t.Errorf("element %v: the wrapper's own validation rejects it: %+v", index, element)
		}
		if element.DeviceId == nil || *element.DeviceId != "device-1" {
			t.Errorf("element %v: deviceId %v", index, element.DeviceId)
		}
		if element.ServiceId == nil || *element.ServiceId != wantServices[index] {
			t.Errorf("element %v: serviceId %v, want %v", index, element.ServiceId, wantServices[index])
		}
		// One column per element: requestIndex is only the whole of the mapping
		// while an element answers about exactly one series.
		if len(element.Columns) != 1 {
			t.Fatalf("element %v: got %v columns, want 1", index, len(element.Columns))
		}
		if element.Columns[0].Name != wantColumns[index] {
			t.Errorf("element %v: column %q, want %q", index, element.Columns[0].Name, wantColumns[index])
		}
		// Limit 1, or the wrapper answers with every raw row in the range.
		if element.Limit == nil || *element.Limit != 1 {
			t.Errorf("element %v: limit %v, want 1", index, element.Limit)
		}
		// The order column, without which the wrapper emits no ORDER BY at all
		// and both ends come back with the same row.
		if element.OrderColumnIndex == nil || *element.OrderColumnIndex != 0 {
			t.Errorf("element %v: orderColumnIndex %v, want 0", index, element.OrderColumnIndex)
		}
		if element.OrderDirection == nil || *element.OrderDirection != wantDirections[index] {
			t.Errorf("element %v: orderDirection %v, want %v", index, element.OrderDirection, wantDirections[index])
		}
		// No server-side aggregation: the difference is taken locally because
		// difference-last did not agree with the readings at the window's ends.
		if element.GroupTime != nil {
			t.Errorf("element %v: groupTime %v, want none", index, *element.GroupTime)
		}
		if element.Columns[0].GroupType != nil {
			t.Errorf("element %v: groupType %v, want none", index, *element.Columns[0].GroupType)
		}
		if element.Time == nil || element.Time.Start == nil || element.Time.End == nil {
			t.Fatalf("element %v: no time range", index)
		}
		if *element.Time.Start != testWindow.Start.Format(time.RFC3339) {
			t.Errorf("element %v: start %q", index, *element.Time.Start)
		}
		if *element.Time.End != testWindow.End.Format(time.RFC3339) {
			t.Errorf("element %v: end %q", index, *element.Time.End)
		}
	}
}

// TestDifferenceNotSum is the arithmetic itself: a cumulative counter read at
// both ends, and the distance between them.
func TestDifferenceNotSum(t *testing.T) {
	client := &fakeClient{answer: fromMeter(meter{
		meterKey("device-1", "energy.value", wrapper.Asc):  1000,
		meterKey("device-1", "energy.value", wrapper.Desc): 1250.5,
	})}
	result := fetch(t, client, []model.Device{
		device("device-1", column(serviceA, "energy.value", model.Electricity)),
	})

	if len(result) != 1 {
		t.Fatalf("got %v readings, want 1: %v", len(result), result)
	}
	wantValue(t, result, "device-1", model.Electricity, 250.5)
}

// TestThreePhaseSumsIntoOneReading: three columns of one carrier are three
// readings of one supply, so their differences add up.
func TestThreePhaseSumsIntoOneReading(t *testing.T) {
	client := &fakeClient{answer: fromMeter(meter{
		meterKey("device-1", "phase1.energy", wrapper.Asc):  100,
		meterKey("device-1", "phase1.energy", wrapper.Desc): 110,
		meterKey("device-1", "phase2.energy", wrapper.Asc):  200,
		meterKey("device-1", "phase2.energy", wrapper.Desc): 220,
		meterKey("device-1", "phase3.energy", wrapper.Asc):  300,
		meterKey("device-1", "phase3.energy", wrapper.Desc): 330,
	})}
	devices := []model.Device{
		device("device-1",
			column(serviceA, "phase1.energy", model.Electricity),
			column(serviceA, "phase2.energy", model.Electricity),
			column(serviceA, "phase3.energy", model.Electricity)),
	}

	result := fetch(t, client, devices)

	if len(client.calls[0]) != 6 {
		t.Errorf("got %v request elements, want 6", len(client.calls[0]))
	}
	if len(result) != 1 {
		t.Fatalf("got %v readings, want 1: %v", len(result), result)
	}
	// 10 + 20 + 30, not the largest phase and not an average of them.
	wantValue(t, result, "device-1", model.Electricity, 60)
}

// TestPartialThreePhase: one phase that reported at only one end contributes
// nothing, but the carrier still has a reading from the phases that did.
func TestPartialThreePhase(t *testing.T) {
	client := &fakeClient{answer: fromMeter(meter{
		meterKey("device-1", "phase1.energy", wrapper.Asc):  100,
		meterKey("device-1", "phase1.energy", wrapper.Desc): 110,
		meterKey("device-1", "phase2.energy", wrapper.Desc): 220,
	})}
	devices := []model.Device{
		device("device-1",
			column(serviceA, "phase1.energy", model.Electricity),
			column(serviceA, "phase2.energy", model.Electricity)),
	}

	result := fetch(t, client, devices)
	// 10 from the complete phase; the incomplete one adds neither 220 nor a 0.
	wantValue(t, result, "device-1", model.Electricity, 10)
}

// TestTwoCarriersOnOneDevice: a combined heat and power unit reads the gas
// going in and the electricity coming out. Two readings, not one sum.
func TestTwoCarriersOnOneDevice(t *testing.T) {
	client := &fakeClient{answer: fromMeter(meter{
		meterKey("chp", "gas.volume", wrapper.Asc):          500,
		meterKey("chp", "gas.volume", wrapper.Desc):         800,
		meterKey("chp", "electricity.energy", wrapper.Asc):  40,
		meterKey("chp", "electricity.energy", wrapper.Desc): 90,
	})}
	devices := []model.Device{
		device("chp",
			column(serviceA, "gas.volume", model.Gas),
			column(serviceB, "electricity.energy", model.Electricity)),
	}

	result := fetch(t, client, devices)

	if len(result) != 2 {
		t.Fatalf("got %v readings, want 2: %v", len(result), result)
	}
	wantValue(t, result, "chp", model.Gas, 300)
	wantValue(t, result, "chp", model.Electricity, 50)
}

// TestNullRowIsSkipped: a null is not a zero, so the row is stepped over and
// the next numeric one is read.
func TestNullRowIsSkipped(t *testing.T) {
	client := &fakeClient{answer: func(elements []RequestElement) ([]ResponseElement, error) {
		result := []ResponseElement{}
		for index, element := range elements {
			if *element.OrderDirection == wrapper.Asc {
				result = append(result, responseOf(index, row(nil), row(42.0)))
			} else {
				result = append(result, responseOf(index, row(100.0)))
			}
		}
		return result, nil
	}}

	result := fetch(t, client, []model.Device{
		device("device-1", column(serviceA, "energy.value", model.Electricity)),
	})
	// 100 - 42, and emphatically not 100 - 0.
	wantValue(t, result, "device-1", model.Electricity, 58)
}

// TestSilentDeviceHasNoEntry asserts absence, not a zero.
func TestSilentDeviceHasNoEntry(t *testing.T) {
	client := &fakeClient{answer: fromMeter(meter{
		meterKey("loud", "energy.value", wrapper.Asc):  10,
		meterKey("loud", "energy.value", wrapper.Desc): 30,
	})}
	devices := []model.Device{
		device("loud", column(serviceA, "energy.value", model.Electricity)),
		device("silent", column(serviceA, "energy.value", model.Electricity)),
	}

	result := fetch(t, client, devices)

	if len(result) != 1 {
		t.Fatalf("got %v readings, want 1: %v", len(result), result)
	}
	wantValue(t, result, "loud", model.Electricity, 20)
	wantAbsent(t, result, "silent", model.Electricity)
}

// TestOneEndOnlyHasNoEntry: a difference needs both ends. A meter that
// reported once in the window has no distance to state, which is not the same
// as having used nothing.
func TestOneEndOnlyHasNoEntry(t *testing.T) {
	for _, test := range []struct {
		name      string
		direction wrapper.Direction
	}{
		{"only the oldest row", wrapper.Asc},
		{"only the newest row", wrapper.Desc},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeClient{answer: fromMeter(meter{
				meterKey("device-1", "energy.value", test.direction): 777,
			})}
			result := fetch(t, client, []model.Device{
				device("device-1", column(serviceA, "energy.value", model.Electricity)),
			})
			if len(result) != 0 {
				t.Fatalf("got %v readings, want none: %v", len(result), result)
			}
			wantAbsent(t, result, "device-1", model.Electricity)
		})
	}
}

// TestOutOfOrderResponse: the wrapper answers from several goroutines and
// appends as they finish, so the position of a response element says nothing.
// Only requestIndex does.
func TestOutOfOrderResponse(t *testing.T) {
	readings := meter{
		meterKey("device-1", "energy.value", wrapper.Asc):  100,
		meterKey("device-1", "energy.value", wrapper.Desc): 101,
		meterKey("device-2", "energy.value", wrapper.Asc):  200,
		meterKey("device-2", "energy.value", wrapper.Desc): 220,
		meterKey("device-3", "energy.value", wrapper.Asc):  300,
		meterKey("device-3", "energy.value", wrapper.Desc): 333,
	}
	inOrder := fromMeter(readings)
	client := &fakeClient{answer: func(elements []RequestElement) ([]ResponseElement, error) {
		result, err := inOrder(elements)
		if err != nil {
			return nil, err
		}
		// Reverse, and rotate, so neither position nor a stable offset could
		// pass by accident.
		reversed := []ResponseElement{}
		for index := len(result) - 1; index >= 0; index-- {
			reversed = append(reversed, result[index])
		}
		return append(reversed[3:], reversed[:3]...), nil
	}}
	devices := []model.Device{
		device("device-1", column(serviceA, "energy.value", model.Electricity)),
		device("device-2", column(serviceA, "energy.value", model.Electricity)),
		device("device-3", column(serviceA, "energy.value", model.Electricity)),
	}

	result := fetch(t, client, devices)

	wantValue(t, result, "device-1", model.Electricity, 1)
	wantValue(t, result, "device-2", model.Electricity, 20)
	wantValue(t, result, "device-3", model.Electricity, 33)
}

// TestRequestIndexOutOfRange: an index naming a question that was not asked is
// stepped over rather than trusted. Batching makes this reachable - an index
// valid for the whole list may be outside the batch it came back in.
func TestRequestIndexOutOfRange(t *testing.T) {
	client := &fakeClient{answer: func(elements []RequestElement) ([]ResponseElement, error) {
		return []ResponseElement{
			responseOf(-1, row(1.0)),
			responseOf(len(elements), row(2.0)),
			responseOf(len(elements)+99, row(3.0)),
		}, nil
	}}

	result := fetch(t, client, []model.Device{
		device("device-1", column(serviceA, "energy.value", model.Electricity)),
	})
	if len(result) != 0 {
		t.Fatalf("got %v readings, want none: %v", len(result), result)
	}
}

// TestBatching: the split is invisible in the result. Every batch size, and an
// odd one splits a column's two ends across two calls - which is only harmless
// because requestIndex is read relative to the batch it belongs to.
func TestBatching(t *testing.T) {
	readings := meter{}
	devices := []model.Device{}
	for _, id := range []string{"device-1", "device-2", "device-3"} {
		devices = append(devices, device(id, column(serviceA, "energy.value", model.Electricity)))
		readings[meterKey(id, "energy.value", wrapper.Asc)] = 100
		readings[meterKey(id, "energy.value", wrapper.Desc)] = 150
	}

	unbatched := fetch(t, &fakeClient{answer: fromMeter(readings)}, devices)
	if len(unbatched) != 3 {
		t.Fatalf("unbatched: got %v readings, want 3", len(unbatched))
	}

	for _, test := range []struct {
		batchSize int
		wantCalls int
	}{
		{1, 6}, {2, 3}, {3, 2}, {4, 2}, {5, 2}, {6, 1}, {7, 1},
	} {
		client := &fakeClient{answer: fromMeter(readings)}
		fetcher := fetcherFor(client)
		fetcher.BatchSize = test.batchSize
		result, err := fetcher.Fetch(context.Background(), devices, testWindow)
		if err != nil {
			t.Fatalf("batch size %v: unexpected error: %v", test.batchSize, err)
		}
		if len(client.calls) != test.wantCalls {
			t.Errorf("batch size %v: got %v calls, want %v", test.batchSize, len(client.calls), test.wantCalls)
		}
		for index, call := range client.calls {
			if len(call) > test.batchSize {
				t.Errorf("batch size %v: call %v carries %v elements", test.batchSize, index, len(call))
			}
		}
		if len(result) != len(unbatched) {
			t.Fatalf("batch size %v: got %v readings, want %v", test.batchSize, len(result), len(unbatched))
		}
		for key, want := range unbatched {
			got, ok := result[key]
			if !ok {
				t.Errorf("batch size %v: missing %v", test.batchSize, key)
				continue
			}
			if got != want {
				t.Errorf("batch size %v: %v got %v, want %v", test.batchSize, key, got, want)
			}
		}
	}
}

// TestBatchingKeepsElementsInOrder: a batch is a window onto the one list that
// is the mapping, so the elements of every call must be its own slice of it.
func TestBatchingKeepsElementsInOrder(t *testing.T) {
	devices := []model.Device{
		device("device-1", column(serviceA, "a.value", model.Electricity)),
		device("device-2", column(serviceA, "b.value", model.Electricity)),
	}
	client := &fakeClient{}
	fetcher := fetcherFor(client)
	fetcher.BatchSize = 3
	if _, err := fetcher.Fetch(context.Background(), devices, testWindow); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := [][]string{
		{
			meterKey("device-1", "a.value", wrapper.Asc),
			meterKey("device-1", "a.value", wrapper.Desc),
			meterKey("device-2", "b.value", wrapper.Asc),
		},
		{
			meterKey("device-2", "b.value", wrapper.Desc),
		},
	}
	if len(client.calls) != len(want) {
		t.Fatalf("got %v calls, want %v", len(client.calls), len(want))
	}
	for callIndex, call := range client.calls {
		if len(call) != len(want[callIndex]) {
			t.Fatalf("call %v: got %v elements, want %v", callIndex, len(call), len(want[callIndex]))
		}
		for index, element := range call {
			if keyOf(element) != want[callIndex][index] {
				t.Errorf("call %v element %v: got %v, want %v", callIndex, index, keyOf(element), want[callIndex][index])
			}
		}
	}
}

// TestNoCarrierColumns: a device that reads no carrier is not asked about. It
// has nothing to contribute, and an element with no columns would be rejected
// by the wrapper and take the whole batch down with it.
func TestNoCarrierColumns(t *testing.T) {
	t.Run("mixed with a device that has columns", func(t *testing.T) {
		client := &fakeClient{answer: fromMeter(meter{
			meterKey("metered", "energy.value", wrapper.Asc):  1,
			meterKey("metered", "energy.value", wrapper.Desc): 4,
		})}
		devices := []model.Device{
			device("no-columns"),
			device("metered", column(serviceA, "energy.value", model.Electricity)),
		}

		result := fetch(t, client, devices)

		if len(client.calls) != 1 {
			t.Fatalf("got %v calls, want 1", len(client.calls))
		}
		if len(client.calls[0]) != 2 {
			t.Fatalf("got %v request elements, want 2", len(client.calls[0]))
		}
		for _, element := range client.calls[0] {
			if *element.DeviceId == "no-columns" {
				t.Errorf("asked about a device with no carrier columns")
			}
		}
		wantValue(t, result, "metered", model.Electricity, 3)
	})

	t.Run("nothing to ask means no call at all", func(t *testing.T) {
		client := &fakeClient{}
		result := fetch(t, client, []model.Device{device("a"), device("b")})
		if len(client.calls) != 0 {
			t.Errorf("got %v calls, want none", len(client.calls))
		}
		if result == nil || len(result) != 0 {
			t.Errorf("got %v, want an empty map", result)
		}
	})

	t.Run("no devices at all", func(t *testing.T) {
		client := &fakeClient{}
		result := fetch(t, client, nil)
		if len(client.calls) != 0 {
			t.Errorf("got %v calls, want none", len(client.calls))
		}
		if result == nil {
			t.Errorf("got nil, want an empty map")
		}
	})
}

// TestClientErrorIsReturned: a failed call is not a set of devices that
// reported nothing. Swallowing it would restructure a graph around a network
// error.
func TestClientErrorIsReturned(t *testing.T) {
	wantErr := errors.New("wrapper unreachable")
	client := &fakeClient{answer: func(elements []RequestElement) ([]ResponseElement, error) {
		return nil, wantErr
	}}

	result, err := fetcherFor(client).Fetch(context.Background(), nil, testWindow)
	if err != nil {
		t.Fatalf("no elements should mean no call: %v", err)
	}
	_ = result

	result, err = fetcherFor(client).Fetch(context.Background(), []model.Device{
		device("device-1", column(serviceA, "energy.value", model.Electricity)),
	}, testWindow)
	if !errors.Is(err, wantErr) {
		t.Fatalf("got %v, want %v", err, wantErr)
	}
	if result != nil {
		t.Errorf("got %v alongside the error, want nil", result)
	}
}

// TestErrorInLaterBatch: a batch that fails after earlier ones succeeded still
// fails the whole fetch. Half the readings would be a tree built on half a
// site.
func TestErrorInLaterBatch(t *testing.T) {
	wantErr := errors.New("second call failed")
	calls := 0
	client := &fakeClient{answer: func(elements []RequestElement) ([]ResponseElement, error) {
		calls++
		if calls > 1 {
			return nil, wantErr
		}
		return []ResponseElement{responseOf(0, row(1.0)), responseOf(1, row(2.0))}, nil
	}}
	devices := []model.Device{
		device("device-1", column(serviceA, "a.value", model.Electricity)),
		device("device-2", column(serviceA, "b.value", model.Electricity)),
	}
	fetcher := fetcherFor(client)
	fetcher.BatchSize = 2

	result, err := fetcher.Fetch(context.Background(), devices, testWindow)
	if !errors.Is(err, wantErr) {
		t.Fatalf("got %v, want %v", err, wantErr)
	}
	if result != nil {
		t.Errorf("got %v alongside the error, want nil", result)
	}
}

// TestContextCancellation: the wrapper client takes no context, so
// cancellation can only be honoured between batches.
func TestContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := &fakeClient{answer: func(elements []RequestElement) ([]ResponseElement, error) {
		cancel()
		return []ResponseElement{}, nil
	}}
	devices := []model.Device{
		device("device-1", column(serviceA, "a.value", model.Electricity)),
		device("device-2", column(serviceA, "b.value", model.Electricity)),
	}
	fetcher := fetcherFor(client)
	fetcher.BatchSize = 2

	_, err := fetcher.Fetch(ctx, devices, testWindow)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want %v", err, context.Canceled)
	}
	if len(client.calls) != 1 {
		t.Errorf("got %v calls, want the second one skipped", len(client.calls))
	}
}

// TestReadingMetadata: the window a figure was measured over and when it was
// read travel with it, because the cache upstream expires on the second and
// the heuristic prints the first.
func TestReadingMetadata(t *testing.T) {
	client := &fakeClient{answer: fromMeter(meter{
		meterKey("device-1", "energy.value", wrapper.Asc):  1,
		meterKey("device-1", "energy.value", wrapper.Desc): 2,
	})}
	result := fetch(t, client, []model.Device{
		device("device-1", column(serviceA, "energy.value", model.Electricity)),
	})

	reading, ok := result[model.ReadingKey{DeviceId: "device-1", Carrier: model.Electricity}]
	if !ok {
		t.Fatalf("no reading: %v", result)
	}
	if reading.Window != testWindow {
		t.Errorf("window: got %v, want %v", reading.Window, testWindow)
	}
	if !reading.FetchedAt.Equal(fixedNow) {
		t.Errorf("fetchedAt: got %v, want %v", reading.FetchedAt, fixedNow)
	}
	if reading.DeviceId != "device-1" || reading.Carrier != model.Electricity {
		t.Errorf("key fields not filled: %+v", reading)
	}
	if reading.Key() != (model.ReadingKey{DeviceId: "device-1", Carrier: model.Electricity}) {
		t.Errorf("reading does not agree with the key it is stored under: %+v", reading)
	}
}

// TestZeroValuedFetcher: BatchSize and Now are exported, so a caller can leave
// them at zero. Neither may panic or send a batch of nothing.
func TestZeroValuedFetcher(t *testing.T) {
	client := &fakeClient{answer: fromMeter(meter{
		meterKey("device-1", "energy.value", wrapper.Asc):  1,
		meterKey("device-1", "energy.value", wrapper.Desc): 5,
	})}
	fetcher := &Fetcher{client: client, token: testToken}
	before := time.Now()

	result, err := fetcher.Fetch(context.Background(), []model.Device{
		device("device-1", column(serviceA, "energy.value", model.Electricity)),
	}, testWindow)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(client.calls) != 1 {
		t.Fatalf("got %v calls, want 1", len(client.calls))
	}
	reading := result[model.ReadingKey{DeviceId: "device-1", Carrier: model.Electricity}]
	if reading.Value != 4 {
		t.Errorf("value: got %v, want 4", reading.Value)
	}
	if reading.FetchedAt.Before(before) {
		t.Errorf("fetchedAt %v predates the call", reading.FetchedAt)
	}
}

// TestReadingOf covers the shapes a response element can carry, including the
// ones that must not be read as a number.
func TestReadingOf(t *testing.T) {
	tests := []struct {
		name    string
		element ResponseElement
		want    float64
		wantOk  bool
	}{
		{name: "no data", element: ResponseElement{}},
		{name: "empty series", element: ResponseElement{Data: [][][]interface{}{{}}}},
		{name: "empty row", element: ResponseElement{Data: [][][]interface{}{{{}}}}},
		{name: "json number", element: responseOf(0, row(json.Number("12.5"))), want: 12.5, wantOk: true},
		{name: "float", element: responseOf(0, row(3.5)), want: 3.5, wantOk: true},
		{name: "integer", element: responseOf(0, row(7)), want: 7, wantOk: true},
		{name: "numeric string", element: responseOf(0, row("8.25")), want: 8.25, wantOk: true},
		{name: "null", element: responseOf(0, row(nil))},
		{name: "empty string is not a zero", element: responseOf(0, row(""))},
		{name: "junk string", element: responseOf(0, row("n/a"))},
		{name: "boolean", element: responseOf(0, row(true))},
		{name: "not a number", element: responseOf(0, row(math.NaN()))},
		{name: "infinity", element: responseOf(0, row("Inf"))},
		{name: "first numeric row wins", element: responseOf(0, row(nil), row(1.0), row(2.0)), want: 1, wantOk: true},
		{
			name:    "second series when the first has nothing",
			element: ResponseElement{Data: [][][]interface{}{{{"t", nil}}, {{"t", 9.0}}}},
			want:    9,
			wantOk:  true,
		},
		{
			name:    "the value is the last column, not the second",
			element: ResponseElement{Data: [][][]interface{}{{{"t", "ignored", 5.0}}}},
			want:    5,
			wantOk:  true,
		},
		{
			name:    "a single column row is read as the value",
			element: ResponseElement{Data: [][][]interface{}{{{6.0}}}},
			want:    6,
			wantOk:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := readingOf(test.element)
			if ok != test.wantOk {
				t.Fatalf("ok: got %v, want %v", ok, test.wantOk)
			}
			if ok && got != test.want {
				t.Errorf("got %v, want %v", got, test.want)
			}
		})
	}
}

// TestNegativeDifference: a counter that went backwards - a meter exchange or
// a rollover - is reported as it was read. Clamping it here would hide the
// exchange behind a plausible looking zero.
func TestNegativeDifference(t *testing.T) {
	client := &fakeClient{answer: fromMeter(meter{
		meterKey("device-1", "energy.value", wrapper.Asc):  900,
		meterKey("device-1", "energy.value", wrapper.Desc): 100,
	})}
	result := fetch(t, client, []model.Device{
		device("device-1", column(serviceA, "energy.value", model.Electricity)),
	})
	wantValue(t, result, "device-1", model.Electricity, -800)
}

// TestDuplicateResponseIndex: two response elements for one question - which
// the wrapper produces for a criteria matching several paths - must not double
// count, and the outcome must not depend on which goroutine appended first.
func TestDuplicateResponseIndex(t *testing.T) {
	client := &fakeClient{answer: func(elements []RequestElement) ([]ResponseElement, error) {
		result := []ResponseElement{}
		for index, element := range elements {
			value := 100.0
			if *element.OrderDirection == wrapper.Desc {
				value = 160.0
			}
			result = append(result, responseOf(index, row(value)), responseOf(index, row(value*2)))
		}
		return result, nil
	}}

	result := fetch(t, client, []model.Device{
		device("device-1", column(serviceA, "energy.value", model.Electricity)),
	})
	wantValue(t, result, "device-1", model.Electricity, 60)
}

// The same physical value annotated on two services of one device type - a
// getter service and an event service is the usual pattern - must be asked
// about once. Summing both differences would report twice what the meter
// counted, and downstream that is a wrong tree rather than a wrong number: the
// device sorts ahead of its real parent and becomes a parent itself.
func TestSameColumnOnTwoServicesIsAskedOnce(t *testing.T) {
	device := model.Device{
		Id: "d1",
		Columns: []model.CarrierColumn{
			{ServiceId: "getter", Name: "reading.energy", Carrier: model.Electricity},
			{ServiceId: "event", Name: "reading.energy", Carrier: model.Electricity},
		},
	}
	window := model.Window{Start: time.Unix(0, 0).UTC(), End: time.Unix(3600, 0).UTC()}

	client := &countingClient{first: 1000, last: 1050}
	fetcher := New(client, "token")
	fetcher.Now = func() time.Time { return time.Unix(4000, 0).UTC() }

	readings, err := fetcher.Fetch(context.Background(), []model.Device{device}, window)
	if err != nil {
		t.Fatal(err)
	}
	// Two elements, not four: one column, both ends of the window.
	if client.elements != 2 {
		t.Errorf("expected 2 request elements, got %v", client.elements)
	}
	reading, known := readings[model.ReadingKey{DeviceId: "d1", Carrier: model.Electricity}]
	if !known {
		t.Fatal("no reading")
	}
	if reading.Value != 50 {
		t.Errorf("expected the meter's own 50, got %v", reading.Value)
	}
}

// Two genuinely different columns of one carrier - the three-phase case - are
// still both asked and still summed. Deduplication is by column path, not by
// carrier.
func TestDistinctColumnsOfOneCarrierAreStillSummed(t *testing.T) {
	device := model.Device{
		Id: "d1",
		Columns: []model.CarrierColumn{
			{ServiceId: "s", Name: "l1.energy", Carrier: model.Electricity},
			{ServiceId: "s", Name: "l2.energy", Carrier: model.Electricity},
		},
	}
	window := model.Window{Start: time.Unix(0, 0).UTC(), End: time.Unix(3600, 0).UTC()}

	client := &countingClient{first: 1000, last: 1050}
	fetcher := New(client, "token")
	fetcher.Now = func() time.Time { return time.Unix(4000, 0).UTC() }

	readings, err := fetcher.Fetch(context.Background(), []model.Device{device}, window)
	if err != nil {
		t.Fatal(err)
	}
	if client.elements != 4 {
		t.Errorf("expected 4 request elements, got %v", client.elements)
	}
	if got := readings[model.ReadingKey{DeviceId: "d1", Carrier: model.Electricity}].Value; got != 100 {
		t.Errorf("expected both phases summed to 100, got %v", got)
	}
}

// countingClient answers every element with the same pair of readings and
// counts how many it was asked.
type countingClient struct {
	elements    int
	first, last float64
}

func (this *countingClient) GetQueriesV2(_ string, requestElements []RequestElement, _ *Options) ([]ResponseElement, int, error) {
	this.elements += len(requestElements)
	result := []ResponseElement{}
	for i, element := range requestElements {
		value := this.first
		if element.OrderDirection != nil && *element.OrderDirection == wrapper.Desc {
			value = this.last
		}
		result = append(result, ResponseElement{
			RequestIndex: i,
			DeviceId:     element.DeviceId,
			Data:         [][][]interface{}{{{"2026-01-01T00:00:00Z", value}}},
		})
	}
	return result, 200, nil
}
