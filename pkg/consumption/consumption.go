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

// Package consumption reads from the timescale wrapper what each device used
// of each carrier over a window.
//
// Meters carry a cumulative reading, so consumption over a stretch is the
// distance between its two ends rather than anything the database can be asked
// for in one number: a sum over the rows in the range would total every reading
// ever taken, and an average would answer with a meter position. Almost
// everything else in this package follows from that - two request elements per
// column, one row each, and the subtraction happening here.
//
// The figures decide where a device is placed in the generated graph, so a
// wrong one is not a wrong label but a wrong tree. That is why the readings are
// two raw rows a reader can look up in the meter's own history, and not a
// server-side aggregate nobody can check.
package consumption

import (
	"context"
	"encoding/json"
	"math"
	"strconv"
	"time"

	"github.com/SENERGY-Platform/graph-provider/pkg/model"
	timescale "github.com/SENERGY-Platform/timescale-wrapper/pkg/client"
	wrapper "github.com/SENERGY-Platform/timescale-wrapper/pkg/model"
)

// The wrapper's own request and response types, under shorter names.
//
// Aliases rather than conversions: the types on Client have to be the ones the
// real wrapper client speaks, or the real client would not satisfy it.
type (
	RequestElement  = wrapper.QueriesRequestElement
	ResponseElement = wrapper.QueriesV2ResponseElement
	Options         = timescale.QueriesV2Options
)

// Client is the part of the timescale wrapper client this package uses.
//
// An interface so the tests need no server. It has no context parameter
// because the wrapper client has none; see Fetcher.Fetch on what that costs.
type Client interface {
	GetQueriesV2(token string, requestElements []RequestElement, options *Options) (result []ResponseElement, code int, err error)
}

// The real client is usable as a Client. Checked here rather than discovered at
// the call site, because the signature has to keep matching a foreign package.
var _ Client = (timescale.Client)(nil)

// DefaultBatchSize is how many request elements are sent in one call.
//
// One round trip per group is the point of /queries/v2 - ninety meters are one
// request rather than a hundred and eighty - but a group has no upper bound,
// and two elements per column means the body grows with the site. The batch is
// not an optimisation: it keeps one enormous request, whose size neither this
// service nor the wrapper has ever been measured at, from being built at all,
// and it bounds what a single failed call costs to the readings in it.
const DefaultBatchSize = 500

// boundLimit is how many rows one request element asks for.
//
// One, at whichever end the ordering puts first. Without a limit the wrapper
// answers with every raw row in the range, which for a meter reporting every
// minute over a 30 day window is a payload nobody wants for a single number.
const boundLimit = 1

// timeColumnIndex is the column the rows are ordered by: the first one, which
// an un-aggregated query selects as "time".
//
// It has to be sent, and sending only a direction is worse than useless. The
// wrapper's getOrderLimitString falls through to an order index of -1 when
// orderColumnIndex is absent - emitting no ORDER BY at all and dropping the
// direction it just worked out - while still applying the limit. Both ends of
// the window then come back with whatever single row the planner produced,
// which is the same row for both: every difference is exactly zero and the
// result reads as "no meter reported anything".
const timeColumnIndex = 0

// bound is which end of the window one request element reads.
type bound int

const (
	// first is the oldest row in the range, read ascending.
	first bound = iota
	// last is the newest row in the range, read descending.
	last
	boundCount
)

// bounds are the two ends of a window, in the order they are sent.
//
// Both, always. Unlike the frontend this package has no total-since-installation
// window, where the newest reading already is everything the meter has counted
// and there is nothing to subtract from.
var bounds = [boundCount]bound{first, last}

// directionOf maps an end of the window to the sort direction that puts it
// first: ascending yields the oldest row of the range, descending the newest.
var directionOf = [boundCount]wrapper.Direction{
	first: wrapper.Asc,
	last:  wrapper.Desc,
}

// Fetcher reads consumption from the timescale wrapper.
//
// Holds the token unexported: it is a bearer token good for every device on the
// platform, and an exported field would put it in reach of any marshaller that
// walks this struct.
type Fetcher struct {
	client Client
	token  string

	// BatchSize is the largest number of request elements sent in one call.
	// Zero or negative means DefaultBatchSize.
	BatchSize int

	// Now is where FetchedAt comes from. A field so a test can fix it.
	Now func() time.Time
}

// New returns a Fetcher with the defaults applied.
func New(client Client, token string) *Fetcher {
	return &Fetcher{
		client:    client,
		token:     token,
		BatchSize: DefaultBatchSize,
		Now:       time.Now,
	}
}

// ask is one question: what one column of one device read over the window.
//
// The carrier travels with the column rather than being derived again later,
// so the number and the medium it is a number of cannot come apart.
type ask struct {
	deviceId  string
	serviceId string
	column    string
	carrier   model.Carrier
}

// leg is one request element: the question it is part of, and which end of the
// window it reads.
//
// askIndex is a position in the list of asks rather than a copy of one, so the
// two elements of a stretch find each other without comparing device ids and
// column names.
type leg struct {
	askIndex int
	bound    bound
}

// ends is what came back for one ask.
//
// A stretch needs both of its ends. Kept as two values and two flags rather
// than a running total, because "read nothing" and "read zero" have to stay
// distinguishable right up to the point where the difference is taken.
type ends struct {
	value [boundCount]float64
	read  [boundCount]bool
}

// Fetch reads what each device used of each carrier over the window.
//
// A device that reported nothing has no entry. Absent is not zero: the
// structure heuristic refuses to place a device it has no number for, and
// handing it a zero would place that device at the very bottom of a tree
// instead.
//
// The context cannot reach the wrapper client, which takes none, so it is
// honoured between batches. A cancellation therefore stops the next call
// rather than the one in flight.
func (this *Fetcher) Fetch(ctx context.Context, devices []model.Device, window model.Window) (map[model.ReadingKey]model.Reading, error) {
	asks, legs, elements := this.build(devices, window)
	result := map[model.ReadingKey]model.Reading{}
	if len(elements) == 0 {
		// Nothing to ask about - a group whose devices read no carrier at all.
		// Returning here rather than sending an empty body, which the wrapper
		// has no reason to be asked to answer.
		return result, nil
	}

	read := make([]ends, len(asks))
	batchSize := this.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	for start := 0; start < len(elements); start += batchSize {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		end := start + batchSize
		if end > len(elements) {
			end = len(elements)
		}

		response, _, err := this.client.GetQueriesV2(this.token, elements[start:end], nil)
		if err != nil {
			// No partial answer: a batch that failed is not a set of devices
			// that reported nothing, and treating it as one would restructure
			// a graph around a network error.
			return nil, err
		}

		for _, element := range response {
			// requestIndex counts within the request body of this call, not
			// within the whole list - the wrapper sets it from the position in
			// the array it was handed. Batching therefore has to add the
			// batch's own offset back on, or every batch after the first would
			// be read against the questions of the first.
			if element.RequestIndex < 0 || start+element.RequestIndex >= end {
				// An index outside the batch is a wrapper answering about a
				// question it was not asked. Skipped rather than guessed at:
				// the outcome is a missing reading, which is the safe one.
				continue
			}
			leg := legs[start+element.RequestIndex]
			if read[leg.askIndex].read[leg.bound] {
				// One named column produces one response element, but a
				// criteria matching several paths does not, and the wrapper
				// appends from several goroutines. First one wins, so the
				// outcome does not depend on the order they finished in.
				continue
			}
			value, ok := readingOf(element)
			if !ok {
				continue
			}
			read[leg.askIndex].value[leg.bound] = value
			read[leg.askIndex].read[leg.bound] = true
		}
	}

	// Difference per column, sum per carrier. A device may carry several
	// columns of one carrier - a three-phase supply annotates the same
	// function once per phase - and those are three readings of one thing.
	totals := map[model.ReadingKey]float64{}
	for index, ask := range asks {
		if !read[index].read[first] || !read[index].read[last] {
			// A meter that reported once or not at all in the window. There is
			// no distance to state, and a column with no distance contributes
			// nothing - not a zero, which would drag the carrier's sum down.
			continue
		}
		key := model.ReadingKey{DeviceId: ask.deviceId, Carrier: ask.carrier}
		totals[key] += read[index].value[last] - read[index].value[first]
	}

	fetchedAt := this.now()
	for key, value := range totals {
		result[key] = model.Reading{
			DeviceId:  key.DeviceId,
			Carrier:   key.Carrier,
			Value:     value,
			Window:    window,
			FetchedAt: fetchedAt,
		}
	}
	return result, nil
}

// build turns the devices into the questions, the legs of those questions and
// the request elements that carry them.
//
// The three slices are built together and returned together: legs[i] describes
// elements[i], and the answer is read back against this very list rather than
// against one rebuilt when it arrives. A list rebuilt at that point could
// reorder - a device gained a column meanwhile - and requestIndex would then
// name a different question than the one that was asked.
func (this *Fetcher) build(devices []model.Device, window model.Window) (asks []ask, legs []leg, elements []RequestElement) {
	start := window.Start.Format(time.RFC3339)
	end := window.End.Format(time.RFC3339)

	for _, device := range devices {
		// The same physical value can be annotated on more than one service of
		// a device type - a getter service and an event service exposing the
		// same reading is the usual pattern. Both would be asked, and both
		// differences summed into one carrier, so the meter would report twice
		// what it counted. Downstream that is not a wrong number but a wrong
		// tree: the device sorts ahead of its real parent and becomes a parent
		// itself. Deduplicated by column path and carrier rather than by
		// service, because the path is what names the value.
		asked := map[string]bool{}

		// A device whose type reads no carrier is skipped entirely rather than
		// asked about with no columns: the wrapper rejects an element without
		// columns, and one such device would fail the whole batch.
		for _, column := range device.Columns {
			key := string(column.Carrier) + "\x00" + column.Name
			if asked[key] {
				continue
			}
			asked[key] = true

			askIndex := len(asks)
			asks = append(asks, ask{
				deviceId:  device.Id,
				serviceId: column.ServiceId,
				column:    column.Name,
				carrier:   column.Carrier,
			})
			for _, atBound := range bounds {
				legs = append(legs, leg{askIndex: askIndex, bound: atBound})
				elements = append(elements, element(device.Id, column, start, end, atBound))
			}
		}
	}
	return asks, legs, elements
}

// element is one request element: one column of one device at one end of the
// window.
//
// One column per element, rather than one element per device with its columns
// gathered. The wrapper would take several, but the answer is then one series
// per column inside one data field, and reading it back means trusting that
// their order matches the order they were asked in. One column per element
// makes requestIndex the whole of the mapping.
//
// No groupTime and no groupType anywhere. The wrapper will compute the
// difference itself, with a difference-last column over a groupTime of the
// window's own size, and the figures that came back did not match the two
// readings at the ends of the window they were asked about. Which of its
// bucketing decisions produces that is inside the wrapper, so it is neither
// something this service can pin down nor something it can steer from outside.
func element(deviceId string, column model.CarrierColumn, start string, end string, atBound bound) RequestElement {
	limit := boundLimit
	orderColumnIndex := timeColumnIndex
	direction := directionOf[atBound]
	return RequestElement{
		DeviceId:  &deviceId,
		ServiceId: &column.ServiceId,
		Time: &wrapper.QueriesRequestElementTime{
			Start: &start,
			End:   &end,
		},
		Columns: []wrapper.QueriesRequestElementColumn{{Name: column.Name}},
		Limit:   &limit,
		// Both of these, always. The direction alone is silently dropped - see
		// timeColumnIndex.
		OrderColumnIndex: &orderColumnIndex,
		OrderDirection:   &direction,
	}
}

// readingOf is the meter reading in one response element: the last column of
// the first row that carries a number.
//
// The last column because an un-aggregated query selects "time" first and the
// value after it. One row is all an element asks for, now that each of them
// reads one end of the window - a sum across rows would be nonsense on a
// counter, where two positions added together are neither a position nor a
// consumption.
//
// Nulls are skipped rather than read as zero. A window in which a meter never
// reported is not a window in which it used nothing, and that difference is
// what decides whether the device may be placed under a parent at all.
func readingOf(element ResponseElement) (float64, bool) {
	for _, series := range element.Data {
		for _, row := range series {
			if len(row) == 0 {
				continue
			}
			if value, ok := numberOf(row[len(row)-1]); ok {
				return value, true
			}
		}
	}
	return 0, false
}

// numberOf reads one cell as a number.
//
// Numeric columns arrive as JSON numbers, but a value the wrapper converted
// between characteristics can arrive as a string, so both are accepted. What is
// not accepted is anything that fails to parse: unlike the frontend, where
// Number("") is zero, an unreadable cell here is a cell that was not read.
// NaN and infinities go the same way - they are not meter positions, and
// subtracting them would poison a whole carrier's sum.
func numberOf(raw interface{}) (float64, bool) {
	switch value := raw.(type) {
	case nil:
		return 0, false
	case float64:
		return finite(value)
	case float32:
		return finite(float64(value))
	case int:
		return float64(value), true
	case int32:
		return float64(value), true
	case int64:
		return float64(value), true
	case json.Number:
		parsed, err := value.Float64()
		if err != nil {
			return 0, false
		}
		return finite(parsed)
	case string:
		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return 0, false
		}
		return finite(parsed)
	default:
		return 0, false
	}
}

func finite(value float64) (float64, bool) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, false
	}
	return value, true
}

// now is Fetcher.Now with a fallback, so a zero-valued Fetcher does not panic
// on a nil field.
func (this *Fetcher) now() time.Time {
	if this.Now == nil {
		return time.Now()
	}
	return this.Now()
}
