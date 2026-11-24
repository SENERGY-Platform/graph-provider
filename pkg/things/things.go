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

// Package things is this service's access to the things of the device
// repository.
//
// A small client of its own instead of the repository's, because the pinned
// device-repository module (v0.2.53) predates things: its client has no thing
// endpoints and its models no Thing. Bumping it would move this service to the
// repository's v3 module path and to a new models version for the sake of five
// requests. The types here carry only the fields this service reads or writes.
//
// The contract is device-repository docs/things-and-device-links.md.
package things

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	platform "github.com/SENERGY-Platform/models/go/models"
)

// KindMeter is a thing that stands for a metering point.
const KindMeter = "meter"

// ListPageSize is how many things are asked for per request. The repository
// answers 100 by default and takes no more than its own limit on a filter by
// device id, so the pages are walked.
const ListPageSize = 100

// idChunk bounds the id filter, which travels in the query string.
const idChunk = 100

// Thing is the part of a thing this service reads and writes.
type Thing struct {
	Id         string               `json:"id"`
	Kind       string               `json:"kind,omitempty"`
	Name       string               `json:"name"`
	Attributes []platform.Attribute `json:"attributes"`
	OwnerId    string               `json:"owner_id"`

	// DeviceLinks are only read: the repository ignores them on create and
	// update, links have their own endpoint.
	DeviceLinks []Link `json:"device_links,omitempty"`
}

// Link states that a device measures a thing during [From, Until).
// A nil Until is an open link.
type Link struct {
	Id       string     `json:"id"`
	DeviceId string     `json:"device_id"`
	From     time.Time  `json:"from"`
	Until    *time.Time `json:"until"`
}

// OpenAt reports whether the link holds at an instant: it has started and has
// not ended. Half-open, like the repository's own periods.
func (this Link) OpenAt(at time.Time) bool {
	if this.From.After(at) {
		return false
	}
	return this.Until == nil || this.Until.After(at)
}

// Open reports whether the link is open now or will be: it has no end, or its
// end is still ahead. A link that has not started yet counts, because the
// device is spoken for from then on.
func (this Link) Open(now time.Time) bool {
	return this.Until == nil || this.Until.After(now)
}

// LinkCreate is the body of a link creation.
//
// From is left nil by this service, which the repository reads as now. Readings
// are not known when a device is first seen, and the factor is left out, which
// the repository reads as 1.
type LinkCreate struct {
	DeviceId string     `json:"device_id"`
	From     *time.Time `json:"from,omitempty"`
	Until    *time.Time `json:"until,omitempty"`
}

// Client talks to the device repository's thing endpoints.
type Client struct {
	baseUrl string
	http    *http.Client
}

// New returns a client for a device repository at baseUrl.
func New(baseUrl string, timeout time.Duration) *Client {
	return &Client{
		baseUrl: strings.TrimRight(baseUrl, "/"),
		http:    &http.Client{Timeout: timeout},
	}
}

// List returns things by id, by linked device, or both filters; with neither it
// returns every thing the token may read. Pages are walked, and an id filter is
// cut into chunks so the query string stays short.
func (this *Client) List(ctx context.Context, token string, ids []string, deviceIds []string) ([]Thing, error, int) {
	result := []Thing{}
	if ids != nil && len(ids) == 0 {
		return result, nil, http.StatusOK
	}
	if deviceIds != nil && len(deviceIds) == 0 {
		return result, nil, http.StatusOK
	}
	for _, idPart := range chunks(ids) {
		for _, devicePart := range chunks(deviceIds) {
			for offset := 0; ; offset += ListPageSize {
				query := url.Values{}
				if idPart != nil {
					// An id filter ignores limit and offset.
					query.Set("ids", strings.Join(idPart, ","))
				} else {
					query.Set("limit", strconv.Itoa(ListPageSize))
					query.Set("offset", strconv.Itoa(offset))
				}
				if devicePart != nil {
					query.Set("device_ids", strings.Join(devicePart, ","))
				}
				query.Set("sort", "id.asc")
				page := []Thing{}
				if err, code := this.do(ctx, token, http.MethodGet, "/things?"+query.Encode(), nil, &page); err != nil {
					return nil, err, code
				}
				result = append(result, page...)
				if idPart != nil || len(page) < ListPageSize {
					break
				}
			}
		}
	}
	return result, nil, http.StatusOK
}

// chunks cuts a filter into parts; nil stays one nil part, which means no filter.
func chunks(values []string) [][]string {
	if values == nil {
		return [][]string{nil}
	}
	result := [][]string{}
	for start := 0; start < len(values); start += idChunk {
		end := min(start+idChunk, len(values))
		result = append(result, values[start:end])
	}
	return result
}

// Create stores a new thing. The id is the repository's, the thing must not
// carry one.
func (this *Client) Create(ctx context.Context, token string, thing Thing) (Thing, error, int) {
	if thing.Id != "" {
		return thing, errors.New("a new thing may not carry an id"), http.StatusBadRequest
	}
	if thing.Attributes == nil {
		thing.Attributes = []platform.Attribute{}
	}
	result := Thing{}
	err, code := this.do(ctx, token, http.MethodPost, "/things", thing, &result)
	return result, err, code
}

// AddDeviceLink links a device to a thing.
func (this *Client) AddDeviceLink(ctx context.Context, token string, thingId string, link LinkCreate) (Thing, error, int) {
	result := Thing{}
	err, code := this.do(ctx, token, http.MethodPost, "/things/"+url.PathEscape(thingId)+"/device-links", link, &result)
	return result, err, code
}

// Delete removes a thing with its links.
func (this *Client) Delete(ctx context.Context, token string, thingId string) (error, int) {
	return this.do(ctx, token, http.MethodDelete, "/things/"+url.PathEscape(thingId), nil, nil)
}

func (this *Client) do(ctx context.Context, token string, method string, path string, body any, into any) (error, int) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return err, http.StatusBadRequest
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, this.baseUrl+path, reader)
	if err != nil {
		return err, http.StatusInternalServerError
	}
	request.Header.Set("Authorization", token)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := this.http.Do(request)
	if err != nil {
		return err, http.StatusInternalServerError
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		return err, http.StatusInternalServerError
	}
	if response.StatusCode >= 300 {
		// The body is the repository's reason and carries no secret; the
		// request, which has the token, is not part of the message.
		return fmt.Errorf("unexpected status code %v on %v %v: %v",
			response.StatusCode, method, strings.SplitN(path, "?", 2)[0], strings.TrimSpace(string(payload))), response.StatusCode
	}
	if into != nil && len(payload) > 0 {
		if err = json.Unmarshal(payload, into); err != nil {
			return fmt.Errorf("unable to read the answer of %v %v: %w", method, strings.SplitN(path, "?", 2)[0], err), http.StatusInternalServerError
		}
	}
	return nil, response.StatusCode
}

// ChangeGraph writes an existing graph as a change at an instant:
// PUT /graphs/{id}?at=<at>&mode=change. The body is the edited projection at
// that instant; what it no longer holds ends there and stays in the stored
// document, where a correction would delete it. The answer is the projection at
// now.
//
// Lives here for the same reason as the rest of this package: the pinned
// repository client's SetGraph takes neither at nor mode.
func (this *Client) ChangeGraph(ctx context.Context, token string, graph platform.Graph, at time.Time) (platform.Graph, error, int) {
	if graph.Id == "" {
		return graph, errors.New("a change needs an existing graph"), http.StatusBadRequest
	}
	query := url.Values{}
	query.Set("at", at.UTC().Format(time.RFC3339Nano))
	query.Set("mode", "change")
	result := platform.Graph{}
	err, code := this.do(ctx, token, http.MethodPut, "/graphs/"+url.PathEscape(graph.Id)+"?"+query.Encode(), graph, &result)
	return result, err, code
}

// EndDeviceLink ends an open link at an instant: POST /things/{id}/device-links/{link}/end.
// No until_values: this service never writes readings.
func (this *Client) EndDeviceLink(ctx context.Context, token string, thingId string, linkId string, until time.Time) (Thing, error, int) {
	result := Thing{}
	body := map[string]time.Time{"until": until.UTC()}
	err, code := this.do(ctx, token, http.MethodPost,
		"/things/"+url.PathEscape(thingId)+"/device-links/"+url.PathEscape(linkId)+"/end", body, &result)
	return result, err, code
}
