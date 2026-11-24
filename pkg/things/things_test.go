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

package things

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	platform "github.com/SENERGY-Platform/models/go/models"
)

func TestCreateAndLinkSendWhatTheRepositoryExpects(t *testing.T) {
	var bodies []map[string]any
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer x" {
			t.Errorf("token missing on %v", request.URL)
		}
		paths = append(paths, request.Method+" "+request.URL.Path)
		body := map[string]any{}
		_ = json.NewDecoder(request.Body).Decode(&body)
		bodies = append(bodies, body)
		_ = json.NewEncoder(writer).Encode(Thing{Id: "t1", Kind: KindMeter, Name: "n"})
	}))
	defer server.Close()
	client := New(server.URL+"/", time.Second)

	thing, err, _ := client.Create(context.Background(), "Bearer x", Thing{Kind: KindMeter, Name: "n", OwnerId: "u"})
	if err != nil || thing.Id != "t1" {
		t.Fatalf("create: %v %+v", err, thing)
	}
	if _, err, _ = client.AddDeviceLink(context.Background(), "Bearer x", "t1", LinkCreate{DeviceId: "d1"}); err != nil {
		t.Fatal(err)
	}
	if paths[0] != "POST /things" || paths[1] != "POST /things/t1/device-links" {
		t.Errorf("unexpected requests %v", paths)
	}
	if bodies[0]["kind"] != "meter" || bodies[0]["owner_id"] != "u" {
		t.Errorf("unexpected thing body %v", bodies[0])
	}
	// No start, no readings, no factor: the repository reads a missing start as
	// now and a missing factor as 1.
	if len(bodies[1]) != 1 || bodies[1]["device_id"] != "d1" {
		t.Errorf("the link body is the device alone, got %v", bodies[1])
	}
	if _, err, _ = client.Create(context.Background(), "Bearer x", Thing{Id: "preset"}); err == nil {
		t.Error("a preset id must be refused before the request")
	}
}

func TestListChunksIdsAndPages(t *testing.T) {
	var queries []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		queries = append(queries, request.URL.RawQuery)
		_ = json.NewEncoder(writer).Encode([]Thing{})
	}))
	defer server.Close()
	client := New(server.URL, time.Second)

	ids := make([]string, 250)
	for i := range ids {
		ids[i] = fmt.Sprintf("t%03d", i)
	}
	if _, err, _ := client.List(context.Background(), "x", ids, nil); err != nil {
		t.Fatal(err)
	}
	if len(queries) != 3 {
		t.Errorf("250 ids are three requests of at most 100, got %v", len(queries))
	}
	for _, query := range queries {
		if strings.Contains(query, "limit") {
			t.Errorf("an id filter ignores limit and offset: %v", query)
		}
	}

	queries = nil
	if _, err, _ := client.List(context.Background(), "x", nil, []string{"d1"}); err != nil {
		t.Fatal(err)
	}
	if len(queries) != 1 || !strings.Contains(queries[0], "device_ids=d1") || !strings.Contains(queries[0], "limit=100") {
		t.Errorf("a device filter is paged, got %v", queries)
	}

	queries = nil
	if result, _, _ := client.List(context.Background(), "x", []string{}, nil); len(result) != 0 || len(queries) != 0 {
		t.Error("an empty id filter asks for nothing")
	}
}

func TestErrorCarriesStatusAndReasonButNotTheToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Error(writer, "device links overlap", http.StatusBadRequest)
	}))
	defer server.Close()
	_, err, code := New(server.URL, time.Second).AddDeviceLink(context.Background(), "Bearer secret", "t1", LinkCreate{DeviceId: "d"})
	if err == nil || code != http.StatusBadRequest || !strings.Contains(err.Error(), "overlap") {
		t.Fatalf("got %v %v", err, code)
	}
	if strings.Contains(err.Error(), "secret") {
		t.Error("the token must not reach an error")
	}
}

func TestLinkOpen(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	later, earlier := now.Add(time.Hour), now.Add(-time.Hour)
	for name, tc := range map[string]struct {
		link Link
		open bool
	}{
		"no end":       {Link{From: earlier}, true},
		"end ahead":    {Link{From: earlier, Until: &later}, true},
		"ended":        {Link{From: earlier, Until: &earlier}, false},
		"ends exactly": {Link{From: earlier, Until: &now}, false},
		"not started":  {Link{From: later}, true},
	} {
		if got := tc.link.Open(now); got != tc.open {
			t.Errorf("%v: got %v", name, got)
		}
	}
}

func TestChangeGraphSendsAtAndMode(t *testing.T) {
	var query, method, path string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		query, method, path = request.URL.RawQuery, request.Method, request.URL.Path
		_, _ = writer.Write([]byte(`{"id":"g1"}`))
	}))
	defer server.Close()
	at := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	_, err, _ := New(server.URL, time.Second).ChangeGraph(context.Background(), "x", platform.Graph{Id: "g1"}, at)
	if err != nil || method != "PUT" || path != "/graphs/g1" || !strings.Contains(query, "mode=change") || !strings.Contains(query, "at=2026-11-24T12%3A00%3A00Z") {
		t.Errorf("unexpected request %v %v ?%v (%v)", method, path, query, err)
	}
}
