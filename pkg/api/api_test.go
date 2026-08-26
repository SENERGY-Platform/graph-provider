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

package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type recorder struct {
	all    int
	groups []string
}

func (this *recorder) RequestAll()              { this.all++ }
func (this *recorder) RequestGroup(path string) { this.groups = append(this.groups, path) }

type fixedStatus struct{ status Status }

func (this fixedStatus) Status() Status { return this.status }

func newTestEngine(t *testing.T, requester Requester, status StatusProvider) http.Handler {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine, err := New(logger, requester, status, Config{AccessLog: true})
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

func do(t *testing.T, engine http.Handler, method string, target string) *httptest.ResponseRecorder {
	t.Helper()
	response := httptest.NewRecorder()
	engine.ServeHTTP(response, httptest.NewRequest(method, target, nil))
	return response
}

func TestHealthIsPlainOk(t *testing.T) {
	engine := newTestEngine(t, &recorder{}, fixedStatus{})
	if got := do(t, engine, http.MethodGet, "/health").Code; got != http.StatusOK {
		t.Errorf("health: %v", got)
	}
}

func TestInfoReportsTheKillSwitchAndTheQueue(t *testing.T) {
	engine := newTestEngine(t, &recorder{}, fixedStatus{status: Status{
		Writable:   false,
		Pending:    7,
		Groups:     3,
		KafkaAlive: true,
	}})

	response := do(t, engine, http.MethodGet, "/info")
	if response.Code != http.StatusOK {
		t.Fatalf("info: %v", response.Code)
	}
	var got Status
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	// Whether writing is on is the first thing anyone wondering why no graph
	// appeared needs to see.
	if got.Writable {
		t.Error("writable must be reported as false")
	}
	if got.Pending != 7 || got.Groups != 3 {
		t.Errorf("queue and cache sizes: %+v", got)
	}
	if !got.KafkaAlive {
		t.Error("kafka_alive must be reported as true")
	}
}

// A service whose trigger consumer has ended still answers every probe, so
// /info is the only place the difference shows. If this stops being reported
// the degraded state becomes invisible again.
func TestInfoReportsALostKafkaConsumer(t *testing.T) {
	engine := newTestEngine(t, &recorder{}, fixedStatus{status: Status{KafkaAlive: false}})

	response := do(t, engine, http.MethodGet, "/info")
	if response.Code != http.StatusOK {
		t.Fatalf("info: %v", response.Code)
	}
	if body := response.Body.String(); !strings.Contains(body, `"kafka_alive":false`) {
		t.Errorf("info must report kafka_alive false, got %v", body)
	}
}

// Accepted, not OK: a pass touches four foreign systems, so the request records
// the wish and returns. An operator waiting for the pass would be waiting on a
// timeout.
func TestReconcileIsAcceptedAndDoesNotWait(t *testing.T) {
	requester := &recorder{}
	engine := newTestEngine(t, requester, fixedStatus{})

	if got := do(t, engine, http.MethodPost, "/reconcile").Code; got != http.StatusAccepted {
		t.Errorf("expected 202, got %v", got)
	}
	if requester.all != 1 {
		t.Errorf("expected one full request, got %v", requester.all)
	}
}

func TestReconcileGroupRequiresAPath(t *testing.T) {
	requester := &recorder{}
	engine := newTestEngine(t, requester, fixedStatus{})

	if got := do(t, engine, http.MethodPost, "/reconcile/group").Code; got != http.StatusBadRequest {
		t.Errorf("expected 400 without a path, got %v", got)
	}
	if len(requester.groups) != 0 {
		t.Error("no group may be requested when the path is missing")
	}

	response := do(t, engine, http.MethodPost, "/reconcile/group?path=%2Facme%2Fwerk-nord")
	if response.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %v", response.Code)
	}
	// The path arrives decoded, slashes and all - group paths are the identity
	// this service works with.
	if len(requester.groups) != 1 || requester.groups[0] != "/acme/werk-nord" {
		t.Errorf("expected the decoded group path, got %+v", requester.groups)
	}
}

func TestUnknownRouteAndWrongMethod(t *testing.T) {
	engine := newTestEngine(t, &recorder{}, fixedStatus{})
	if got := do(t, engine, http.MethodGet, "/nothing").Code; got != http.StatusNotFound {
		t.Errorf("expected 404, got %v", got)
	}
	if got := do(t, engine, http.MethodGet, "/reconcile").Code; got != http.StatusNotFound && got != http.StatusMethodNotAllowed {
		t.Errorf("a GET on /reconcile must not trigger a pass, got %v", got)
	}
}

// The recovery middleware has to be in place: a status provider that panics
// must produce a 500, not take the process down with it.
func TestAPanicBecomesAnError(t *testing.T) {
	engine := newTestEngine(t, &recorder{}, panicStatus{})
	if got := do(t, engine, http.MethodGet, "/info").Code; got != http.StatusInternalServerError {
		t.Errorf("expected 500 from the recovery handler, got %v", got)
	}
}

type panicStatus struct{}

func (panicStatus) Status() Status { panic("boom") }
