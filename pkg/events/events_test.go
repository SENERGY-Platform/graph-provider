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

package events

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/SENERGY-Platform/graph-provider/pkg/config"
	"github.com/SENERGY-Platform/service-commons/pkg/kafka"
)

const deviceId = "urn:infai:ses:device:11111111-1111-1111-1111-111111111111"

// putDeviceMessage mirrors device-repository's publisher.DeviceCommand for a
// PUT, full device body included, the way it actually appears on the wire.
const putDeviceMessage = `{
	"command": "PUT",
	"id": "urn:infai:ses:device:11111111-1111-1111-1111-111111111111",
	"device": {
		"id": "urn:infai:ses:device:11111111-1111-1111-1111-111111111111",
		"local_id": "meter-1",
		"name": "Main Meter",
		"device_type_id": "urn:infai:ses:device-type:22222222-2222-2222-2222-222222222222",
		"attributes": [
			{"key": "shared/something", "value": "true"}
		]
	}
}`

// deleteDeviceMessage mirrors the same DeviceCommand shape for a DELETE. The
// device body is still attached, the way the publisher sends it.
const deleteDeviceMessage = `{
	"command": "DELETE",
	"id": "urn:infai:ses:device:11111111-1111-1111-1111-111111111111",
	"device": {
		"id": "urn:infai:ses:device:11111111-1111-1111-1111-111111111111",
		"local_id": "meter-1",
		"name": "Main Meter",
		"device_type_id": "urn:infai:ses:device-type:22222222-2222-2222-2222-222222222222"
	}
}`

// rightsMessage mirrors permissions-v2's kafka.Command for a RIGHTS message,
// full rights object included. keycloak_groups_rights deliberately holds a
// value that is not a Right object: if anything in this package tried to
// decode Rights, this would fail to unmarshal. It does not, because nothing
// here ever reads it.
const rightsMessage = `{
	"command": "RIGHTS",
	"id": "urn:infai:ses:device:11111111-1111-1111-1111-111111111111",
	"owner": "user-1",
	"rights": {
		"user_rights": {
			"user-1": {"read": true, "write": true, "execute": true, "administrate": true}
		},
		"group_rights": {},
		"keycloak_groups_rights": {
			"/group-a": "not a Right object"
		}
	}
}`

func TestHandleDevicePut(t *testing.T) {
	pending := NewPending()
	if _, err := handle(KindDevice, []byte(putDeviceMessage), pending); err != nil {
		t.Fatalf("handle() error = %v, want nil", err)
	}
	assertOnlyPending(t, pending, Trigger{Kind: KindDevice, Id: deviceId})
}

func TestHandleDeviceDelete(t *testing.T) {
	pending := NewPending()
	if _, err := handle(KindDevice, []byte(deleteDeviceMessage), pending); err != nil {
		t.Fatalf("handle() error = %v, want nil", err)
	}
	assertOnlyPending(t, pending, Trigger{Kind: KindDevice, Id: deviceId})
}

func TestHandleRights(t *testing.T) {
	pending := NewPending()
	if _, err := handle(KindDevice, []byte(rightsMessage), pending); err != nil {
		t.Fatalf("handle() error = %v, want nil", err)
	}
	assertOnlyPending(t, pending, Trigger{Kind: KindDevice, Id: deviceId})
}

func TestHandleDeviceType(t *testing.T) {
	pending := NewPending()
	message := `{"command": "PUT", "id": "urn:infai:ses:device-type:22222222-2222-2222-2222-222222222222"}`
	if _, err := handle(KindDeviceType, []byte(message), pending); err != nil {
		t.Fatalf("handle() error = %v, want nil", err)
	}
	assertOnlyPending(t, pending, Trigger{
		Kind: KindDeviceType,
		Id:   "urn:infai:ses:device-type:22222222-2222-2222-2222-222222222222",
	})
}

func TestHandleGraph(t *testing.T) {
	pending := NewPending()
	message := `{"command": "PUT", "id": "graph-1"}`
	if _, err := handle(KindGraph, []byte(message), pending); err != nil {
		t.Fatalf("handle() error = %v, want nil", err)
	}
	assertOnlyPending(t, pending, Trigger{Kind: KindGraph, Id: "graph-1"})
}

func TestHandleGarbageBytes(t *testing.T) {
	pending := NewPending()
	_, err := handle(KindDevice, []byte("not json at all"), pending)
	if err == nil {
		t.Fatal("handle() error = nil, want an error for non-JSON input")
	}
	if got := pending.Len(); got != 0 {
		t.Fatalf("Len() = %d, want 0 - a garbage message must add nothing", got)
	}
}

func TestHandleEmptyId(t *testing.T) {
	pending := NewPending()
	_, err := handle(KindDevice, []byte(`{"command": "PUT", "id": ""}`), pending)
	if err == nil {
		t.Fatal("handle() error = nil, want an error for an empty id")
	}
	if got := pending.Len(); got != 0 {
		t.Fatalf("Len() = %d, want 0 - a message with no id must add nothing", got)
	}
}

func TestHandleMissingIdField(t *testing.T) {
	pending := NewPending()
	_, err := handle(KindDevice, []byte(`{"command": "PUT"}`), pending)
	if err == nil {
		t.Fatal("handle() error = nil, want an error when id is absent entirely")
	}
	if got := pending.Len(); got != 0 {
		t.Fatalf("Len() = %d, want 0", got)
	}
}

func TestTopicKinds(t *testing.T) {
	cfg := config.KafkaConfig{
		DeviceTopic:     "renamed-devices",
		DeviceTypeTopic: "renamed-device-types",
		GraphTopic:      "renamed-graphs",
	}
	kinds := topicKinds(cfg)
	want := map[string]Kind{
		"renamed-devices":      KindDevice,
		"renamed-device-types": KindDeviceType,
		"renamed-graphs":       KindGraph,
	}
	for topic, kind := range want {
		if kinds[topic] != kind {
			t.Errorf("topicKinds()[%q] = %q, want %q", topic, kinds[topic], kind)
		}
	}
	if len(kinds) != len(want) {
		t.Errorf("topicKinds() has %d entries, want %d", len(kinds), len(want))
	}
}

// assertOnlyPending fails the test unless pending holds exactly want and
// nothing else.
func assertOnlyPending(t *testing.T, pending *Pending, want Trigger) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got := pending.Take(ctx)
	if len(got) != 1 || got[0] != want {
		t.Fatalf("pending = %#v, want [%#v]", got, want)
	}
}

// recordingHandlers counts each failure kind separately: the whole point of
// Handlers is that the two are never confused for one another.
type recordingHandlers struct {
	mu            sync.Mutex
	deliveries    []Delivery
	messageErrors []error
	consumersLost []error
}

func (this *recordingHandlers) handlers() Handlers {
	return Handlers{
		OnDelivery: func(delivery Delivery) {
			this.mu.Lock()
			defer this.mu.Unlock()
			this.deliveries = append(this.deliveries, delivery)
		},
		OnMessageError: func(err error) {
			this.mu.Lock()
			defer this.mu.Unlock()
			this.messageErrors = append(this.messageErrors, err)
		},
		OnConsumerLost: func(err error) {
			this.mu.Lock()
			defer this.mu.Unlock()
			this.consumersLost = append(this.consumersLost, err)
		},
	}
}

func (this *recordingHandlers) counts() (messages int, lost int) {
	this.mu.Lock()
	defer this.mu.Unlock()
	return len(this.messageErrors), len(this.consumersLost)
}

func (this *recordingHandlers) delivered() []Delivery {
	this.mu.Lock()
	defer this.mu.Unlock()
	return append([]Delivery{}, this.deliveries...)
}

// Every record produces exactly one OnDelivery call, naming what it became.
// The callback is what tells "nothing arrived" from "something arrived and
// produced no work", so it has to fire for the records that produce no
// trigger just as much as for the ones that do.
func TestDeliveryIsReportedForEveryRecord(t *testing.T) {
	cases := []struct {
		name  string
		msg   kafka.Message
		want  Delivery
		wantH int // expected OnMessageError calls
	}{
		{
			name: "a record that becomes a trigger",
			msg: kafka.Message{
				Topic: "devices", Partition: 2, Offset: 41,
				Key: []byte(deviceId), Value: []byte(putDeviceMessage),
			},
			want: Delivery{
				Topic: "devices", Partition: 2, Offset: 41, Key: deviceId,
				Kind: KindDevice, Id: deviceId,
			},
		},
		{
			name: "a rights record, which is a trigger like any other",
			msg: kafka.Message{
				Topic: "devices", Partition: 0, Offset: 7,
				Key: []byte(deviceId + "/rights"), Value: []byte(rightsMessage),
			},
			want: Delivery{
				Topic: "devices", Partition: 0, Offset: 7, Key: deviceId + "/rights",
				Kind: KindDevice, Id: deviceId,
			},
		},
		{
			name: "a record that cannot be decoded still arrived",
			msg: kafka.Message{
				Topic: "devices", Partition: 1, Offset: 3,
				Key: []byte("whatever"), Value: []byte("not json at all"),
			},
			want: Delivery{
				Topic: "devices", Partition: 1, Offset: 3, Key: "whatever",
				Kind: KindDevice, Id: "",
			},
			wantH: 1,
		},
		{
			name: "a record on a topic this service does not know",
			msg: kafka.Message{
				Topic: "something-else", Partition: 0, Offset: 1,
				Value: []byte(putDeviceMessage),
			},
			want: Delivery{Topic: "something-else", Partition: 0, Offset: 1},
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			recorder := &recordingHandlers{}
			listener := newListener(map[string]Kind{"devices": KindDevice}, NewPending(), recorder.handlers())

			if err := listener(test.msg); err != nil {
				t.Fatalf("listener() error = %v, want nil", err)
			}
			delivered := recorder.delivered()
			if len(delivered) != 1 {
				t.Fatalf("OnDelivery called %d times, want exactly 1 per record", len(delivered))
			}
			if delivered[0] != test.want {
				t.Errorf("OnDelivery got %+v, want %+v", delivered[0], test.want)
			}
			if messages, _ := recorder.counts(); messages != test.wantH {
				t.Errorf("OnMessageError called %d times, want %d", messages, test.wantH)
			}
		})
	}
}

// Handlers is all-optional; a nil OnDelivery must not cost a panic on the
// partition.
func TestNilOnDeliveryIsOptional(t *testing.T) {
	listener := newListener(map[string]Kind{"devices": KindDevice}, NewPending(), Handlers{})
	if err := listener(kafka.Message{Topic: "devices", Value: []byte(putDeviceMessage)}); err != nil {
		t.Fatalf("listener() error = %v, want nil", err)
	}
}

func TestListenerAddsTrigger(t *testing.T) {
	pending := NewPending()
	recorder := &recordingHandlers{}
	listener := newListener(map[string]Kind{"devices": KindDevice}, pending, recorder.handlers())

	if err := listener(kafka.Message{Topic: "devices", Value: []byte(putDeviceMessage)}); err != nil {
		t.Fatalf("listener() error = %v, want nil", err)
	}
	if messages, lost := recorder.counts(); messages != 0 || lost != 0 {
		t.Fatalf("callbacks = (%d message, %d lost), want (0, 0)", messages, lost)
	}
	assertOnlyPending(t, pending, Trigger{Kind: KindDevice, Id: deviceId})
}

func TestListenerDecodeFailureIsMessageLevelOnly(t *testing.T) {
	pending := NewPending()
	recorder := &recordingHandlers{}
	consumers := &Consumers{}
	listener := newListener(map[string]Kind{"devices": KindDevice}, pending, recorder.handlers())

	// A listener error would make the library retry for ten minutes and then
	// end consumption, so a bad record must never produce one.
	if err := listener(kafka.Message{Topic: "devices", Value: []byte("not json at all")}); err != nil {
		t.Fatalf("listener() error = %v, want nil for a malformed record", err)
	}
	messages, lost := recorder.counts()
	if messages != 1 {
		t.Fatalf("OnMessageError called %d times, want 1", messages)
	}
	if lost != 0 {
		t.Fatalf("OnConsumerLost called %d times, want 0 - one bad record is not a dead consumer", lost)
	}
	if !consumers.Alive() {
		t.Fatal("Alive() = false after a malformed record, want true")
	}
	if got := pending.Len(); got != 0 {
		t.Fatalf("Len() = %d, want 0", got)
	}
}

func TestListenerUnknownTopicIsSilent(t *testing.T) {
	pending := NewPending()
	recorder := &recordingHandlers{}
	listener := newListener(map[string]Kind{"devices": KindDevice}, pending, recorder.handlers())

	if err := listener(kafka.Message{Topic: "something-else", Value: []byte(putDeviceMessage)}); err != nil {
		t.Fatalf("listener() error = %v, want nil", err)
	}
	if messages, lost := recorder.counts(); messages != 0 || lost != 0 {
		t.Fatalf("callbacks = (%d message, %d lost), want (0, 0)", messages, lost)
	}
	if got := pending.Len(); got != 0 {
		t.Fatalf("Len() = %d, want 0", got)
	}
}

func TestConsumerLostFlipsLiveness(t *testing.T) {
	recorder := &recordingHandlers{}
	consumers := &Consumers{}
	if !consumers.Alive() {
		t.Fatal("Alive() = false before anything happened, want true")
	}

	want := errors.New("while committing consumption: devices rebalance in progress")
	consumers.libraryOnError(context.Background(), recorder.handlers())(want)

	if consumers.Alive() {
		t.Fatal("Alive() = true after the consumer was reported lost, want false")
	}
	messages, lost := recorder.counts()
	if messages != 0 {
		t.Fatalf("OnMessageError called %d times, want 0 - the consumer is gone, no record is at fault", messages)
	}
	if lost != 1 {
		t.Fatalf("OnConsumerLost called %d times, want 1", lost)
	}
	if got := recorder.consumersLost[0]; got != want {
		t.Fatalf("OnConsumerLost got %v, want %v", got, want)
	}
}

func TestConsumerLostAfterShutdownIsNotAFailure(t *testing.T) {
	recorder := &recordingHandlers{}
	consumers := &Consumers{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// The library commits with the same context it consumes with, so a
	// shutdown between fetch and commit reaches OnError. Paging on that would
	// page on every clean SIGTERM.
	consumers.libraryOnError(ctx, recorder.handlers())(errors.New("while committing consumption: devices context canceled"))

	if _, lost := recorder.counts(); lost != 0 {
		t.Fatalf("OnConsumerLost called %d times during shutdown, want 0", lost)
	}
	if !consumers.Alive() {
		t.Fatal("Alive() = false after a shutdown-time error, want true")
	}
}

func TestNilHandlersAreOptional(t *testing.T) {
	pending := NewPending()
	consumers := &Consumers{}
	listener := newListener(map[string]Kind{"devices": KindDevice}, pending, Handlers{})

	if err := listener(kafka.Message{Topic: "devices", Value: []byte("not json at all")}); err != nil {
		t.Fatalf("listener() error = %v, want nil", err)
	}
	consumers.libraryOnError(context.Background(), Handlers{})(errors.New("gone"))
	if consumers.Alive() {
		t.Fatal("Alive() = true, want false - liveness must not depend on a callback being set")
	}
}
