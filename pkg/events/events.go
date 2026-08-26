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

// Package events turns Kafka messages into triggers.
//
// It is deliberately the dumbest package in the service. Nothing here is
// derived from a message except the resource id; reconciliation re-queries the
// authoritative source for whatever the id points at. Not because a replay
// would be incomplete - the topics are compacted and kept forever - but
// because a view assembled here would be a second opinion about state this
// service does not own, and the stale one is the one nobody notices. See
// SPEC.md, "Kafka is a trigger, not a source". A handler's only job is to drop
// that id into a Pending set and return, so it never stalls a partition behind
// the four foreign systems reconciliation talks to.
package events

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/SENERGY-Platform/graph-provider/pkg/config"
	"github.com/SENERGY-Platform/service-commons/pkg/kafka"
)

// Kind is which trigger table row a message belongs to, decided by the topic
// it arrived on and nothing else.
type Kind string

const (
	KindDevice     Kind = "device"
	KindDeviceType Kind = "device-type"
	KindGraph      Kind = "graph"
)

// Trigger is one piece of pending work: re-check this id against its
// authoritative source. It never carries anything read from a message body.
type Trigger struct {
	Kind Kind
	Id   string
}

// triggerMessage is the only shape ever decoded from a message body.
//
// Command is kept only to tell a malformed message from a well-formed one;
// the trigger built from a PUT, a DELETE and a RIGHTS command is the same,
// so its value is never branched on. Id is the one field a trigger needs.
// The device, the rights and the graph body are never unmarshalled - that is
// the whole point of this package, see the package doc.
type triggerMessage struct {
	Command string `json:"command"`
	Id      string `json:"id"`
}

// handle decodes one message and adds its trigger to pending, or reports why
// it could not. It is unexported so tests can drive it with raw bytes
// without a broker.
func handle(kind Kind, value []byte, pending *Pending) error {
	var msg triggerMessage
	if err := json.Unmarshal(value, &msg); err != nil {
		return fmt.Errorf("events: %s message is not valid JSON: %w", kind, err)
	}
	if msg.Id == "" {
		return fmt.Errorf("events: %s message has no id", kind)
	}
	pending.Add(Trigger{Kind: kind, Id: msg.Id})
	return nil
}

// topicKinds maps each configured topic name to the Kind a message on it
// becomes, so a renamed topic keeps working without a code change.
func topicKinds(cfg config.KafkaConfig) map[string]Kind {
	return map[string]Kind{
		cfg.DeviceTopic:     KindDevice,
		cfg.DeviceTypeTopic: KindDeviceType,
		cfg.GraphTopic:      KindGraph,
	}
}

// Handlers are the two failure kinds of the Kafka path, kept apart because
// they are not the same event and must not be reported the same way.
//
// The distinction is the whole point of this type: a bad record costs one
// trigger, a lost consumer costs every trigger from then on.
type Handlers struct {
	// OnMessageError is called for a record that could not be used. The
	// record is skipped and consumption continues, so whatever id it named is
	// picked up by the next safety-net pass instead. A warning at most.
	OnMessageError func(error)

	// OnConsumerLost is called when a consumer goroutine has ended. Nothing
	// brings it back: from that moment the service sees no live trigger at
	// all and only the interval sweep still works, while every health probe
	// still passes. The process is expected to exit so the orchestrator
	// restarts it.
	OnConsumerLost func(error)
}

// Consumers is the liveness of what one Start call created.
//
// It answers one question - is the trigger path still there - so a dead
// consumer cannot hide behind a service that reports itself healthy.
type Consumers struct {
	// lost is set by the first consumer that ends and never cleared. With a
	// consumer group the library runs a single goroutine for all topics, so
	// there is one thing to lose; without one it runs a goroutine per
	// partition, and losing any of them means a partition is no longer read.
	// Either way "any consumer gone" is the honest answer.
	lost atomic.Bool
}

// Alive reports whether every consumer this Start call created is still
// running. The zero value is alive, so a Consumers is usable before its
// consumers have done anything.
func (this *Consumers) Alive() bool {
	return !this.lost.Load()
}

// libraryOnError adapts the library's OnError to OnConsumerLost.
//
// Every one of the library's OnError call sites is followed directly by a
// return from the consuming goroutine - a failed fetch, a listener that gave
// up after its ten minutes of retries, and a failed commit alike. So OnError
// never means "this one message went wrong"; it means consumption has ended.
// Commits are synchronous here (CommitInterval 0) and a commit fails when the
// generation ends, so an ordinary consumer-group rebalance arrives on this
// path - which is why the library's own default for it is log.Fatal.
//
// ctx is checked first. The library passes the same ctx to CommitMessages
// without guarding it against cancellation, so a shutdown between fetch and
// commit surfaces here as an error; treating that as a lost consumer would
// page somebody on every clean SIGTERM. Once ctx is done the consumer is
// meant to be gone.
func (this *Consumers) libraryOnError(ctx context.Context, handlers Handlers) func(error) {
	return func(err error) {
		if ctx.Err() != nil {
			return
		}
		this.lost.Store(true)
		if handlers.OnConsumerLost != nil {
			handlers.OnConsumerLost(err)
		}
	}
}

// newListener builds the per-message callback. It takes only what it uses so
// tests can drive both failure kinds without a broker, the way they drive
// handle with raw bytes.
func newListener(kinds map[string]Kind, pending *Pending, handlers Handlers) func(kafka.Message) error {
	return func(delivery kafka.Message) error {
		kind, ok := kinds[delivery.Topic]
		if !ok {
			// Not one of the subscribed topics; NewMultiConsumer never calls
			// this listener for anything else, so this is unreached in
			// practice and only guards against a future change in topics.
			return nil
		}
		if err := handle(kind, delivery.Value, pending); err != nil && handlers.OnMessageError != nil {
			handlers.OnMessageError(err)
		}
		// Always nil: the consumer retries a listener that returns an error
		// for ten minutes and then reports it as the end of consumption,
		// which would turn one malformed message into a dead partition. A bad
		// message is reported and skipped, never escalated.
		return nil
	}
}

// Start subscribes to the device, device-type and graph topics and returns
// once the consumers are running. They keep running until ctx is done, at
// which point they signal wg and stop. The returned Consumers answers
// whether they are still running.
//
// The error return says almost nothing about Kafka, and cannot be made to.
// NewMultiConsumer only builds a kafka-go reader and starts a goroutine; the
// reader dials lazily and reconnects internally forever, so an unreachable
// broker or a wrong address produces a nil error here and a stream of
// reconnect attempts in the library's error log. The only synchronous errors
// it has are an empty topic list and topic creation, which this service does
// not ask for. Liveness, not this error, is therefore what tells an operator
// that the trigger path is gone - and a broker that was never reached looks
// alive, because nothing has ended.
func Start(ctx context.Context, cfg config.KafkaConfig, wg *sync.WaitGroup, pending *Pending, handlers Handlers) (*Consumers, error) {
	kinds := topicKinds(cfg)
	topics := []string{cfg.DeviceTopic, cfg.DeviceTypeTopic, cfg.GraphTopic}
	consumers := &Consumers{}

	err := kafka.NewMultiConsumer(ctx, kafka.Config{
		KafkaUrl:      cfg.Url,
		ConsumerGroup: cfg.ConsumerGroup,
		Wg:            wg,
		OnError:       consumers.libraryOnError(ctx, handlers),

		// StartOffset is intentionally left unset. Committed offsets, not a
		// configured start point, decide where reading resumes: a restart
		// continues where it left off, and an additional replica joining the
		// same stable consumer group is assigned partitions rather than
		// re-reading the topic from some chosen point. Only the very first
		// run of a consumer group has no committed offset yet, and the
		// service's own startup sweep - not this package - is what covers
		// whatever came before that first run.
	}, topics, newListener(kinds, pending, handlers))
	if err != nil {
		return nil, err
	}
	return consumers, nil
}
