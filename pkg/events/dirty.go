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
	"sort"
	"sync"
)

// Pending is a deduplicating set of work waiting to be done.
//
// Ten messages about one device between two Take calls are one Trigger: the
// set holds the same key once no matter how often Add sees it, so a backlog
// after an outage costs one pass per affected id rather than one per
// message.
type Pending struct {
	mu   sync.Mutex
	set  map[Trigger]struct{}
	wake chan struct{}
}

// NewPending returns an empty Pending set.
func NewPending() *Pending {
	return &Pending{
		set:  map[Trigger]struct{}{},
		wake: make(chan struct{}, 1),
	}
}

// Add records trigger as pending work. It never blocks: the caller is a
// Kafka handler mid-partition, and the work behind a trigger touches four
// foreign systems and can take seconds. A handler that waited for it would
// stall everything queued behind it on the same partition.
func (this *Pending) Add(trigger Trigger) {
	this.mu.Lock()
	this.set[trigger] = struct{}{}
	this.mu.Unlock()
	select {
	case this.wake <- struct{}{}:
	default:
		// A wakeup is already pending; Take will find this trigger when it
		// next looks, so a second signal would only be dropped anyway.
	}
}

// Take blocks until at least one trigger is pending or ctx is done. On
// success it returns every trigger accumulated since the last Take and
// empties the set; on a done context it returns nil promptly. The result is
// sorted by Kind then Id so tests and logs are reproducible - the order
// itself carries no meaning.
func (this *Pending) Take(ctx context.Context) []Trigger {
	for {
		this.mu.Lock()
		if len(this.set) > 0 {
			result := make([]Trigger, 0, len(this.set))
			for trigger := range this.set {
				result = append(result, trigger)
			}
			this.set = map[Trigger]struct{}{}
			this.mu.Unlock()
			sort.Slice(result, func(i, j int) bool {
				if result[i].Kind != result[j].Kind {
					return result[i].Kind < result[j].Kind
				}
				return result[i].Id < result[j].Id
			})
			return result
		}
		this.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil
		case <-this.wake:
			// Loop back and re-check under the lock; the wakeup only means
			// something changed, not that the set is still non-empty.
		}
	}
}

// Len reports how many distinct triggers are currently pending. For tests
// and diagnostics; nothing in this package's own logic depends on it.
func (this *Pending) Len() int {
	this.mu.Lock()
	defer this.mu.Unlock()
	return len(this.set)
}
