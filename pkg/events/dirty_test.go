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
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestPendingDeduplicates(t *testing.T) {
	pending := NewPending()
	trigger := Trigger{Kind: KindDevice, Id: "device-1"}
	for i := 0; i < 10; i++ {
		pending.Add(trigger)
	}
	if got := pending.Len(); got != 1 {
		t.Fatalf("Len() = %d, want 1", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got := pending.Take(ctx)
	want := []Trigger{trigger}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Take() = %#v, want %#v", got, want)
	}
	if got := pending.Len(); got != 0 {
		t.Fatalf("Len() after Take() = %d, want 0", got)
	}
}

func TestPendingTakeOrderIsDeterministic(t *testing.T) {
	pending := NewPending()
	// Added out of order on purpose.
	pending.Add(Trigger{Kind: KindGraph, Id: "b"})
	pending.Add(Trigger{Kind: KindDevice, Id: "z"})
	pending.Add(Trigger{Kind: KindDevice, Id: "a"})
	pending.Add(Trigger{Kind: KindDeviceType, Id: "m"})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got := pending.Take(ctx)
	want := []Trigger{
		{Kind: KindDevice, Id: "a"},
		{Kind: KindDevice, Id: "z"},
		{Kind: KindDeviceType, Id: "m"},
		{Kind: KindGraph, Id: "b"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Take() = %#v, want %#v", got, want)
	}
}

func TestPendingTakeBlocksUntilAdd(t *testing.T) {
	pending := NewPending()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan []Trigger, 1)
	go func() {
		done <- pending.Take(ctx)
	}()

	select {
	case got := <-done:
		t.Fatalf("Take() returned %#v before any Add()", got)
	case <-time.After(100 * time.Millisecond):
		// Expected: nothing pending yet, Take is still blocked.
	}

	trigger := Trigger{Kind: KindDevice, Id: "device-1"}
	pending.Add(trigger)

	select {
	case got := <-done:
		want := []Trigger{trigger}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("Take() = %#v, want %#v", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("Take() did not return after Add()")
	}
}

func TestPendingTakeReturnsNilOnDoneContext(t *testing.T) {
	pending := NewPending()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan []Trigger, 1)
	go func() {
		done <- pending.Take(ctx)
	}()

	select {
	case got := <-done:
		if got != nil {
			t.Fatalf("Take() = %#v, want nil", got)
		}
	case <-time.After(time.Second):
		t.Fatal("Take() did not return promptly on a done context")
	}
}

func TestPendingConcurrent(t *testing.T) {
	pending := NewPending()
	const goroutines = 20
	const perGoroutine = 100

	var wg sync.WaitGroup
	stop := make(chan struct{})

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				pending.Add(Trigger{Kind: KindDevice, Id: "device"})
			}
		}(g)
	}

	// A concurrent reader draining the set while producers are still adding,
	// to exercise Add and Take racing against each other.
	var takeWg sync.WaitGroup
	takeWg.Add(1)
	go func() {
		defer takeWg.Done()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		for {
			select {
			case <-stop:
				return
			default:
				takeCtx, takeCancel := context.WithTimeout(ctx, 50*time.Millisecond)
				pending.Take(takeCtx)
				takeCancel()
			}
		}
	}()

	wg.Wait()
	close(stop)
	takeWg.Wait()

	// Whatever is left over must still be a well-formed, deduplicated set.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got := pending.Take(ctx)
	if len(got) > 1 {
		t.Fatalf("Take() = %#v, want at most the one distinct trigger ever added", got)
	}
}
