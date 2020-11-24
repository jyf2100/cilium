// Copyright 2020 Authors of Hubble
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

// +build !privileged_tests

// FIXME add test for Write during RUnlock/Lock in AllEvents

package container2

import (
	"fmt"
	"testing"

	v1 "github.com/cilium/cilium/pkg/hubble/api/v1"

	"github.com/stretchr/testify/assert"
)

// testCapacities is a slice of capacities used for testing.
var testCapacities = []int{
	0,    // For empty ring buffers.
	1,    // For edge cases.
	4,    // For debugging small powers of two.
	7,    // For debugging small powers of two minus one.
	255,  // A power of two minus one.
	4096, // A larger power of two.
}

func TestNewRingBuffer(t *testing.T) {
	forEachCapacity(t, testCapacities, nil, func(t *testing.T, capacity int, b *RingBuffer) {
		assert.Equal(t, capacity, len(b.buffer))
		assert.Zero(t, b.last)
		assert.Zero(t, b.next)
		assert.Empty(t, b.Buffer(nil))
	})
}

func TestRingBufferReadAll(t *testing.T) {
	forEachCapacity(t, testCapacities, nil, func(t *testing.T, capacity int, b *RingBuffer) {
		// This test requires a non-zero capacity.
		if capacity == 0 {
			return
		}

		events := newLazyEventStream(t)
		events.fill(b)

		// Request all events. Create a buffered channel so the events can be
		// received without blocking.
		ch, cancel := b.ReadAll(3 * capacity)

		// Test that the first events received are the buffered events.
		for i := 0; i < capacity; i++ {
			assert.Equal(t, events.at(i), <-ch)
		}

		// Write the remaining events.
		for i := capacity; i < 3*capacity; i++ {
			events.writeNext(b)
		}

		// Test that the events received match all events.
		for i := capacity; i < 3*capacity; i++ {
			assert.Equal(t, events.at(i), <-ch)
		}

		// Check reader statistics.
		for _, f := range b.readers {
			assert.Equal(t, events.n(), f.sent)
			assert.Zero(t, f.dropped)
		}

		// Test that no events are received after calling cancel.
		cancel()
		events.writeNext(b)
		// requireChannelEmpty(t, ch) // FIXME

		// Check statistics.
		assert.Equal(t, events.n(), b.next, "next")
		assert.Equal(t, events.n()-capacity, b.last, "last")
		assert.Equal(t, 2*capacity, b.sent, "sent")
		assert.Equal(t, 0, b.dropped, "dropped")
	})
}

func TestRingBufferReadAllCancel(t *testing.T) {
	forEachCapacity(t, testCapacities, nil, func(t *testing.T, capacity int, b *RingBuffer) {
		// This test requires a capacity of at least a few events.
		if capacity < 4 {
			return
		}

		events := newLazyEventStream(t)
		events.fill(b)

		// Create a reader that we'll cancel while receiving the buffered
		// events.
		ch1, cancel1 := b.ReadAll(1)

		// Create a reader that we'll cancel immediately after switching to follow
		// mode.
		ch2, cancel2 := b.ReadAll(1)

		// Create a reader that we'll cancel some time after switching to follow
		// mode.
		ch3, cancel3 := b.ReadAll(1)

		// Read the first half of the buffered events.
		for i := 0; i < capacity/2; i++ {
			assert.Equal(t, events.at(i), <-ch1)
			assert.Equal(t, events.at(i), <-ch2)
			assert.Equal(t, events.at(i), <-ch3)
		}

		cancel1()
		/*
			// Cancel reader 1 and test that it no longer receives events.
			select {
			case <-ch1:
				t.Fatal("unexpected receive")
			default:
			}

			// Read the second half of the buffered events.
			for i := capacity / 2; i < capacity; i++ {
				assert.Equal(t, events[i], <-ch2)
				assert.Equal(t, events[i], <-ch3)
			}
		*/
		cancel2()
		cancel3()
	})
}

func TestRingBufferReadAllSlowReader(t *testing.T) {
	forEachCapacity(t, testCapacities, nil, func(t *testing.T, capacity int, b *RingBuffer) {
		events := newLazyEventStream(t)
		events.fill(b)

		// Request all events. Create a buffered channel so the events can be
		// received without blocking.
		ch, cancel := b.ReadAll(1)

		// Test that the first events received are the buffered events.
		for i := 0; i < capacity; i++ {
			assert.Equal(t, events.at(i), <-ch)
		}

		// Test a slow reader than only reads one event for every two written.
		for i := capacity; i < 3*capacity; i += 2 {
			events.writeNext(b)
			events.writeNext(b)
			<-ch
		}

		// Check reader statistics.
		for _, f := range b.readers {
			assert.Equal(t, 2*capacity, f.sent)
			assert.Equal(t, capacity, f.dropped)
		}

		cancel()

		// Check statistics.
		assert.Equal(t, events.n(), b.next, "next")
		assert.Equal(t, events.n()-capacity, b.last, "last")
		// assert.Equal(t, capacity, r.sent, "sent") // FIXME
		assert.Equal(t, capacity, b.dropped, "dropped")
	})
}

func TestRingBufferReadAllVerySlowReader(t *testing.T) {
	forEachCapacity(t, testCapacities, nil, func(t *testing.T, capacity int, b *RingBuffer) {
		// This test requires a non-zero capacity.
		if capacity == 0 {
			return
		}

		events := newLazyEventStream(t)
		events.fill(b)

		// Request all events. Create a buffered channel so the events can be
		// received without blocking.
		ch, cancel := b.ReadAll(1)

		// Test that the first events received are the buffered events.
		for i := 0; i < capacity; i++ {
			<-ch
		}

		// Fill the buffer again, twice, without receiving events.
		for i := 0; i < 2*capacity; i++ {
			events.writeNext(b)
		}

		<-ch
		// assert.Equal(t, events.at(2*capacity-1), <-ch) // FIXME
		// FIXME complete this test

		cancel()
	})
}

func TestRingBufferReadNewCancel(t *testing.T) {
	forEachCapacity(t, testCapacities, nil, func(t *testing.T, capacity int, b *RingBuffer) {
		events := newLazyEventStream(t)
		events.fill(b)

		// Create a buffered channel that can receive one event without blocking.
		ch, cancel := b.ReadNew(1)

		// Test that an event is received.
		event1 := events.writeNext(b)
		assert.Equal(t, event1, requireChannelReceive(t, ch))

		// Test that events that cannot be received are dropped.
		event2 := events.writeNext(b)
		b.Write(event2)
		assert.Equal(t, event2, requireChannelReceive(t, ch))

		// Check the reader's statistics.
		for _, f := range b.readers {
			assert.Equal(t, 2, f.sent)
			assert.Equal(t, 1, f.dropped)
		}

		// Test that no events are received after calling cancel.
		cancel()
		events.writeNext(b)
		// requireChannelEmpty(t, ch) // FIXME

		// Check statistics.
		assert.Equal(t, capacity+4, b.next, "next")
		assert.Equal(t, 4, b.last, "last")
		assert.Equal(t, 2, b.sent, "sent")
		assert.Equal(t, 1, b.dropped, "dropped")
	})
}

func TestRingBufferReadSince(t *testing.T) {
	forEachCapacity(t, testCapacities, nil, func(t *testing.T, capacity int, b *RingBuffer) {
		// This test requires a capacity greater than one.
		if capacity <= 1 {
			return
		}

		for _, tc := range []struct {
			name           string
			sinceIndex     int
			expectedEvents int
		}{
			{
				name:           "before_last",
				sinceIndex:     -1,
				expectedEvents: 2 * capacity,
			},
			{
				name:           "last",
				sinceIndex:     0,
				expectedEvents: 2 * capacity,
			},
			{
				name:           "second",
				sinceIndex:     2,
				expectedEvents: 2*capacity - 2,
			},
			{
				name:           "next",
				sinceIndex:     capacity,
				expectedEvents: capacity,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				b := NewRingBuffer(WithCapacity(capacity))
				events := newLazyEventStream(t)
				events.fill(b)

				// Create a reader that reads the ith event onwards.
				ch, cancel := b.ReadSince(events.time(tc.sinceIndex), 2*capacity)

				// Fill the buffer a second time.
				events.fill(b)

				// Count the number of events received.
				eventsReceived := 0
				for i := 0; i < tc.expectedEvents; i++ {
					select {
					case <-ch:
						eventsReceived++
					default:
						t.Fatalf("%d events received, expected %d", eventsReceived, tc.expectedEvents)
					}
				}
				cancel()
				requireChannelEmpty(t, ch)
				assert.Equal(t, tc.expectedEvents, eventsReceived)
			})
		}
	})
}

func TestRingBufferReadSinceZeroCapacity(t *testing.T) {
	b := NewRingBuffer()
	events := newLazyEventStream(t)
	b.Write(events.next())
	ch, cancel := b.ReadSince(events.time(0), 1)
	event := events.writeNext(b)
	assert.Equal(t, event, <-ch)
	requireChannelEmpty(t, ch)
	cancel()
}

func TestRingBufferWrite(t *testing.T) {
	forEachCapacity(t, testCapacities, nil, func(t *testing.T, capacity int, b *RingBuffer) {
		// Skip the test if the capacity is too large. This test is accidentally
		// quadratic in the buffer's capacity, i.e. the running time is
		// O(capacity^2), because it calls Buffer() (which is O(capacity))
		// O(capacity) times.
		if capacity > 1024 {
			t.Skip()
		}

		events := newLazyEventStream(t)
		var buffer []*v1.Event

		// Fill the the buffer with unique events.
		for i := 0; i < capacity; i++ {
			events.writeNext(b)
			assert.Equal(t, i+1, b.next-b.last)
			buffer = b.Buffer(buffer)
			assert.Equal(t, events.slice(0, i+1), buffer)
		}

		// Write more events to fill the buffer twice.
		for i := capacity; i < 3*capacity; i++ {
			events.writeNext(b)
			assert.Equal(t, capacity, b.next-b.last)
			buffer = b.Buffer(buffer)
			assert.Equal(t, events.lastSlice(capacity), buffer)
		}
	})
}

// forEachCapacity calls f with a new RingBuffer for each capacity.
func forEachCapacity(t *testing.T, capacities []int, options []RingBufferOption, f func(*testing.T, int, *RingBuffer)) {
	for _, capacity := range capacities {
		t.Run(fmt.Sprintf("capacity_%d", capacity), func(t *testing.T) {
			os := []RingBufferOption{
				WithCapacity(capacity),
			}
			os = append(os, options...)
			b := NewRingBuffer(os...)
			f(t, capacity, b)
		})
	}
}

// requireChannelEmpty requires that there are no more events available to read
// from ch.
func requireChannelEmpty(t *testing.T, ch <-chan *v1.Event) {
	select {
	case <-ch:
		t.Fatal("unexpected receive")
	default:
	}
}

// requireChannelReceive requires that an event can be read from ch.
func requireChannelReceive(t *testing.T, ch <-chan *v1.Event) *v1.Event {
	select {
	case event := <-ch:
		return event
	default:
		t.Fatal("no event received")
		return nil
	}
}

// withRUnlockLockFunc sets the function that is called between releasing the
// read lock and acquiring the write lock when switching a reader into follow
// mode.
func withRUnlockLockFunc(f func()) RingBufferOption {
	return func(b *RingBuffer) {
		b.rUnlockLockFunc = f
	}
}
