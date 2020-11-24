package container2_test

import (
	"testing"

	v1 "github.com/cilium/cilium/pkg/hubble/api/v1"
	"github.com/cilium/cilium/pkg/hubble/container2"

	"github.com/stretchr/testify/require"
)

// benchmarkCapacity is the capacity used for benchmarks.
const benchmarkCapacity = 4096

type benchmarkRingBufferOptions struct {
	capacity    int
	readers     int
	slowReaders int
}

func BenchmarkRingBufferNoReaders(b *testing.B) {
	benchmarkRingBuffer(b, benchmarkRingBufferOptions{})
}

func BenchmarkRingBufferOneReader(b *testing.B) {
	benchmarkRingBuffer(b, benchmarkRingBufferOptions{
		readers: 1,
	})
}

func BenchmarkRingBufferOneSlowReader(b *testing.B) {
	benchmarkRingBuffer(b, benchmarkRingBufferOptions{
		slowReaders: 1,
	})
}

func BenchmarkRingBufferEightReaders(b *testing.B) {
	benchmarkRingBuffer(b, benchmarkRingBufferOptions{
		readers: 8,
	})
}

func BenchmarkRingBufferEightreadersEightSlowReaders(b *testing.B) {
	benchmarkRingBuffer(b, benchmarkRingBufferOptions{
		readers:     8,
		slowReaders: 8,
	})
}

func benchmarkRingBuffer(b *testing.B, options benchmarkRingBufferOptions) {
	b.ReportAllocs()

	if options.capacity == 0 {
		options.capacity = benchmarkCapacity
	}

	rb := container2.NewRingBuffer(
		container2.WithCapacity(options.capacity),
	)

	// A reader counts the events it receives over a channel and reports the
	// result over a channel.
	var cancels []func()
	makeReader := func(resultCh chan<- int, capacity int) {
		ch, cancel := rb.ReadNew(capacity)
		go func() {
			eventsReceived := 0
			for range ch {
				eventsReceived++
			}
			resultCh <- eventsReceived
		}()
		cancels = append(cancels, cancel)
	}

	// readers are created with a channel capacity of b.N ensuring that they
	// can receive all events without blocking.
	readerEventsReceived := make(chan int, options.readers)
	for i := 0; i < options.readers; i++ {
		makeReader(readerEventsReceived, b.N)
	}

	// Slow readers are created with a channel capacity of 0 meaning that they
	// will only receive events if their goroutine is reading from the channel
	// when the event is written.
	slowReaderEventsReceived := make(chan int, options.slowReaders)
	for i := 0; i < options.slowReaders; i++ {
		makeReader(slowReaderEventsReceived, 0)
	}

	event := &v1.Event{}

	b.ResetTimer()

	// Write the same event b.N times.
	for i := 0; i < b.N; i++ {
		rb.Write(event)
	}

	// Cancel all readers.
	for _, cancel := range cancels {
		cancel()
	}

	// Collect results from all readers.
	for i := 0; i < options.readers; i++ {
		require.Equal(b, b.N, <-readerEventsReceived)
	}
	close(readerEventsReceived)
	for i := 0; i < options.slowReaders; i++ {
		require.GreaterOrEqual(b, b.N, <-slowReaderEventsReceived)
	}
	close(slowReaderEventsReceived)
}
