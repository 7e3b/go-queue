package queue

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	q := New[int]()

	if q == nil {
		t.Fatal("New returned nil")
	}

	q.Close()
}

func TestQueue_PushAndPop(t *testing.T) {
	q := New[string]()
	defer q.Close()

	if err := q.Push("hello"); err != nil {
		t.Fatalf("Push: %v", err)
	}

	if got := receive(t, q.Pop()); got != "hello" {
		t.Fatalf("expected hello, got %q", got)
	}
}

func TestQueue_FIFO(t *testing.T) {
	q := New[int]()
	defer q.Close()

	const n = 1000

	for i := 0; i < n; i++ {
		if err := q.Push(i); err != nil {
			t.Fatalf("Push(%d): %v", i, err)
		}
	}

	for i := 0; i < n; i++ {
		got := receive(t, q.Pop())

		if got != i {
			t.Fatalf("expected %d, got %d", i, got)
		}
	}
}

func TestQueue_MultiplePushes(t *testing.T) {
	q := New[int]()
	defer q.Close()

	const n = 10000

	for i := 0; i < n; i++ {
		if err := q.Push(i); err != nil {
			t.Fatalf("Push(%d): %v", i, err)
		}
	}

	for i := 0; i < n; i++ {
		got := receive(t, q.Pop())

		if got != i {
			t.Fatalf("expected %d, got %d", i, got)
		}
	}
}

func TestQueue_InterleavedPushPop(t *testing.T) {
	q := New[int]()
	defer q.Close()

	for i := 0; i < 10000; i++ {
		if err := q.Push(i); err != nil {
			t.Fatalf("Push(%d): %v", i, err)
		}

		got := receive(t, q.Pop())

		if got != i {
			t.Fatalf("expected %d, got %d", i, got)
		}
	}
}

func TestQueue_PopBlocksUntilValueAvailable(t *testing.T) {
	q := New[int]()
	defer q.Close()

	ch := q.Pop()

	select {
	case value := <-ch:
		t.Fatalf("Pop returned before Push: %v", value)

	case <-time.After(50 * time.Millisecond):
		// Expected.
	}

	if err := q.Push(42); err != nil {
		t.Fatalf("Push: %v", err)
	}

	if got := receive(t, ch); got != 42 {
		t.Fatalf("expected 42, got %d", got)
	}
}

func TestQueue_PushAfterClose(t *testing.T) {
	q := New[int]()

	q.Close()

	if err := q.Push(1); err == nil {
		t.Fatal("expected Push after Close to return an error")
	}
}

func TestQueue_PushAfterClose_DoesNotModifyQueue(t *testing.T) {
	q := New[int]()

	q.Close()

	if err := q.Push(1); err == nil {
		t.Fatal("expected Push to fail")
	}

	ch := q.Pop()

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("received value from closed queue")
		}

	case <-time.After(time.Second):
		t.Fatal("Pop did not complete after queue was closed")
	}
}

func TestQueue_PopAfterClose(t *testing.T) {
	q := New[int]()

	q.Close()

	ch := q.Pop()

	select {
	case value, ok := <-ch:
		if ok {
			t.Fatalf("expected no value, got %v", value)
		}

	case <-time.After(time.Second):
		t.Fatal("Pop did not complete after Close")
	}
}

func TestQueue_CloseUnblocksPendingPop(t *testing.T) {
	q := New[int]()

	ch := q.Pop()

	select {
	case <-ch:
		t.Fatal("Pop completed before Close")

	case <-time.After(50 * time.Millisecond):
		// Expected.
	}

	q.Close()

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected Pop channel to close without a value")
		}

	case <-time.After(time.Second):
		t.Fatal("Pop remained blocked after Close")
	}
}

func TestQueue_ManyPendingPops(t *testing.T) {
	q := New[int]()
	defer q.Close()

	const n = 1000

	channels := make([]<-chan int, n)

	for i := 0; i < n; i++ {
		channels[i] = q.Pop()
	}

	for i := 0; i < n; i++ {
		if err := q.Push(i); err != nil {
			t.Fatalf("Push(%d): %v", i, err)
		}
	}

	received := make(map[int]int, n)

	for _, ch := range channels {
		value := receive(t, ch)
		received[value]++
	}

	for i := 0; i < n; i++ {
		if received[i] != 1 {
			t.Fatalf("value %d received %d times", i, received[i])
		}
	}
}

func TestQueue_CloseUnblocksManyPendingPops(t *testing.T) {
	q := New[int]()

	const n = 1000

	channels := make([]<-chan int, n)

	for i := 0; i < n; i++ {
		channels[i] = q.Pop()
	}

	q.Close()

	for i, ch := range channels {
		select {
		case _, ok := <-ch:
			if ok {
				t.Fatalf("Pop %d received a value after Close", i)
			}

		case <-time.After(time.Second):
			t.Fatalf("Pop %d remained blocked after Close", i)
		}
	}
}

func TestQueue_MultipleProducers(t *testing.T) {
	q := New[int]()
	defer q.Close()

	const producers = 100
	const perProducer = 1000
	const total = producers * perProducer

	var wg sync.WaitGroup
	wg.Add(producers)

	for producer := 0; producer < producers; producer++ {
		producer := producer

		go func() {
			defer wg.Done()

			for i := 0; i < perProducer; i++ {
				value := producer*perProducer + i

				if err := q.Push(value); err != nil {
					t.Errorf("Push(%d): %v", value, err)
					return
				}
			}
		}()
	}

	wg.Wait()

	received := make(map[int]int, total)

	for i := 0; i < total; i++ {
		value := receive(t, q.Pop())
		received[value]++
	}

	for i := 0; i < total; i++ {
		if received[i] != 1 {
			t.Fatalf("value %d received %d times", i, received[i])
		}
	}
}

func TestQueue_MultipleConsumers(t *testing.T) {
	q := New[int]()

	const n = 10000
	const consumers = 100

	results := make(chan int, n)

	var consumerWG sync.WaitGroup
	consumerWG.Add(consumers)

	for i := 0; i < consumers; i++ {
		go func() {
			defer consumerWG.Done()

			for {
				value, ok := receiveOK(t, q.Pop())
				if !ok {
					return
				}

				results <- value
			}
		}()
	}

	for i := 0; i < n; i++ {
		if err := q.Push(i); err != nil {
			t.Fatalf("Push(%d): %v", i, err)
		}
	}

	received := make(map[int]int, n)

	for i := 0; i < n; i++ {
		select {
		case value := <-results:
			received[value]++

		case <-time.After(5 * time.Second):
			t.Fatalf("timed out after receiving %d/%d values", i, n)
		}
	}

	for i := 0; i < n; i++ {
		if received[i] != 1 {
			t.Fatalf("value %d received %d times", i, received[i])
		}
	}

	q.Close()
	consumerWG.Wait()
}

func TestQueue_MultipleProducersAndConsumers(t *testing.T) {
	q := New[int]()

	const producers = 50
	const consumers = 50
	const perProducer = 1000
	const total = producers * perProducer

	results := make(chan int, total)

	var consumerWG sync.WaitGroup
	consumerWG.Add(consumers)

	for i := 0; i < consumers; i++ {
		go func() {
			defer consumerWG.Done()

			for {
				value, ok := receiveOK(t, q.Pop())
				if !ok {
					return
				}

				results <- value
			}
		}()
	}

	var producerWG sync.WaitGroup
	producerWG.Add(producers)

	for producer := 0; producer < producers; producer++ {
		producer := producer

		go func() {
			defer producerWG.Done()

			for i := 0; i < perProducer; i++ {
				value := producer*perProducer + i

				if err := q.Push(value); err != nil {
					t.Errorf("Push(%d): %v", value, err)
					return
				}
			}
		}()
	}

	producerWG.Wait()

	received := make(map[int]int, total)

	for i := 0; i < total; i++ {
		select {
		case value := <-results:
			received[value]++

		case <-time.After(10 * time.Second):
			t.Fatalf("timed out after receiving %d/%d values", i, total)
		}
	}

	for i := 0; i < total; i++ {
		if received[i] != 1 {
			t.Fatalf("value %d received %d times", i, received[i])
		}
	}

	q.Close()
	consumerWG.Wait()
}

func TestQueue_BurstPushes(t *testing.T) {
	q := New[int]()
	defer q.Close()

	const n = 100000

	for i := 0; i < n; i++ {
		if err := q.Push(i); err != nil {
			t.Fatalf("Push(%d): %v", i, err)
		}
	}

	for i := 0; i < n; i++ {
		got := receive(t, q.Pop())

		if got != i {
			t.Fatalf("expected %d, got %d", i, got)
		}
	}
}

func TestQueue_NoLostWakeup(t *testing.T) {
	for iteration := 0; iteration < 1000; iteration++ {
		q := New[int]()

		if err := q.Push(iteration); err != nil {
			t.Fatalf("iteration %d: Push: %v", iteration, err)
		}

		got := receive(t, q.Pop())

		if got != iteration {
			t.Fatalf(
				"iteration %d: expected %d, got %d",
				iteration,
				iteration,
				got,
			)
		}

		q.Close()
	}
}

func TestQueue_PushCloseRace(t *testing.T) {
	for iteration := 0; iteration < 1000; iteration++ {
		q := New[int]()

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			_ = q.Push(iteration)
		}()

		go func() {
			defer wg.Done()
			q.Close()
		}()

		wg.Wait()
	}
}

func TestQueue_ConcurrentPushAndClose(t *testing.T) {
	for iteration := 0; iteration < 100; iteration++ {
		q := New[int]()

		const producers = 100

		var wg sync.WaitGroup
		wg.Add(producers + 1)

		var successful atomic.Int64
		var failed atomic.Int64

		for producer := 0; producer < producers; producer++ {
			go func(value int) {
				defer wg.Done()

				if err := q.Push(value); err != nil {
					failed.Add(1)
				} else {
					successful.Add(1)
				}
			}(producer)
		}

		go func() {
			defer wg.Done()
			q.Close()
		}()

		wg.Wait()

		if successful.Load()+failed.Load() != producers {
			t.Fatalf(
				"successful + failed pushes = %d, want %d",
				successful.Load()+failed.Load(),
				producers,
			)
		}
	}
}

func TestQueue_ConcurrentPopAndClose(t *testing.T) {
	for iteration := 0; iteration < 100; iteration++ {
		q := New[int]()

		const consumers = 100

		var wg sync.WaitGroup
		wg.Add(consumers + 1)

		var received atomic.Int64

		for i := 0; i < consumers; i++ {
			go func() {
				defer wg.Done()

				value, ok := receiveOK(t, q.Pop())
				_ = value

				if ok {
					received.Add(1)
				}
			}()
		}

		go func() {
			defer wg.Done()
			q.Close()
		}()

		wg.Wait()
	}
}

func TestQueue_ConcurrentPushPopClose(t *testing.T) {
	for iteration := 0; iteration < 100; iteration++ {
		q := New[int]()

		const producers = 20
		const consumers = 20
		const pushesPerProducer = 1000

		var producerWG sync.WaitGroup
		var consumerWG sync.WaitGroup

		producerWG.Add(producers)
		consumerWG.Add(consumers)

		for producer := 0; producer < producers; producer++ {
			producer := producer

			go func() {
				defer producerWG.Done()

				for i := 0; i < pushesPerProducer; i++ {
					_ = q.Push(producer*pushesPerProducer + i)
				}
			}()
		}

		for i := 0; i < consumers; i++ {
			go func() {
				defer consumerWG.Done()

				for {
					_, ok := receiveOK(t, q.Pop())

					if !ok {
						return
					}
				}
			}()
		}

		time.Sleep(time.Millisecond)

		q.Close()

		producerWG.Wait()
		consumerWG.Wait()
	}
}

func TestQueue_ManyConcurrentCloses(t *testing.T) {
	q := New[int]()

	const closers = 100

	var wg sync.WaitGroup
	wg.Add(closers)

	for i := 0; i < closers; i++ {
		go func() {
			defer wg.Done()
			q.Close()
		}()
	}

	wg.Wait()
}

func TestQueue_CloseIsSynchronous(t *testing.T) {
	q := New[int]()

	ch := q.Pop()

	done := make(chan struct{})

	go func() {
		q.Close()
		close(done)
	}()

	select {
	case <-done:
		// Close returned.

	case <-time.After(time.Second):
		t.Fatal("Close did not return")
	}

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected Pop channel to be closed")
		}

	case <-time.After(time.Second):
		t.Fatal("pending Pop was not released")
	}
}

func TestQueue_ZeroValueTypes(t *testing.T) {
	type item struct {
		ID   int
		Name string
	}

	q := New[item]()
	defer q.Close()

	expected := item{
		ID:   42,
		Name: "hello",
	}

	if err := q.Push(expected); err != nil {
		t.Fatalf("Push: %v", err)
	}

	got := receive(t, q.Pop())

	if got != expected {
		t.Fatalf("expected %+v, got %+v", expected, got)
	}
}

func TestQueue_PointerValues(t *testing.T) {
	q := New[*int]()
	defer q.Close()

	value := 42

	if err := q.Push(&value); err != nil {
		t.Fatalf("Push: %v", err)
	}

	got := receive(t, q.Pop())

	if got == nil {
		t.Fatal("received nil pointer")
	}

	if *got != value {
		t.Fatalf("expected %d, got %d", value, *got)
	}
}

func TestQueue_NilPointerValue(t *testing.T) {
	q := New[*int]()
	defer q.Close()

	if err := q.Push(nil); err != nil {
		t.Fatalf("Push: %v", err)
	}

	got := receive(t, q.Pop())

	if got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestQueue_StringValues(t *testing.T) {
	q := New[string]()
	defer q.Close()

	values := []string{
		"",
		"hello",
		"world",
		"日本語",
		"🚀",
	}

	for _, value := range values {
		if err := q.Push(value); err != nil {
			t.Fatalf("Push(%q): %v", value, err)
		}
	}

	for _, expected := range values {
		got := receive(t, q.Pop())

		if got != expected {
			t.Fatalf("expected %q, got %q", expected, got)
		}
	}
}

func TestQueue_Stress(t *testing.T) {
	const (
		iterations        = 20
		producersPerRound = 50
		consumersPerRound = 50
		itemsPerProducer  = 500
	)

	for iteration := 0; iteration < iterations; iteration++ {
		q := New[int]()

		total := producersPerRound * itemsPerProducer
		results := make(chan int, total)

		var producers sync.WaitGroup
		var consumers sync.WaitGroup

		producers.Add(producersPerRound)
		consumers.Add(consumersPerRound)

		for i := 0; i < consumersPerRound; i++ {
			go func() {
				defer consumers.Done()

				for {
					value, ok := receiveOK(t, q.Pop())

					if !ok {
						return
					}

					results <- value
				}
			}()
		}

		for producer := 0; producer < producersPerRound; producer++ {
			producer := producer

			go func() {
				defer producers.Done()

				for i := 0; i < itemsPerProducer; i++ {
					value := producer*itemsPerProducer + i

					if err := q.Push(value); err != nil {
						return
					}
				}
			}()
		}

		producers.Wait()

		received := make(map[int]int, total)

		for i := 0; i < total; i++ {
			select {
			case value := <-results:
				received[value]++

			case <-time.After(10 * time.Second):
				t.Fatalf(
					"iteration %d: timed out after receiving %d/%d",
					iteration,
					i,
					total,
				)
			}
		}

		for i := 0; i < total; i++ {
			if received[i] != 1 {
				t.Fatalf(
					"iteration %d: value %d received %d times",
					iteration,
					i,
					received[i],
				)
			}
		}

		q.Close()
		consumers.Wait()
	}
}

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()

	select {
	case value := <-ch:
		return value

	case <-time.After(time.Second):
		t.Fatal("timed out waiting for value")

		var zero T
		return zero
	}
}

func receiveOK[T any](t *testing.T, ch <-chan T) (T, bool) {
	t.Helper()

	select {
	case value, ok := <-ch:
		return value, ok

	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for channel")

		var zero T
		return zero, false
	}
}
