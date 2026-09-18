package queue

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestQueue_ConcurrentPushPop(t *testing.T) {
	q := New[int]()

	const (
		producers    = 20
		itemsPerProd = 1000
		total        = producers * itemsPerProd
	)

	var pushed atomic.Int64
	var popped atomic.Int64

	var producerWG sync.WaitGroup
	var consumerWG sync.WaitGroup

	// Producers.
	producerWG.Add(producers)
	for i := 0; i < producers; i++ {
		go func(id int) {
			defer producerWG.Done()

			for j := 0; j < itemsPerProd; j++ {
				if err := q.Push(id*itemsPerProd + j); err != nil {
					t.Errorf("Push failed: %v", err)
					return
				}
				pushed.Add(1)
			}
		}(i)
	}

	// Consumers.
	consumerWG.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer consumerWG.Done()

			for {
				_, err := q.Pop()
				if err != nil {
					return
				}

				popped.Add(1)

				if popped.Load() >= total {
					return
				}
			}
		}()
	}

	producerWG.Wait()

	// Wait until every pushed item has been popped.
	deadline := time.After(5 * time.Second)

	for popped.Load() < pushed.Load() {
		select {
		case <-deadline:
			t.Fatalf(
				"timeout: pushed=%d popped=%d",
				pushed.Load(),
				popped.Load(),
			)
		default:
			time.Sleep(time.Millisecond)
		}
	}

	q.Close()
	consumerWG.Wait()

	if pushed.Load() != total {
		t.Fatalf("expected %d pushes, got %d", total, pushed.Load())
	}

	if popped.Load() != total {
		t.Fatalf("expected %d pops, got %d", total, popped.Load())
	}
}

func TestQueue_ConcurrentPush(t *testing.T) {
	q := New[int]()

	const (
		producers    = 100
		itemsPerProd = 100
		total        = producers * itemsPerProd
	)

	var wg sync.WaitGroup
	var pushed atomic.Int64

	wg.Add(producers)

	for i := 0; i < producers; i++ {
		go func(id int) {
			defer wg.Done()

			for j := 0; j < itemsPerProd; j++ {
				if err := q.Push(id*itemsPerProd + j); err != nil {
					t.Errorf("Push failed: %v", err)
					return
				}

				pushed.Add(1)
			}
		}(i)
	}

	wg.Wait()

	if got := pushed.Load(); got != total {
		t.Fatalf("expected %d pushes, got %d", total, got)
	}

	q.Close()
}

func TestQueue_ConcurrentPop(t *testing.T) {
	q := New[int]()

	const total = 10_000

	for i := 0; i < total; i++ {
		if err := q.Push(i); err != nil {
			t.Fatalf("Push failed: %v", err)
		}
	}

	var wg sync.WaitGroup
	var popped atomic.Int64

	const consumers = 100

	wg.Add(consumers)

	for i := 0; i < consumers; i++ {
		go func() {
			defer wg.Done()

			for {
				_, err := q.Pop()
				if err != nil {
					return
				}

				n := popped.Add(1)
				if n >= total {
					return
				}
			}
		}()
	}

	deadline := time.After(5 * time.Second)

	for popped.Load() < total {
		select {
		case <-deadline:
			t.Fatalf("timeout: popped=%d", popped.Load())
		default:
			time.Sleep(time.Millisecond)
		}
	}

	q.Close()
	wg.Wait()

	if got := popped.Load(); got != total {
		t.Fatalf("expected %d pops, got %d", total, got)
	}
}

func TestQueue_ConcurrentPushAndClose(t *testing.T) {
	q := New[int]()

	const producers = 100

	var wg sync.WaitGroup

	wg.Add(producers)

	for i := 0; i < producers; i++ {
		go func(id int) {
			defer wg.Done()

			for j := 0; j < 1000; j++ {
				_ = q.Push(id*1000 + j)
			}
		}(i)
	}

	// Race Close against Push.
	go func() {
		time.Sleep(time.Millisecond)
		q.Close()
	}()

	wg.Wait()

	// Close again to verify repeated Close is safe.
	q.Close()
}

func TestQueue_PushAfterClose(t *testing.T) {
	q := New[int]()

	q.Close()

	if err := q.Push(1); err == nil {
		t.Fatal("expected Push after Close to fail")
	}
}

func TestQueue_PopAfterClose(t *testing.T) {
	q := New[int]()

	q.Close()

	_, err := q.Pop()
	if err == nil {
		t.Fatal("expected Pop after Close to fail")
	}
}

func TestQueue_ConcurrentClose(t *testing.T) {
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

func TestQueue_ConcurrentPushPopAndClose(t *testing.T) {
	q := New[int]()

	const (
		producers = 50
		consumers = 50
	)

	var wg sync.WaitGroup

	// Producers.
	for i := 0; i < producers; i++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			for j := 0; j < 1000; j++ {
				_ = q.Push(id*1000 + j)

				// Force more scheduling opportunities.
				if j%10 == 0 {
					time.Sleep(time.Microsecond)
				}
			}
		}(i)
	}

	// Consumers.
	for i := 0; i < consumers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				_, err := q.Pop()
				if err != nil {
					return
				}
			}
		}()
	}

	// Race everything against Close.
	time.Sleep(time.Millisecond)
	q.Close()

	wg.Wait()
}

