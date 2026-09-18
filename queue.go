package queue

import (
	"context"
	"fmt"
	"sync"
)

// Queue is a concurrent, generic FIFO queue.
//
// A Queue supports multiple concurrent producers and consumers.
// Push adds values to the queue, while Pop waits for and returns
// the next available value.
//
// Values are delivered in FIFO order. When multiple consumers are
// waiting, each value is delivered to exactly one consumer.
//
// The queue is unbounded and does not apply backpressure to producers.
// Push only blocks briefly while synchronizing access to the queue's
// internal state.
//
// A Queue must be closed when it is no longer needed. Closing the
// queue releases its background processing goroutine and unblocks
// pending Pop calls.
type Queue[T any] interface {
	// Push adds a value to the back of the queue.
	//
	// Push is safe to call concurrently with other Push, Pop, and Close
	// operations. It returns an error if the queue has already been closed.
	Push(T) error

	// Pop returns a channel that receives the next available value.
	//
	// If the queue is empty, Pop waits until a value becomes available
	// or the queue is closed. The returned channel is closed when the
	// queue is closed before a value can be delivered.
	//
	// Each queued value is delivered to at most one Pop call.
	Pop() <-chan T

	// Close closes the queue and waits for its background processing
	// goroutine to exit.
	//
	// After Close returns, Push returns an error and no new values can
	// be added. Pending Pop calls are unblocked and their returned
	// channels are closed if no value has already been delivered.
	//
	// Values that have not yet been delivered when the queue is closed
	// may be discarded.
	//
	// Close is safe to call multiple times and concurrently.
	Close()
}

// New creates and starts a new concurrent FIFO queue for values of type T.
//
// The returned queue is ready for concurrent use by multiple producers
// and consumers.
func New[T any]() Queue[T] {
	return newQueue[T]()
}

type queue[T any] struct {
	ctx    context.Context
	cancel context.CancelFunc
	mu     *sync.Mutex
	store  []T
	sigCh  chan struct{}
	subCh  chan T
	wg     *sync.WaitGroup
}

func newQueue[T any]() *queue[T] {
	ctx, cancel := context.WithCancel(context.Background())
	q := &queue[T]{
		ctx:    ctx,
		cancel: cancel,
		mu:     &sync.Mutex{},
		sigCh:  make(chan struct{}, 1),
		subCh:  make(chan T),
		wg:     &sync.WaitGroup{},
	}
	q.wg.Add(1)
	go q.loop()
	return q
}

func (q *queue[T]) Push(input T) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	err := q.ctx.Err()
	if err != nil {
		return fmt.Errorf("q.ctx.Err: %w", err)
	}
	q.store = append(q.store, input)
	select {
	case q.sigCh <- struct{}{}:
	default:
	}
	return nil
}

func (q *queue[T]) loop() {
	defer q.wg.Done()
	for {
		select {
		case <-q.ctx.Done():
			return
		case <-q.sigCh:
			q.mu.Lock()
			values := q.store
			q.store = []T{}
			q.mu.Unlock()
			for _, value := range values {
				select {
				case <-q.ctx.Done():
					return
				case q.subCh <- value:
				}
			}
		}
	}
}

func (q *queue[T]) Pop() <-chan T {
	ch := make(chan T, 1)
	go func() {
		defer close(ch)
		select {
		case <-q.ctx.Done():
		case value, ok := <-q.subCh:
			if !ok {
				return
			}
			ch <- value
		}
	}()
	return ch
}

func (q *queue[T]) Close() {
	q.mu.Lock()
	q.cancel()
	q.mu.Unlock()
	q.wg.Wait()
}
