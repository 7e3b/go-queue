package queue

import (
	"context"
	"fmt"
	"sync"
)

// Queue is a concurrent, generic FIFO queue.
//
// A Queue supports multiple concurrent producers and consumers.
// Push adds an item to the queue, while Pop waits until an item is
// available or the queue is closed.
//
// The queue is unbounded and Push does not block waiting for a consumer.
//
// Calling Close stops the queue and causes blocked or subsequent
// Push and Pop operations to return an error. Items that have not
// yet been delivered when Close is called may be discarded.
//
// A Queue must not be copied after its first use.
type Queue[T any] interface {
	// Push adds input to the queue.
	//
	// Push returns an error if the queue has already been closed.
	// Push is safe to call concurrently.
	Push(T) error

	// Pop removes and returns the next available item from the queue.
	//
	// Pop blocks while the queue is empty. It returns an error when
	// the queue is closed.
	//
	// Pop is safe to call concurrently.
	Pop() (T, error)

	// Close stops the queue.
	//
	// Close is safe to call concurrently and may be called multiple
	// times. Pending items may not be delivered after Close returns.
	Close()
}

// New creates a new concurrent queue for values of type T.
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

func (q *queue[T]) Pop() (T, error) {
	select {
	case <-q.ctx.Done():
		var zero T
		err := q.ctx.Err()
		return zero, fmt.Errorf("q.ctx.Err: %w", err)
	case value := <-q.subCh:
		return value, nil
	}
}

func (q *queue[T]) Close() {
	q.mu.Lock()
	q.cancel()
	q.mu.Unlock()
	q.wg.Wait()
}
