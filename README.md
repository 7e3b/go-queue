# go-queue

A small, generic, concurrency-safe FIFO queue for Go.

`go-queue` provides an unbounded in-memory queue that supports multiple concurrent producers and consumers. It uses Go generics for type safety and exposes an asynchronous `Pop` API.

## Features

* Generic and type-safe
* FIFO ordering
* Multiple concurrent producers
* Multiple concurrent consumers
* Safe concurrent `Push`, `Pop`, and `Close`
* Unbounded in-memory storage
* Asynchronous `Pop`
* Graceful shutdown of internal processing
* No external dependencies

## Installation

```bash
go get github.com/7e3b/go-queue
```

## Usage

```go
package main

import (
	"fmt"

	"github.com/7e3b/go-queue"
)

func main() {
	q := queue.New[string]()
	defer q.Close()

	if err := q.Push("hello"); err != nil {
		panic(err)
	}

	if value, ok := <-q.Pop(); ok {
		fmt.Println(value)
	}
}
```

Output:

```text
hello
```

## Multiple Producers and Consumers

A queue can safely be shared between multiple goroutines.

```go
q := queue.New[int]()
defer q.Close()

for i := 0; i < 10; i++ {
	go func(i int) {
		if err := q.Push(i); err != nil {
			// Queue may have been closed.
			return
		}
	}(i)
}

for i := 0; i < 10; i++ {
	go func() {
		value, ok := <-q.Pop()
		if !ok {
			return
		}

		fmt.Println(value)
	}()
}
```

Each queued value is delivered to at most one consumer.

## API

### `New`

```go
q := queue.New[T]()
```

Creates a new queue for values of type `T`.

The queue starts its internal processing goroutine when created and is ready for concurrent use immediately.

### `Push`

```go
err := q.Push(value)
```

Adds a value to the back of the queue.

`Push` is safe to call concurrently with other `Push`, `Pop`, and `Close` operations.

If the queue has already been closed, `Push` returns an error.

### `Pop`

```go
ch := q.Pop()

value, ok := <-ch
```

Returns a channel that receives the next available value.

If the queue is empty, the returned channel waits until a value becomes available.

If the queue is closed before a value can be delivered, the returned channel is closed.

```go
value, ok := <-q.Pop()

if !ok {
	// Queue was closed before a value was delivered.
	return
}

fmt.Println(value)
```

Each queued value is delivered to at most one `Pop` call.

### `Close`

```go
q.Close()
```

Closes the queue and waits for its internal processing goroutine to stop.

After `Close` returns:

* `Push` returns an error.
* New values cannot be added.
* Pending `Pop` calls are unblocked.
* `Pop` channels that cannot receive a value are closed.
* The internal processing goroutine has exited.

`Close` is safe to call multiple times and concurrently.

Values that have not yet been delivered when the queue is closed may be discarded.

## FIFO Ordering

Values are delivered in first-in, first-out order.

For sequential producers:

```go
q.Push(1)
q.Push(2)
q.Push(3)

<-q.Pop() // 1
<-q.Pop() // 2
<-q.Pop() // 3
```

### Concurrent Producers

When multiple goroutines call `Push` concurrently, the queue does **not** guarantee ordering based on goroutine creation order, scheduling order, or the order in which the goroutines were started.

For example:

```go
go q.Push(1)
go q.Push(2)
```

does not guarantee that `1` will be queued before `2`.

The queue's FIFO ordering applies to the order in which values are successfully added to the queue. With concurrent producers, that ordering is determined by synchronization and whichever `Push` operation enters the queue first.

Therefore:

```text
Sequential Push calls:
    Push(1) → Push(2) → Push(3)
                     ↓
                1 → 2 → 3

Concurrent Push calls:
    Push(1) ─┐
             ├──► queue ──► FIFO according to enqueue order
    Push(2) ─┘
```

### Concurrent Consumers

FIFO ordering determines the order in which values are made available by the queue, but it does not determine which consumer receives a particular value.

For example, with two consumers:

```text
Queue:       1 → 2 → 3 → 4

Consumer A ──────┐
                 ├── receives values
Consumer B ──────┘
```

The consumers may receive different values depending on scheduling.

The queue guarantees that a queued value is delivered to at most one consumer; it does not guarantee a particular distribution of values among concurrent consumers.

## Concurrency

The queue is designed for concurrent access:

```text
                 ┌─────────────┐
 Producer ──────►             │
 Producer ──────►    Queue     ├──────► Consumer
 Producer ──────►             │
                 └─────────────┘
                        │
                        ├──────────────► Consumer
                        │
                        └──────────────► Consumer
```

Multiple goroutines may call `Push`, `Pop`, and `Close` simultaneously.

The implementation synchronizes access to its internal storage and coordinates producers, consumers, and shutdown.

## Unbounded Queue

The queue does not have a fixed capacity.

Values are temporarily stored in memory until they can be delivered to consumers.

Therefore, memory usage can grow with the number of queued values.

If producer throughput consistently exceeds consumer throughput, the queue can grow without bound.

If bounded memory or producer backpressure is required, a bounded queue or semaphore-based design may be more appropriate.

## Blocking Behavior

`Push` does not wait for a consumer to receive the value.

A successful `Push` adds the value to the queue and returns.

`Pop` waits when no value is immediately available:

```text
Push                         Pop

  │                           │
  │                           ├── waiting
  │                           │
  ├──► queue                  │
  │                           │
  │                           ◄── value
  │                           │
```

This means producers and consumers operate independently.

## Shutdown

A typical application should close the queue when it is no longer needed:

```go
q := queue.New[Job]()
defer q.Close()
```

`Close` also unblocks consumers waiting in `Pop`.

For example:

```go
q := queue.New[int]()

ch := q.Pop()

q.Close()

_, ok := <-ch

// ok == false
```

Closing the queue does **not** guarantee that every value already stored in the queue will be delivered. Shutdown can interrupt the internal delivery loop, so values that have not yet been delivered may be discarded.

## No Persistence

The queue is entirely in-memory.

Values are lost when the process exits or when the queue is closed before they are delivered.

It should not be used as a durable message broker or distributed queue.

## Error Handling

The only operation that currently returns an error is `Push`.

```go
if err := q.Push(value); err != nil {
	// Queue has been closed.
}
```

The error indicates that the queue is no longer accepting values.

## Testing

Run the standard test suite:

```bash
go test ./...
```

Run with Go's race detector:

```bash
go test -race ./...
```

For repeated concurrency testing:

```bash
go test -race -count=100 ./...
```

The test suite covers:

* FIFO ordering
* Multiple producers
* Multiple consumers
* Concurrent producers and consumers
* Concurrent `Push` and `Close`
* Concurrent `Pop` and `Close`
* Concurrent `Close`
* Pending `Pop` calls
* Shutdown behavior
* Burst workloads
* Lost-wakeup scenarios
* Generic value types
* Pointer values, including `nil`

## Design

The queue consists of three main pieces:

```text
                    ┌──────────────┐
                    │    store     │
                    │    []T       │
                    └──────┬───────┘
                           │
                    synchronization
                           │
                    ┌──────▼───────┐
                    │    loop      │
                    │  goroutine   │
                    └──────┬───────┘
                           │
                          subCh
                           │
                    ┌──────▼───────┐
                    │    Pop()     │
                    │   consumer   │
                    └──────────────┘
```

`Push` appends values to the internal store and signals the background processing goroutine.

The processing goroutine moves queued values to consumers through the internal delivery channel.

`Pop` creates a consumer-facing channel and waits for either:

1. A value to become available.
2. The queue to be closed.

A buffered signal channel is used to coalesce multiple producer notifications. This allows multiple values to be drained together rather than requiring one signal for every `Push`.

## Scope

This package intentionally provides a small in-memory queue abstraction.

It does **not** provide:

* Persistence
* Distributed coordination
* Retries
* Dead-letter queues
* Scheduling
* Rate limiting
* Backpressure
* Consumer acknowledgements
* At-least-once delivery
* Exactly-once delivery
* Cross-process communication

For those requirements, a message broker, job queue, or workflow system may be more appropriate.

## License

See [LICENSE](LICENSE).
