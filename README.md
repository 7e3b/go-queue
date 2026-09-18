# go-queue

A small, generic, concurrent FIFO queue for Go.

## Features

* **Generic** — works with any Go type.
* **Concurrent-safe** — supports multiple concurrent producers and consumers.
* **FIFO** — items are stored and delivered in insertion order.
* **Blocking `Pop`** — waits until an item is available.
* **Unbounded** — `Push` does not block waiting for consumers.
* **Graceful shutdown** — `Close` stops the queue and unblocks waiting operations.
* **Zero dependencies** — built entirely with the Go standard library.

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

	if err := q.Push("hello"); err != nil {
		panic(err)
	}

	if err := q.Push("world"); err != nil {
		panic(err)
	}

	value, err := q.Pop()
	if err != nil {
		panic(err)
	}

	fmt.Println(value) // hello

	value, err = q.Pop()
	if err != nil {
		panic(err)
	}

	fmt.Println(value) // world

	q.Close()
}
```

## API

### `New[T]`

Creates a new queue for values of type `T`.

```go
q := queue.New[MyType]()
```

### `Push`

Adds an item to the queue.

```go
err := q.Push(value)
```

`Push` is safe for concurrent use.

It returns an error if the queue has been closed.

`Push` does not block waiting for a consumer. The queue is unbounded, so
items can accumulate if producers are faster than consumers.

### `Pop`

Returns the next available item.

```go
value, err := q.Pop()
```

`Pop` blocks while the queue is empty.

It returns an error when the queue has been closed.

`Pop` is safe for concurrent use.

With multiple concurrent consumers, the queue maintains FIFO delivery internally,
but callers should not rely on which consumer receives a particular item.

### `Close`

Stops the queue.

```go
q.Close()
```

`Close` is safe to call concurrently and may be called multiple times.

Pending items may not be delivered after the queue is closed.

## Concurrency

The queue is designed to be shared between goroutines:

```go
q := queue.New[int]()

for i := 0; i < 10; i++ {
	go func(id int) {
		for j := 0; j < 100; j++ {
			_ = q.Push(id*100 + j)
		}
	}(i)
}

for i := 0; i < 5; i++ {
	go func() {
		for {
			value, err := q.Pop()
			if err != nil {
				return
			}

			fmt.Println(value)
		}
	}()
}
```

A queue must not be copied after its first use.

## Backpressure

This queue intentionally does **not** provide backpressure.

Because the queue is unbounded, a producer can continue adding items even when
consumers cannot keep up:

```text
Producer
    │
    ▼
┌─────────┐
│ Queue   │  ← grows as producers
│         │    outpace consumers
└────┬────┘
     │
     ▼
 Consumer
```

If backpressure is required, use a bounded queue or another mechanism such as
a semaphore, rate limiter, or external message broker with flow control.

## Shutdown Semantics

`Close` stops the queue's internal dispatcher and waits for it to terminate.

Closing the queue does **not** guarantee that pending items will be drained.

Therefore:

> `Close` means stop the queue; pending work may be discarded.

It is not a graceful-drain operation.

## Implementation

The queue uses a small internal dispatcher to separate producers from consumers:

```text
             Push
               │
               ▼
        ┌─────────────┐
        │  []T store  │
        └──────┬──────┘
               │
        work notification
               │
               ▼
        ┌─────────────┐
        │  dispatcher │
        └──────┬──────┘
               │
               ▼
             subCh
               │
               ▼
              Pop
```

The work notification channel has a capacity of one and acts as a coalesced
"work available" signal. Multiple calls to `Push` therefore do not require one
notification per item.

The dispatcher takes the accumulated items from the internal store and sends
them through the subscription channel.

## Testing

Run the tests normally:

```bash
go test ./...
```

Run them with the race detector:

```bash
go test -race ./...
```

For additional concurrency stress:

```bash
go test -race -count=100 ./...
```

The race detector is particularly useful for validating concurrent `Push`,
`Pop`, and `Close` operations.

## License

See [LICENSE](LICENSE).
