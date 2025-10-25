# Nakusp: A Background Job Processing System in Go

Nakusp is a flexible and extensible background job processing system written in Go. It is designed to be transport-agnostic, allowing you to choose the backend that best fits your needs, whether it's an in-memory queue for testing, a robust Redis-based queue for production, or a persistent SQLite database.

## Architecture

The system is composed of two main components:

*   **Nakusp Core**: The orchestrator that manages job execution. It launches transport-specific processes (like fetching and heartbeating) and maintains a pool of goroutines to execute jobs as they arrive. The core delegates loop control to transports, allowing each transport to manage its own polling cadence and execution rhythm.

*   **Transports**: Pluggable modules that implement the `Transport` interface and provide the queuing mechanism. Each transport is responsible for:
    *   Managing its own long-running processes (Heartbeat and Fetch methods)
    *   Controlling polling intervals and execution cadence
    *   Handling graceful shutdown via context cancellation
    *   Ensuring atomic operations for job state transitions

### Built-in Transports

Nakusp comes with three production-ready transports:

*   **FakeTransport**: An in-memory transport ideal for testing. It simulates blocking behavior without external dependencies, making it perfect for unit tests.

*   **RedisTransport**: A production-grade transport using Redis lists and Lua scripts for atomic operations. Features include:
    *   Atomic job fetching with distributed locking
    *   Worker heartbeat tracking with automatic expiration
    *   Configurable polling intervals (200ms default for fetching)
    *   Support for Dead Letter Queue (DLQ) for failed jobs

*   **SQLiteTransport**: A persistent transport using SQLite for local job storage. Features include:
    *   Transaction-based job locking to prevent duplicate processing
    *   Configurable heartbeat (30s) and fetch (250ms) intervals
    *   Automatic job expiration and worker health monitoring
    *   Suitable for single-node deployments or development environments

### Transport Interface

All transports must implement the following interface:

```go
type Transport interface {
    Publish(ctx context.Context, job *Job) error
    Heartbeat(ctx context.Context, id string) error  // Blocks until context cancelled
    Fetch(ctx context.Context, id string, jobQueue chan *Job) error  // Blocks until context cancelled
    Requeue(ctx context.Context, job *Job) error
    SendToDLQ(ctx context.Context, job *Job) error
    Completed(ctx context.Context, job *Job) error
}
```

The `Heartbeat` and `Fetch` methods are designed to run as long-lived goroutines, blocking until the context is cancelled. This design allows each transport to control its own execution cadence without requiring the core system to manage timing logic.

## Getting Started

### Prerequisites

*   Go 1.25 or later
*   Docker and Docker Compose

### Installation

1.  Clone the repository:

    ```sh
    git clone https://github.com/ieshan/nakusp.git
    cd nakusp
    ```

2.  Install dependencies:

    ```sh
    go mod tidy
    ```

### Running Tests

To run the tests, you can use the provided Docker Compose setup, which includes a Redis instance:

```sh
docker-compose -f compose.yml run --rm nakusp-test
```

## Usage

Here's a basic example of how to use Nakusp with the default in-memory transport:

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ieshan/nakusp/models"
)

func main() {
	// Create a new Nakusp instance with default settings
	n := NewNakusp(nil, nil)

	// Define a handler for a task
	handler := models.Handler{
		MaxRetry: 3,
		Func: func(job *models.Job) error {
			fmt.Printf("Processing job %s with payload: %s\n", job.ID, job.Payload)
			return nil
		},
	}

	// Register the handler
	n.AddHandler("my-task", handler)

	// Start a worker
	go n.StartWorker(DefaultTransport)

	// Publish a job
	if err := n.Publish(context.Background(), "my-task", "hello, world!"); err != nil {
		panic(err)
	}

	// Wait for the job to be processed
	time.Sleep(1 * time.Second)
}
```

## Transports

Nakusp's strength lies in its flexible transport system. You can easily switch between transports or even use multiple transports in the same application.

### FakeTransport

The `FakeTransport` is an in-memory transport that is perfect for testing. It requires no external dependencies and is the default transport if none are provided.

### RedisTransport

The `RedisTransport` uses Redis as a backend and is suitable for production environments. To use it, you'll need a running Redis instance.

```go
import (
	"github.com/ieshan/nakusp/transports"
	"github.com/redis/go-redis/v9"
)

// ...

redisClient := redis.NewClient(&redis.Options{
	Addr: "localhost:6379",
})

redisTransport := transports.NewRedis(redisClient, nil, nil)

// ...
```

### SQLiteTransport

The `SQLiteTransport` provides a persistent job queue using a SQLite database. This is a good option if you need persistence without the overhead of a full-fledged Redis server.

```go
import "github.com/ieshan/nakusp/transports"

// ...

sqliteTransport, err := transports.NewSQLite("/path/to/your.db")
if err != nil {
	panic(err)
}

// ...
```
