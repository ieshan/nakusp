# Nakusp: A Background Job Processing System in Go

Nakusp is a flexible and extensible background job processing system written in Go. It is designed to be transport-agnostic, allowing you to choose the backend that best fits your needs, whether it's an in-memory queue for testing, a robust Redis-based queue for production, or a persistent SQLite database.

## Features

- **Multiple Transport Backends**: Choose from in-memory (FakeTransport), Redis, or SQLite
- **Scheduled Tasks**: Built-in cron-like scheduling with efficient timer-based execution
- **Retry Logic**: Configurable retry counts with automatic Dead Letter Queue (DLQ) support
- **Graceful Shutdown**: Signal-based shutdown with configurable timeout
- **Concurrent Processing**: Worker pool with configurable concurrency
- **Type-Safe**: Strongly typed job handlers and configuration

## Architecture

The system is composed of two main components:

*   **Nakusp Core**: The orchestrator that manages job execution. It launches transport-specific processes (like consuming and heartbeating) and maintains a pool of goroutines to execute jobs as they arrive. The core delegates loop control to transports, allowing each transport to manage its own polling cadence and execution rhythm.

*   **Transports**: Pluggable modules that implement the `Transport` interface and provide the queuing mechanism. Each transport is responsible for:
    *   Managing its own long-running processes (Heartbeat and Consume methods)
    *   Controlling polling intervals and execution cadence
    *   Handling graceful shutdown via context cancellation
    *   Ensuring atomic operations for job state transitions

### Built-in Transports

Nakusp comes with three production-ready transports:

*   **FakeTransport**: An in-memory transport ideal for testing. It simulates blocking behavior without external dependencies, making it perfect for unit tests.

*   **RedisTransport**: A production-grade transport using Redis lists and Lua scripts for atomic operations. Features include:
    *   Atomic job consuming with distributed locking
    *   Worker heartbeat tracking with automatic expiration
    *   Configurable polling intervals (200ms default for consuming)
    *   Support for Dead Letter Queue (DLQ) for failed jobs

*   **SQLiteTransport**: A persistent transport using SQLite for local job storage. Features include:
    *   Transaction-based job locking to prevent duplicate processing
    *   Configurable heartbeat (30s) and consume (250ms) intervals
    *   Automatic job expiration and worker health monitoring
    *   Suitable for single-node deployments or development environments

### Transport Interface

All transports must implement the following interface:

```go
type Transport interface {
    Publish(ctx context.Context, job *Job) error
    Heartbeat(ctx context.Context, id string) error  // Blocks until context cancelled
    Consume(ctx context.Context, id string, jobQueue chan *Job) error  // Blocks until context cancelled
    ConsumeAll(ctx context.Context, id string, jobQueue chan *Job) error
    Requeue(ctx context.Context, job *Job) error
    SendToDLQ(ctx context.Context, job *Job) error
    Completed(ctx context.Context, job *Job) error
}
```

The `Heartbeat` and `Consume` methods are designed to run as long-lived goroutines, blocking until the context is cancelled. This design allows each transport to control its own execution cadence without requiring the core system to manage timing logic.

The `ConsumeAll` method is designed for batch processing scenarios where you want to process all currently queued jobs and then exit. Unlike `Consume`, which runs continuously, `ConsumeAll` fetches all available jobs, sends them to the job queue, closes the channel, and returns once all jobs have been processed.

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

	"github.com/ieshan/nakusp"
	"github.com/ieshan/nakusp/models"
)

func main() {
	// Create a new Nakusp instance with default settings
	n := nakusp.NewNakusp(nil, nil)

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
	go n.StartWorker(nakusp.DefaultTransport)

	// Publish a job
	if err := n.Publish(context.Background(), "my-task", "hello, world!"); err != nil {
		panic(err)
	}

	// Wait for the job to be processed
	time.Sleep(1 * time.Second)
}
```

### Scheduled Tasks

Nakusp supports scheduling tasks to run at regular intervals. The scheduler uses a smart timer-based approach that efficiently handles multiple tasks with different intervals using a single timer.

```go
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ieshan/nakusp"
	"github.com/ieshan/nakusp/models"
)

func main() {
	// Create a new Nakusp instance
	n := nakusp.NewNakusp(nil, nil)

	// Define handlers for different tasks
	cleanupHandler := models.Handler{
		MaxRetry: 3,
		Func: func(job *models.Job) error {
			fmt.Println("Running cleanup task...")
			// Perform cleanup logic
			return nil
		},
	}

	healthCheckHandler := models.Handler{
		MaxRetry: 1,
		Func: func(job *models.Job) error {
			fmt.Println("Running health check...")
			// Perform health check logic
			return nil
		},
	}

	// Register handlers
	n.AddHandler("cleanup-task", cleanupHandler)
	n.AddHandler("health-check", healthCheckHandler)

	// Schedule tasks with different intervals
	if err := n.AddSchedule("cleanup-task", 1*time.Hour); err != nil {
		panic(err)
	}
	if err := n.AddSchedule("health-check", 30*time.Second); err != nil {
		panic(err)
	}

	// Start the worker (scheduler will start automatically)
	go func() {
		if err := n.StartWorker(nakusp.DefaultTransport); err != nil {
			fmt.Printf("Worker error: %v\n", err)
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("Shutting down...")
}
```

#### How Scheduling Works

The scheduler uses an efficient timer-based algorithm:

1. **Single Timer**: Instead of creating multiple tickers (one per task), the scheduler uses a single `time.Timer` that always waits for the next soonest task.

2. **Dynamic Recalculation**: After each timer fires, the scheduler:
   - Executes all tasks whose scheduled time has arrived
   - Calculates the next execution time for each executed task
   - Re-sorts tasks by their next execution time
   - Resets the timer for the soonest upcoming task

3. **Catch-up Logic**: If a task execution takes longer than expected, the scheduler automatically catches up by scheduling the next execution relative to the current time rather than falling further behind.

4. **Graceful Shutdown**: The scheduler respects context cancellation and properly cleans up the timer on shutdown.

This approach is more efficient than using multiple tickers, especially when dealing with many tasks with different intervals, as it minimizes the number of goroutines and timer resources.

### Consuming All Jobs

The `ConsumeAll` method allows you to process all jobs in a queue and then exit. This is useful for batch processing or for running jobs as part of a script.

```go
package main

import (
	"context"
	"fmt"

	"github.com/ieshan/nakusp"
	"github.com/ieshan/nakusp/models"
)

func main() {
	// Create a new Nakusp instance with default settings
	n := nakusp.NewNakusp(nil, nil)

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

	// Publish some jobs
	if err := n.Publish(context.Background(), "my-task", "hello, world!"); err != nil {
		panic(err)
	}
	if err := n.Publish(context.Background(), "my-task", "another job"); err != nil {
		panic(err)
	}

	// Consume all jobs and exit
	if err := n.ConsumeAll(nakusp.DefaultTransport); err != nil {
		panic(err)
	}
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
