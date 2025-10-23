# Nakusp: A Background Job Processing System in Go

Nakusp is a flexible and extensible background job processing system written in Go. It is designed to be transport-agnostic, allowing you to choose the backend that best fits your needs, whether it's an in-memory queue for testing, a robust Redis-based queue for production, or a persistent SQLite database.

## Architecture

The system is composed of two main components:

*   **Nakusp2**: The core worker that manages job execution. It fetches jobs from a transport, processes them in a pool of goroutines, and handles retries and failures.
*   **Transports**: Pluggable modules that provide the queuing mechanism. Nakusp comes with three built-in transports:
    *   **FakeTransport**: An in-memory transport for testing purposes.
    *   **RedisTransport**: A production-ready transport that uses Redis lists and hashes.
    *   **SQLiteTransport**: A persistent transport that uses a SQLite database.

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
	// Create a new Nakusp2 instance with default settings
	n := NewNakusp2(nil, nil)

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
