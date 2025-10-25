// Package models defines the core data structures and interfaces for the Nakusp job queue system.
package models

import (
	"context"
	"time"
)

// Config holds configuration parameters for the Nakusp system.
type Config struct {
	// MaxWorkers is the maximum number of concurrent workers that can process jobs.
	MaxWorkers int

	// DefaultTaskRuntime is the default timeout in seconds for a task execution.
	DefaultTaskRuntime int

	// HeartbeatPeriod is the interval at which workers send heartbeats to indicate they're alive.
	HeartbeatPeriod time.Duration

	// GracefulTimeout is the duration to wait for graceful shutdown before forcing termination.
	GracefulTimeout time.Duration
}

// Job represents a unit of work to be processed by a worker.
type Job struct {
	// ID is the unique identifier for the job.
	ID string

	// Name is the job type identifier, used to route jobs to appropriate handlers.
	Name string

	// RetryCount tracks how many times this job has been retried after failures.
	RetryCount int

	// Payload contains the job-specific data as a string.
	Payload string
}

// HandlerFunc is a function that processes a job.
// It should return an error if the job processing fails.
type HandlerFunc func(job *Job) error

// Handler defines how a specific job type should be processed.
type Handler struct {
	// MaxRetry is the maximum number of times a job can be retried before being sent to the DLQ.
	MaxRetry int

	// Func is the function that processes the job.
	Func HandlerFunc
}

// Transport defines the interface that all transport implementations must satisfy.
// Transports are responsible for managing job queues and worker coordination.
// All methods that run in loops (Heartbeat, Fetch) should block until the context is cancelled.
type Transport interface {
	// Publish adds a new job to the queue.
	Publish(ctx context.Context, job *Job) error

	// Heartbeat runs in a loop, periodically updating the worker's status to indicate it's alive.
	// This method should block until the context is cancelled.
	Heartbeat(ctx context.Context, id string) error

	// Fetch runs in a loop, continuously retrieving jobs from the queue and sending them to jobQueue.
	// This method should block until the context is cancelled.
	Fetch(ctx context.Context, id string, jobQueue chan *Job) error

	// Requeue moves a failed job back to the queue for retry.
	Requeue(ctx context.Context, job *Job) error

	// SendToDLQ moves a job to the Dead Letter Queue after it has exceeded its retry limit.
	SendToDLQ(ctx context.Context, job *Job) error

	// Completed marks a job as successfully completed and removes it from the queue.
	Completed(ctx context.Context, job *Job) error
}
