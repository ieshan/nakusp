package models

import (
	"context"
	"time"
)

type Config struct {
	MaxWorkers         int
	DefaultTaskRuntime int
	HeartbeatPeriod    time.Duration
	GracefulTimeout    time.Duration
}

type Job struct {
	ID         string
	Name       string
	RetryCount int
	Payload    string
}

type HandlerFunc func(job *Job) error

type Handler struct {
	MaxRetry int
	Func     HandlerFunc
}

type Transport interface {
	Publish(ctx context.Context, job *Job) error
	Heartbeat(ctx context.Context, id string) error
	Fetch(ctx context.Context, id string, jobQueue chan *Job) error
	Requeue(ctx context.Context, job *Job) error
	SendToDLQ(ctx context.Context, job *Job) error
	Completed(ctx context.Context, job *Job) error
}
