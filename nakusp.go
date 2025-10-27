package nakusp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ieshan/nakusp/models"
	trnspt "github.com/ieshan/nakusp/transports"
)

const (
	DefaultTransport = "default"
)

// Nakusp is a background job processing system that supports multiple transport layers.
// It manages a pool of workers to execute jobs asynchronously.
type Nakusp struct {
	id                string
	config            *models.Config
	handlers          map[string]models.Handler
	transportHandlers map[string]string
	lock              *sync.RWMutex
	wg                sync.WaitGroup
	jobQueue          chan *models.Job
	transports        map[string]models.Transport
}

// NewNakusp creates a new Nakusp instance.
// It takes a configuration and a map of transports. If either is nil, it uses default values.
func NewNakusp(config *models.Config, transports map[string]models.Transport) *Nakusp {
	if config == nil {
		config = &models.Config{
			MaxWorkers:         5,
			DefaultTaskRuntime: 600,
			GracefulTimeout:    time.Second * 5,
		}
	}
	if transports == nil {
		transports = map[string]models.Transport{
			DefaultTransport: trnspt.NewFake(),
		}
	}
	return &Nakusp{
		id:                RandomID(),
		config:            config,
		handlers:          make(map[string]models.Handler),
		transportHandlers: make(map[string]string),
		lock:              &sync.RWMutex{},
		wg:                sync.WaitGroup{},
		jobQueue:          make(chan *models.Job, config.MaxWorkers),
		transports:        transports,
	}
}

// ID returns the unique identifier for the Nakusp worker instance.
func (n *Nakusp) ID() string {
	return n.id
}

// AddHandler registers a handler for a given task name.
func (n *Nakusp) AddHandler(taskName string, handler models.Handler) {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.handlers[taskName] = handler
}

// Publish sends a new job to the appropriate transport based on the task name.
// If no specific transport is registered for the task, it uses the default transport.
func (n *Nakusp) Publish(ctx context.Context, taskName string, payload string) error {
	n.lock.Lock()
	defer n.lock.Unlock()

	transportName, ok := n.transportHandlers[taskName]
	if !ok {
		transportName = DefaultTransport
	}
	return n.transports[transportName].Publish(ctx, &models.Job{
		ID:         RandomID(),
		Name:       taskName,
		Payload:    payload,
		RetryCount: 0,
	})
}

// ConsumeAll consumes all jobs from the specified transport and processes them.
// It starts a consumer goroutine that fetches all jobs and puts them into the job queue.
// It then waits for all jobs to be processed by the workers before returning.
func (n *Nakusp) ConsumeAll(transportName string) error {
	n.lock.RLock()
	transport := n.transports[transportName]
	n.lock.RUnlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Use a channel to receive the error from the consumer goroutine
	errChan := make(chan error, 1)

	go func() {
		errChan <- transport.ConsumeAll(ctx, n.id, n.jobQueue)
	}()

	for j := range n.jobQueue {
		n.wg.Add(1)
		go func(job *models.Job) {
			defer n.wg.Done()
			if err := n.ExecuteJob(ctx, transport, job); err != nil {
				slog.Error("job execution error", slog.Any("error", err))
			}
		}(j)
	}
	n.wg.Wait()

	// Get the error from the consumer goroutine
	return <-errChan
}

// StartWorker begins the job processing loop for a given transport.
// It listens for jobs and executes them in separate goroutines.
func (n *Nakusp) StartWorker(transportName string) error {
	n.lock.RLock()
	transport := n.transports[transportName]
	n.lock.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), n.config.GracefulTimeout)
	defer cancel()

	// Signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGABRT, syscall.SIGTERM)

	n.wg.Add(2)
	go n.RunUntilCancelled(ctx, transport.Heartbeat)
	go n.RunUntilCancelled(
		ctx,
		func(ctx context.Context, id string) error {
			return transport.Consume(ctx, id, n.jobQueue)
		},
	)

	for {
		select {
		case job := <-n.jobQueue:
			n.wg.Add(1)
			go func(job *models.Job) {
				defer n.wg.Done()
				if err := n.ExecuteJob(ctx, transport, job); err != nil {
					slog.Error("job execution error", slog.Any("error", err))
				}
			}(job)
		case <-sigChan:
			slog.Debug("got exit signal in StartWorker")
			cancel()
			n.wg.Wait()
			close(n.jobQueue)
			return nil
		}
	}
}

// RunUntilCancelled is a helper function that runs a given handler function until the context is cancelled.
// The handler is responsible for honouring the context (including any pacing or blocking behaviour).
func (n *Nakusp) RunUntilCancelled(ctx context.Context, handlerFn func(context.Context, string) error) {
	defer n.wg.Done()

	select {
	case <-ctx.Done():
		return
	default:
	}

	if err := handlerFn(ctx, n.id); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		slog.Error(
			"error in RunUntilCancelled",
			slog.Any("error", err),
		)
	}
}

// ExecuteJob processes a single job. It finds the appropriate handler and executes it.
// It handles retries, and moving jobs to the DLQ based on the handler's outcome.
func (n *Nakusp) ExecuteJob(ctx context.Context, transport models.Transport, job *models.Job) error {
	n.lock.RLock()
	hf, ok := n.handlers[job.Name]
	n.lock.RUnlock()

	var err error
	if !ok {
		err = fmt.Errorf("handler '%s' not found", job.Name)
		slog.Error("error in ExecuteJob", slog.Any("error", err))
		return err
	}

	select {
	case <-ctx.Done():
		return transport.Requeue(ctx, job)
	default:
		err = hf.Func(job)
		if err != nil {
			if job.RetryCount < hf.MaxRetry {
				job.RetryCount++
				err = transport.Requeue(ctx, job)
			} else {
				err = transport.SendToDLQ(ctx, job)
			}
		} else {
			err = transport.Completed(ctx, job)
		}
		return err
	}
}

// Close closes all transports and releases any resources.
func (n *Nakusp) Close(ctx context.Context) error {
	for _, transport := range n.transports {
		if err := transport.Close(ctx); err != nil {
			return err
		}
	}
	return nil
}
