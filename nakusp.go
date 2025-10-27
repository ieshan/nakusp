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
	schedules         map[string]time.Duration
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
		schedules:         make(map[string]time.Duration),
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
// If scheduled tasks are registered, it also starts a scheduler goroutine
// that publishes jobs at their configured intervals using a smart timer-based approach.
func (n *Nakusp) StartWorker(transportName string) error {
	n.lock.RLock()
	transport := n.transports[transportName]
	n.lock.RUnlock()

	ctx, cancel := context.WithCancel(context.Background())
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

	// Start scheduler if there are scheduled tasks
	n.lock.RLock()
	hasSchedules := len(n.schedules) > 0
	n.lock.RUnlock()

	if hasSchedules {
		n.wg.Add(1)
		go n.RunUntilCancelled(ctx, n.runScheduler)
	}

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

// AddSchedule registers a task to be executed periodically at the specified interval.
// The task will be automatically published to the transport when StartWorker is called.
// The schedule interval must be positive. Multiple tasks can have different intervals,
// and the scheduler will efficiently manage all of them using a single timer.
//
// Example:
//
//	n.AddSchedule("cleanup-task", 1*time.Hour)
//	n.AddSchedule("health-check", 30*time.Second)
func (n *Nakusp) AddSchedule(taskName string, schedule time.Duration) error {
	if schedule <= 0 {
		return fmt.Errorf("schedule interval must be positive, got %v", schedule)
	}

	n.lock.Lock()
	defer n.lock.Unlock()

	n.schedules[taskName] = schedule
	return nil
}

// scheduleEntry represents a scheduled task with its next execution time.
type scheduleEntry struct {
	name     string
	interval time.Duration
	next     time.Time
}

// runScheduler manages all scheduled tasks using a single timer.
// It always schedules the timer for the next soonest task, and after each fire,
// it recalculates the next wait duration. This approach is more efficient than
// using multiple tickers, especially when dealing with many tasks with different intervals.
func (n *Nakusp) runScheduler(ctx context.Context, _ string) error {
	// Snapshot schedules to avoid holding lock during execution
	n.lock.RLock()
	if len(n.schedules) == 0 {
		n.lock.RUnlock()
		return nil
	}

	entries := make([]scheduleEntry, 0, len(n.schedules))
	now := time.Now()
	for name, interval := range n.schedules {
		entries = append(entries, scheduleEntry{
			name:     name,
			interval: interval,
			next:     now.Add(interval),
		})
	}
	n.lock.RUnlock()

	// Sort by next execution time to find the soonest task
	n.sortScheduleEntries(entries)

	// Calculate initial delay
	delay := entries[0].next.Sub(now)
	if delay < 0 {
		delay = time.Millisecond
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()

		case <-timer.C:
			now := time.Now()

			// Execute all tasks whose time has come (handles tasks with same interval)
			for i := range entries {
				if entries[i].next.After(now) {
					break
				}

				// Publish the scheduled job
				if err := n.Publish(ctx, entries[i].name, ""); err != nil &&
					!errors.Is(err, context.Canceled) &&
					!errors.Is(err, context.DeadlineExceeded) {
					slog.Error(
						"failed to publish scheduled job",
						slog.String("task", entries[i].name),
						slog.Any("error", err),
					)
				}

				// Schedule next execution
				entries[i].next = entries[i].next.Add(entries[i].interval)

				// If we're behind schedule, catch up to current time
				if entries[i].next.Before(now) {
					entries[i].next = now.Add(entries[i].interval)
				}
			}

			// Re-sort to find next soonest task
			n.sortScheduleEntries(entries)

			// Calculate next delay
			delay := time.Until(entries[0].next)
			if delay < 0 {
				delay = time.Millisecond
			}

			timer.Reset(delay)
		}
	}
}

// sortScheduleEntries sorts schedule entries by next execution time (earliest first).
func (n *Nakusp) sortScheduleEntries(entries []scheduleEntry) {
	// Simple insertion sort - efficient for small arrays and mostly-sorted data
	for i := 1; i < len(entries); i++ {
		key := entries[i]
		j := i - 1
		for j >= 0 && entries[j].next.After(key.next) {
			entries[j+1] = entries[j]
			j--
		}
		entries[j+1] = key
	}
}
