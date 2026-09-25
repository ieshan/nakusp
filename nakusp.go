package nakusp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ieshan/idx"
	"github.com/ieshan/nakusp/models"
	trnspt "github.com/ieshan/nakusp/transports"
)

const (
	DefaultTransport = "default"
	MaxTaskNameLen   = 250
)

// Nakusp is a background job processing system that supports multiple transport layers.
// It manages a pool of workers to execute jobs asynchronously.
type Nakusp struct {
	id                idx.ID
	config            *models.Config
	handlers          map[string]models.Handler
	transportHandlers map[string]string
	lock              *sync.RWMutex
	wg                *sync.WaitGroup
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
	ts := make(map[string]models.Transport, len(transports)+1)
	for name, t := range transports {
		ts[name] = t
	}
	if len(ts) == 0 {
		ts[DefaultTransport] = trnspt.NewFake()
	} else if _, ok := ts[DefaultTransport]; !ok {
		if len(ts) == 1 {
			for _, t := range ts {
				ts[DefaultTransport] = t
			}
		} else {
			slog.Warn("no default transport configured; unrouted tasks will fail to publish")
		}
	}

	return &Nakusp{
		id:                idx.NewID(),
		config:            config,
		handlers:          make(map[string]models.Handler),
		transportHandlers: make(map[string]string),
		lock:              &sync.RWMutex{},
		wg:                &sync.WaitGroup{},
		transports:        ts,
		schedules:         make(map[string]time.Duration),
	}
}

// ID returns the unique identifier for the Nakusp worker instance.
func (n *Nakusp) ID() idx.ID {
	return n.id
}

// transport returns the transport registered under name.
func (n *Nakusp) transport(name string) (models.Transport, error) {
	n.lock.RLock()
	defer n.lock.RUnlock()
	t, ok := n.transports[name]
	if !ok || t == nil {
		return nil, fmt.Errorf("transport %q not found", name)
	}
	return t, nil
}

// transportForTask resolves the transport a task is bound to, falling back to the default.
func (n *Nakusp) transportForTask(taskName string) (models.Transport, error) {
	n.lock.RLock()
	defer n.lock.RUnlock()
	transportName, ok := n.transportHandlers[taskName]
	if !ok {
		transportName = DefaultTransport
	}
	t, ok := n.transports[transportName]
	if !ok || t == nil {
		return nil, fmt.Errorf("transport %q not found for task %q", transportName, taskName)
	}
	return t, nil
}

// AddHandler registers a handler for a given task name.
func (n *Nakusp) AddHandler(taskName string, handler models.Handler) error {
	if len(taskName) > MaxTaskNameLen {
		return errors.New("task name is too long")
	}
	n.lock.Lock()
	defer n.lock.Unlock()
	n.handlers[taskName] = handler
	return nil
}

// BindTransport routes a task to a named transport instead of the default.
// Jobs for the task are consumed by workers started on that transport.
func (n *Nakusp) BindTransport(taskName, transportName string) error {
	n.lock.Lock()
	defer n.lock.Unlock()
	if t, ok := n.transports[transportName]; !ok || t == nil {
		return fmt.Errorf("transport %q not found", transportName)
	}
	n.transportHandlers[taskName] = transportName
	return nil
}

// Publish sends a new job to the appropriate transport based on the task name.
// If no specific transport is registered for the task, it uses the default transport.
func (n *Nakusp) Publish(ctx context.Context, taskName string, payload string) (idx.ID, error) {
	taskId := idx.NewID()
	transport, err := n.transportForTask(taskName)
	if err != nil {
		return taskId, err
	}
	return taskId, transport.Publish(ctx, &models.Job{
		ID:         taskId,
		Name:       taskName,
		Payload:    payload,
		RetryCount: 0,
	})
}

// ConsumeAll consumes all jobs from the specified transport and processes them.
// The transport closes jobQueue when done; we drain buffered jobs and return
// the transport's error. Cancel ctx to abort.
func (n *Nakusp) ConsumeAll(ctx context.Context, transportName string) error {
	transport, err := n.transport(transportName)
	if err != nil {
		return err
	}

	jobQueue := make(chan *models.Job, n.config.MaxWorkers)
	errChan := make(chan error, 1)
	var wg sync.WaitGroup

	dispatch := func(job *models.Job) {
		wg.Add(1)
		go func(job *models.Job) {
			defer wg.Done()
			if err := n.ExecuteJob(ctx, transport, job); err != nil {
				slog.Error("job execution error", slog.Any("error", err))
			}
		}(job)
	}

	go func() {
		errChan <- transport.ConsumeAll(ctx, n.id, jobQueue)
	}()

	for jobQueue != nil {
		select {
		case job, ok := <-jobQueue:
			if !ok {
				jobQueue = nil
				continue
			}
			dispatch(job)
		case err := <-errChan:
			// Transport finished without closing the channel — drain what's
			// buffered, then finish. (Contract says transports close the
			// channel; this guards against implementations that don't.)
			for {
				select {
				case job, ok := <-jobQueue:
					if !ok {
						wg.Wait()
						return err
					}
					dispatch(job)
				default:
					wg.Wait()
					return err
				}
			}
		}
	}
	wg.Wait()
	return <-errChan
}

// StartWorker begins the job processing loop for a given transport.
// It listens for jobs and executes them in separate goroutines.
// If scheduled tasks are registered, it also starts a scheduler goroutine
// that publishes jobs at their configured intervals using a single timer-based approach.
//
// The worker runs until ctx is cancelled; the caller owns signal handling.
// On cancellation it waits up to config.GracefulTimeout for in-flight work
// to finish before returning.
func (n *Nakusp) StartWorker(ctx context.Context, transportName string) error {
	transport, err := n.transport(transportName)
	if err != nil {
		return err
	}

	jobQueue := make(chan *models.Job, n.config.MaxWorkers)

	n.wg.Add(2)
	go n.RunUntilCancelled(ctx, transport.Heartbeat)
	go n.RunUntilCancelled(
		ctx,
		func(ctx context.Context, id idx.ID) error {
			return transport.Consume(ctx, id, jobQueue)
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
		case job := <-jobQueue:
			n.wg.Add(1)
			go func(job *models.Job) {
				defer n.wg.Done()
				if err := n.ExecuteJob(ctx, transport, job); err != nil {
					slog.Error("job execution error", slog.Any("error", err))
				}
			}(job)
		case <-ctx.Done():
			done := make(chan struct{})
			go func() { n.wg.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(n.config.GracefulTimeout):
				slog.Warn("graceful shutdown timed out",
					slog.Duration("timeout", n.config.GracefulTimeout))
			}
			return nil
		}
	}
}

// RunUntilCancelled is a helper function that runs a given handler function until the context is cancelled.
// The handler is responsible for honouring the context (including any pacing or blocking behaviour).
func (n *Nakusp) RunUntilCancelled(ctx context.Context, handlerFn func(context.Context, idx.ID) error) {
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
// A missing handler or a handler panic sends the job to the DLQ. Bookkeeping
// calls (Requeue/Completed/SendToDLQ) run on a context detached from ctx so a
// cancelled worker context cannot strand an in-flight job.
func (n *Nakusp) ExecuteJob(ctx context.Context, transport models.Transport, job *models.Job) (err error) {
	n.lock.RLock()
	hf, ok := n.handlers[job.Name]
	n.lock.RUnlock()

	bg := context.WithoutCancel(ctx)

	if !ok {
		err = fmt.Errorf("handler '%s' not found", job.Name)
		slog.Error("error in ExecuteJob", slog.Any("error", err))
		if dlqErr := transport.SendToDLQ(bg, job); dlqErr != nil {
			return errors.Join(err, dlqErr)
		}
		return err
	}

	select {
	case <-ctx.Done():
		return transport.Requeue(bg, job)
	default:
	}

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler '%s' panicked: %v", job.Name, r)
			slog.Error("handler panic", slog.Any("error", err))
			if dlqErr := transport.SendToDLQ(bg, job); dlqErr != nil {
				err = errors.Join(err, dlqErr)
			}
		}
	}()

	if err = hf.Func(job); err != nil {
		if job.RetryCount < hf.MaxRetry {
			job.RetryCount++
			err = transport.Requeue(bg, job)
		} else {
			err = transport.SendToDLQ(bg, job)
		}
	} else {
		err = transport.Completed(bg, job)
	}
	return err
}

// Close closes all transports and releases any resources.
// Transports registered under multiple names are closed only once.
func (n *Nakusp) Close(ctx context.Context) error {
	seen := make(map[models.Transport]bool, len(n.transports))
	for _, transport := range n.transports {
		if seen[transport] {
			continue
		}
		seen[transport] = true
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
func (n *Nakusp) runScheduler(ctx context.Context, _ idx.ID) error {
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
				if _, err := n.Publish(ctx, entries[i].name, ""); err != nil &&
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
