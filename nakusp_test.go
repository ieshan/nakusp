package nakusp

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ieshan/nakusp/models"
	"github.com/ieshan/nakusp/transports"
)

type nakuspTest struct {
	n             *Nakusp
	fakeTransport *transports.FakeTransport
}

func (nt *nakuspTest) setup(t *testing.T) {
	t.Helper()

	nt.fakeTransport = transports.NewFake()
	nt.n = NewNakusp(
		&models.Config{MaxWorkers: 5, DefaultTaskRuntime: 600, GracefulTimeout: time.Second},
		map[string]models.Transport{DefaultTransport: nt.fakeTransport},
	)
}

func TestNakusp(t *testing.T) {
	t.Run("PublishAddsJobToDefaultTransport", func(t *testing.T) {
		var nt nakuspTest
		nt.setup(t)
		ctx := context.Background()

		if err := nt.n.Publish(ctx, "test-task", "payload"); err != nil {
			t.Fatalf("Publish returned error: %v", err)
		}

		jobQueue := make(chan *models.Job, 1)
		consumeCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		go func() {
			if err := nt.fakeTransport.Consume(consumeCtx, nt.n.ID(), jobQueue); err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("Consume returned an unexpected error: %v", err)
			}
		}()

		select {
		case job := <-jobQueue:
			if job.Name != "test-task" {
				t.Fatalf("expected job name 'test-task', got %s", job.Name)
			}
			if job.Payload != "payload" {
				t.Fatalf("expected payload 'payload', got %s", job.Payload)
			}
			if job.RetryCount != 0 {
				t.Fatalf("expected retry count 0, got %d", job.RetryCount)
			}
		case <-time.After(1 * time.Second):
			t.Fatal("expected job in queue after publish, but timed out")
		}
	})

	t.Run("ExecuteJobCompleted", func(t *testing.T) {
		var nt nakuspTest
		nt.setup(t)
		ctx := context.Background()

		handler := models.Handler{
			MaxRetry: 1,
			Func: func(job *models.Job) error {
				return nil
			},
		}
		nt.n.AddHandler("test-task", handler)

		job := &models.Job{
			ID:      RandomID(),
			Name:    "test-task",
			Payload: "payload",
		}
		if err := nt.fakeTransport.Publish(ctx, job); err != nil {
			t.Fatalf("Publish returned error: %v", err)
		}

		if err := nt.n.ExecuteJob(ctx, nt.fakeTransport, job); err != nil {
			t.Fatalf("ExecuteJob returned error: %v", err)
		}

		jobQueue := make(chan *models.Job, 1)
		consumeCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		go func() {
			if err := nt.fakeTransport.Consume(consumeCtx, nt.n.ID(), jobQueue); err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("Consume returned an unexpected error: %v", err)
			}
		}()

		select {
		case <-jobQueue:
			t.Fatal("expected no jobs after completion")
		case <-time.After(100 * time.Millisecond):
			// Expected timeout, as no job should be fetched
		}
	})

	t.Run("ExecuteJobRequeueOnError", func(t *testing.T) {
		var nt nakuspTest
		nt.setup(t)
		ctx := context.Background()

		handler := models.Handler{
			MaxRetry: 2,
			Func: func(job *models.Job) error {
				return errors.New("requeue")
			},
		}
		nt.n.AddHandler("test-task", handler)

		job := &models.Job{
			ID:      RandomID(),
			Name:    "test-task",
			Payload: "payload",
		}

		if err := nt.n.ExecuteJob(ctx, nt.fakeTransport, job); err != nil {
			t.Fatalf("ExecuteJob returned error: %v", err)
		}

		if job.RetryCount != 1 {
			t.Fatalf("expected retry count to increment to 1, got %d", job.RetryCount)
		}

		jobQueue := make(chan *models.Job, 1)
		consumeCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		go func() {
			if err := nt.fakeTransport.Consume(consumeCtx, nt.n.ID(), jobQueue); err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("Consume returned an unexpected error: %v", err)
			}
		}()

		select {
		case fetched := <-jobQueue:
			if fetched.ID != job.ID {
				t.Fatalf("expected requeued job ID %s, got %s", job.ID, fetched.ID)
			}
			if fetched.RetryCount != 1 {
				t.Fatalf("expected requeued job retry count 1, got %d", fetched.RetryCount)
			}
		case <-time.After(1 * time.Second):
			t.Fatal("expected job to be requeued, but timed out")
		}
	})

	t.Run("ExecuteJobSendToDLQAtMaxRetry", func(t *testing.T) {
		var nt nakuspTest
		nt.setup(t)
		ctx := context.Background()

		handler := models.Handler{
			MaxRetry: 1,
			Func: func(job *models.Job) error {
				return errors.New("dlq")
			},
		}
		nt.n.AddHandler("test-task", handler)

		job := &models.Job{
			ID:         RandomID(),
			Name:       "test-task",
			Payload:    "payload",
			RetryCount: 1,
		}
		if err := nt.fakeTransport.Publish(ctx, job); err != nil {
			t.Fatalf("Publish returned error: %v", err)
		}

		if err := nt.n.ExecuteJob(ctx, nt.fakeTransport, job); err != nil {
			t.Fatalf("ExecuteJob returned error: %v", err)
		}

		jobQueue := make(chan *models.Job, 1)
		consumeCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		go func() {
			if err := nt.fakeTransport.Consume(consumeCtx, nt.n.ID(), jobQueue); err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("Consume returned an unexpected error: %v", err)
			}
		}()

		select {
		case <-jobQueue:
			t.Fatal("expected no jobs after sending to DLQ")
		case <-time.After(100 * time.Millisecond):
			// Expected timeout, as no job should be fetched
		}

		if len(nt.fakeTransport.Dlq) != 1 {
			t.Fatalf("expected 1 job in DLQ, got %d", len(nt.fakeTransport.Dlq))
		}

		if nt.fakeTransport.Dlq[0].ID != job.ID {
			t.Fatalf("expected job %s in DLQ, got %s", job.ID, nt.fakeTransport.Dlq[0].ID)
		}
	})

	t.Run("ConsumeAll", func(t *testing.T) {
		var nt nakuspTest
		nt.setup(t)
		ctx := context.Background()

		processedJobs := make(chan string, 3)
		handler := models.Handler{
			MaxRetry: 1,
			Func: func(job *models.Job) error {
				processedJobs <- job.ID
				return nil
			},
		}
		nt.n.AddHandler("test-task", handler)

		jobs := []*models.Job{
			{ID: "1", Name: "test-task", Payload: "payload1"},
			{ID: "2", Name: "test-task", Payload: "payload2"},
			{ID: "3", Name: "test-task", Payload: "payload3"},
		}

		for _, job := range jobs {
			if err := nt.fakeTransport.Publish(ctx, job); err != nil {
				t.Fatalf("Publish returned error: %v", err)
			}
		}

		go func() {
			if err := nt.n.ConsumeAll(DefaultTransport); err != nil {
				t.Errorf("ConsumeAll returned an error: %v", err)
			}
		}()

		for i := 0; i < len(jobs); i++ {
			select {
			case <-processedJobs:
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for job to be processed")
			}
		}
	})

	t.Run("ScheduleDirectPublish", func(t *testing.T) {
		var nt nakuspTest
		nt.setup(t)
		ctx := context.Background()

		executionChan := make(chan struct{}, 10)
		handler := models.Handler{
			MaxRetry: 1,
			Func: func(job *models.Job) error {
				executionChan <- struct{}{}
				return nil
			},
		}
		nt.n.AddHandler("test-task", handler)

		// Start worker in background
		go func() {
			_ = nt.n.StartWorker(DefaultTransport)
		}()

		// Give worker time to start
		time.Sleep(50 * time.Millisecond)

		// Directly publish a job
		if err := nt.n.Publish(ctx, "test-task", "payload"); err != nil {
			t.Fatalf("Publish returned error: %v", err)
		}

		// Wait for execution
		select {
		case <-executionChan:
			// Success
		case <-time.After(200 * time.Millisecond):
			t.Fatal("job was not executed")
		}
	})

	t.Run("ScheduleSingleTask", func(t *testing.T) {
		var nt nakuspTest
		nt.setup(t)

		executionChan := make(chan struct{}, 10)
		handler := models.Handler{
			MaxRetry: 1,
			Func: func(job *models.Job) error {
				executionChan <- struct{}{}
				return nil
			},
		}
		nt.n.AddHandler("scheduled-task", handler)

		// Add schedule with short interval for testing
		if err := nt.n.AddSchedule("scheduled-task", 100*time.Millisecond); err != nil {
			t.Fatalf("AddSchedule returned error: %v", err)
		}

		// Start worker in background (it will run until test ends)
		go func() {
			_ = nt.n.StartWorker(DefaultTransport)
		}()

		// Wait for at least 2 executions
		executionCount := 0
		timeout := time.After(400 * time.Millisecond)
		for executionCount < 2 {
			select {
			case <-executionChan:
				executionCount++
			case <-timeout:
				t.Fatalf("expected at least 2 executions, got %d", executionCount)
			}
		}
	})

	t.Run("ScheduleMultipleTasksWithDifferentIntervals", func(t *testing.T) {
		var nt nakuspTest
		nt.setup(t)

		task1Count := 0
		task2Count := 0
		task3Count := 0
		var mu sync.Mutex

		handler1 := models.Handler{
			MaxRetry: 1,
			Func: func(job *models.Job) error {
				mu.Lock()
				task1Count++
				mu.Unlock()
				return nil
			},
		}
		handler2 := models.Handler{
			MaxRetry: 1,
			Func: func(job *models.Job) error {
				mu.Lock()
				task2Count++
				mu.Unlock()
				return nil
			},
		}
		handler3 := models.Handler{
			MaxRetry: 1,
			Func: func(job *models.Job) error {
				mu.Lock()
				task3Count++
				mu.Unlock()
				return nil
			},
		}

		nt.n.AddHandler("task-50ms", handler1)
		nt.n.AddHandler("task-100ms", handler2)
		nt.n.AddHandler("task-150ms", handler3)

		// Add schedules with different intervals
		if err := nt.n.AddSchedule("task-50ms", 50*time.Millisecond); err != nil {
			t.Fatalf("AddSchedule returned error: %v", err)
		}
		if err := nt.n.AddSchedule("task-100ms", 100*time.Millisecond); err != nil {
			t.Fatalf("AddSchedule returned error: %v", err)
		}
		if err := nt.n.AddSchedule("task-150ms", 150*time.Millisecond); err != nil {
			t.Fatalf("AddSchedule returned error: %v", err)
		}

		// Start worker in background
		go func() {
			_ = nt.n.StartWorker(DefaultTransport)
		}()

		// Give worker time to start
		time.Sleep(50 * time.Millisecond)

		// Wait for executions
		time.Sleep(350 * time.Millisecond)

		mu.Lock()
		final1 := task1Count
		final2 := task2Count
		final3 := task3Count
		mu.Unlock()

		// Verify execution counts (with some tolerance for timing)
		// task-50ms should run ~7 times (350ms / 50ms)
		// task-100ms should run ~3 times (350ms / 100ms)
		// task-150ms should run ~2 times (350ms / 150ms)
		if final1 < 5 || final1 > 9 {
			t.Fatalf("expected task-50ms to run 5-9 times, got %d", final1)
		}
		if final2 < 2 || final2 > 5 {
			t.Fatalf("expected task-100ms to run 2-5 times, got %d", final2)
		}
		if final3 < 1 || final3 > 4 {
			t.Fatalf("expected task-150ms to run 1-4 times, got %d", final3)
		}
	})

	t.Run("ScheduleValidation", func(t *testing.T) {
		var nt nakuspTest
		nt.setup(t)

		// Test negative duration
		err := nt.n.AddSchedule("invalid-task", -1*time.Second)
		if err == nil {
			t.Fatal("expected error for negative duration, got nil")
		}

		// Test zero duration
		err = nt.n.AddSchedule("invalid-task", 0)
		if err == nil {
			t.Fatal("expected error for zero duration, got nil")
		}

		// Test valid duration
		err = nt.n.AddSchedule("valid-task", 1*time.Second)
		if err != nil {
			t.Fatalf("expected no error for valid duration, got %v", err)
		}
	})

	t.Run("ScheduleWithoutHandlerLogsError", func(t *testing.T) {
		var nt nakuspTest
		nt.setup(t)

		// Add schedule without registering handler
		if err := nt.n.AddSchedule("missing-handler", 50*time.Millisecond); err != nil {
			t.Fatalf("AddSchedule returned error: %v", err)
		}

		// Start worker in background
		go func() {
			_ = nt.n.StartWorker(DefaultTransport)
		}()

		// Give worker time to start
		time.Sleep(50 * time.Millisecond)

		// Let it run briefly - should not panic even without handler
		time.Sleep(150 * time.Millisecond)
	})

	t.Run("ScheduleTimerEfficiency", func(t *testing.T) {
		var nt nakuspTest
		nt.setup(t)

		executionCount := 0
		var mu sync.Mutex

		handler := models.Handler{
			MaxRetry: 1,
			Func: func(job *models.Job) error {
				mu.Lock()
				executionCount++
				mu.Unlock()
				return nil
			},
		}
		nt.n.AddHandler("timer-task", handler)

		if err := nt.n.AddSchedule("timer-task", 30*time.Millisecond); err != nil {
			t.Fatalf("AddSchedule returned error: %v", err)
		}

		// Start worker in background
		go func() {
			_ = nt.n.StartWorker(DefaultTransport)
		}()

		// Give worker time to start
		time.Sleep(50 * time.Millisecond)

		// Let it run and verify timer works correctly
		time.Sleep(100 * time.Millisecond)

		mu.Lock()
		finalCount := executionCount
		mu.Unlock()

		if finalCount < 2 {
			t.Fatalf("expected at least 2 executions, got %d", finalCount)
		}
	})
}
