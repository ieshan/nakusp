package nakusp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ieshan/idx"
	"github.com/ieshan/nakusp/models"
	"github.com/ieshan/nakusp/transports"
)

// safeBuffer is a threadsafe buffer for capturing slog output during tests.
type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

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

// noCloseTransport violates the Transport.ConsumeAll contract: it returns
// without closing jobQueue. Nakusp.ConsumeAll must not hang on it.
type noCloseTransport struct{ *transports.FakeTransport }

func (t *noCloseTransport) ConsumeAll(_ context.Context, _ idx.ID, _ chan *models.Job) error {
	return errors.New("boom")
}

// countingCloseTransport records how many times Close is invoked.
type countingCloseTransport struct {
	*transports.FakeTransport
	mu     sync.Mutex
	closes int
}

func (c *countingCloseTransport) Close(ctx context.Context) error {
	c.mu.Lock()
	c.closes++
	c.mu.Unlock()
	return c.FakeTransport.Close(ctx)
}

func TestNakusp(t *testing.T) {
	t.Run("PublishAddsJobToDefaultTransport", func(t *testing.T) {
		var nt nakuspTest
		nt.setup(t)
		ctx := context.Background()

		jobId, err := nt.n.Publish(ctx, "test-task", "payload")
		if err != nil {
			t.Fatalf("Publish returned error: %v", err)
		}

		jobQueue := make(chan *models.Job, 1)
		consumeCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		go func() {
			if err = nt.fakeTransport.Consume(consumeCtx, nt.n.ID(), jobQueue); err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("Consume returned an unexpected error: %v", err)
			}
		}()

		select {
		case job := <-jobQueue:
			if job.ID != jobId {
				t.Fatalf("expected job ID %s, got %s", jobId, job.ID)
			}
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
		if err := nt.n.AddHandler("test-task", handler); err != nil {
			t.Fatalf("AddHandler returned error: %v", err)
		}

		job := &models.Job{
			ID:      idx.NewID(),
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
		if err := nt.n.AddHandler("test-task", handler); err != nil {
			t.Fatalf("AddHandler returned error: %v", err)
		}

		job := &models.Job{
			ID:      idx.NewID(),
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
		if err := nt.n.AddHandler("test-task", handler); err != nil {
			t.Fatalf("AddHandler returned error: %v", err)
		}

		job := &models.Job{
			ID:         idx.NewID(),
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

	t.Run("ExecuteJobMissingHandlerSendsToDLQ", func(t *testing.T) {
		var nt nakuspTest
		nt.setup(t)
		ctx := context.Background()

		job := &models.Job{ID: idx.NewID(), Name: "no-handler", Payload: "payload"}
		if err := nt.fakeTransport.Publish(ctx, job); err != nil {
			t.Fatalf("Publish returned error: %v", err)
		}

		err := nt.n.ExecuteJob(ctx, nt.fakeTransport, job)
		if err == nil {
			t.Fatal("expected error for missing handler, got nil")
		}

		if len(nt.fakeTransport.Dlq) != 1 {
			t.Fatalf("expected 1 job in DLQ, got %d", len(nt.fakeTransport.Dlq))
		}
		if nt.fakeTransport.Dlq[0].ID != job.ID {
			t.Fatalf("expected job %s in DLQ, got %s", job.ID, nt.fakeTransport.Dlq[0].ID)
		}
	})

	t.Run("ExecuteJobHandlerPanicSendsToDLQ", func(t *testing.T) {
		var nt nakuspTest
		nt.setup(t)
		ctx := context.Background()

		handler := models.Handler{
			MaxRetry: 3,
			Func:     func(job *models.Job) error { panic("boom") },
		}
		if err := nt.n.AddHandler("panic-task", handler); err != nil {
			t.Fatalf("AddHandler returned error: %v", err)
		}

		job := &models.Job{ID: idx.NewID(), Name: "panic-task", Payload: "payload"}
		err := nt.n.ExecuteJob(ctx, nt.fakeTransport, job)
		if err == nil {
			t.Fatal("expected error from panicking handler, got nil")
		}
		if !strings.Contains(err.Error(), "panicked") {
			t.Fatalf("unexpected error: %v", err)
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

		processedJobs := make(chan idx.ID, 3)
		handler := models.Handler{
			MaxRetry: 1,
			Func: func(job *models.Job) error {
				processedJobs <- job.ID
				return nil
			},
		}
		if err := nt.n.AddHandler("test-task", handler); err != nil {
			t.Fatalf("AddHandler returned error: %v", err)
		}

		jobs := []*models.Job{
			{ID: idx.NewID(), Name: "test-task", Payload: "payload1"},
			{ID: idx.NewID(), Name: "test-task", Payload: "payload2"},
			{ID: idx.NewID(), Name: "test-task", Payload: "payload3"},
		}

		for _, job := range jobs {
			if err := nt.fakeTransport.Publish(ctx, job); err != nil {
				t.Fatalf("Publish returned error: %v", err)
			}
		}

		go func() {
			if err := nt.n.ConsumeAll(ctx, DefaultTransport); err != nil {
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

	t.Run("PublishErrorsWhenDefaultTransportMissing", func(t *testing.T) {
		n := NewNakusp(nil, map[string]models.Transport{
			"a": transports.NewFake(),
			"b": transports.NewFake(),
		})

		_, err := n.Publish(context.Background(), "test-task", "payload")
		if err == nil {
			t.Fatal("expected error when default transport is missing, got nil")
		}
		if !strings.Contains(err.Error(), `transport "default" not found`) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("BindTransportRoutesTask", func(t *testing.T) {
		fakeDefault := transports.NewFake()
		fakeSecond := transports.NewFake()
		n := NewNakusp(nil, map[string]models.Transport{
			DefaultTransport: fakeDefault,
			"second":         fakeSecond,
		})

		if err := n.BindTransport("bound-task", "second"); err != nil {
			t.Fatalf("BindTransport returned error: %v", err)
		}

		ctx := context.Background()
		if _, err := n.Publish(ctx, "bound-task", "payload"); err != nil {
			t.Fatalf("Publish returned error: %v", err)
		}
		if _, err := n.Publish(ctx, "unbound-task", "payload"); err != nil {
			t.Fatalf("Publish returned error: %v", err)
		}

		if len(fakeSecond.Jobs) != 1 || fakeSecond.Jobs[0].Name != "bound-task" {
			t.Fatalf("expected bound-task on second transport, got %v", fakeSecond.Jobs)
		}
		if len(fakeDefault.Jobs) != 1 || fakeDefault.Jobs[0].Name != "unbound-task" {
			t.Fatalf("expected unbound-task on default transport, got %v", fakeDefault.Jobs)
		}
	})

	t.Run("BindTransportUnknownTransport", func(t *testing.T) {
		var nt nakuspTest
		nt.setup(t)

		err := nt.n.BindTransport("task", "nonexistent")
		if err == nil {
			t.Fatal("expected error for unknown transport, got nil")
		}
		if !strings.Contains(err.Error(), `transport "nonexistent" not found`) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("SingleTransportAliasedAsDefault", func(t *testing.T) {
		solo := &countingCloseTransport{FakeTransport: transports.NewFake()}
		n := NewNakusp(nil, map[string]models.Transport{"solo": solo})

		if _, err := n.Publish(context.Background(), "test-task", "payload"); err != nil {
			t.Fatalf("Publish returned error: %v", err)
		}
		if len(solo.Jobs) != 1 {
			t.Fatalf("expected job published to aliased transport, got %d jobs", len(solo.Jobs))
		}

		if err := n.Close(context.Background()); err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
		solo.mu.Lock()
		closes := solo.closes
		solo.mu.Unlock()
		if closes != 1 {
			t.Fatalf("expected Close to run once, got %d", closes)
		}
	})

	t.Run("EmptyTransportMapGetsFakeDefault", func(t *testing.T) {
		n := NewNakusp(nil, map[string]models.Transport{})
		if _, err := n.Publish(context.Background(), "test-task", "payload"); err != nil {
			t.Fatalf("Publish returned error: %v", err)
		}
	})

	t.Run("NewNakuspDoesNotMutateCallerMap", func(t *testing.T) {
		callerMap := map[string]models.Transport{"solo": transports.NewFake()}
		_ = NewNakusp(nil, callerMap)
		if _, ok := callerMap[DefaultTransport]; ok {
			t.Fatal("NewNakusp mutated the caller's transports map")
		}
	})

	t.Run("ConsumeAllUnknownTransport", func(t *testing.T) {
		var nt nakuspTest
		nt.setup(t)

		err := nt.n.ConsumeAll(context.Background(), "nonexistent")
		if err == nil {
			t.Fatal("expected error for unknown transport, got nil")
		}
		if !strings.Contains(err.Error(), `transport "nonexistent" not found`) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("ConsumeAllConcurrentTransports", func(t *testing.T) {
		fakeDefault := transports.NewFake()
		fakeSecond := transports.NewFake()
		n := NewNakusp(
			&models.Config{MaxWorkers: 5, DefaultTaskRuntime: 600, GracefulTimeout: time.Second},
			map[string]models.Transport{DefaultTransport: fakeDefault, "second": fakeSecond},
		)

		var mu sync.Mutex
		processed := make(map[idx.ID]bool)
		handler := models.Handler{
			MaxRetry: 0,
			Func: func(job *models.Job) error {
				mu.Lock()
				processed[job.ID] = true
				mu.Unlock()
				return nil
			},
		}
		if err := n.AddHandler("test-task", handler); err != nil {
			t.Fatalf("AddHandler returned error: %v", err)
		}

		ctx := context.Background()
		jobIDs := make(map[idx.ID]bool)
		for _, ft := range []*transports.FakeTransport{fakeDefault, fakeSecond} {
			for i := 0; i < 2; i++ {
				job := &models.Job{ID: idx.NewID(), Name: "test-task", Payload: "payload"}
				if err := ft.Publish(ctx, job); err != nil {
					t.Fatalf("Publish returned error: %v", err)
				}
				jobIDs[job.ID] = true
			}
		}

		var wg sync.WaitGroup
		errs := make([]error, 2)
		wg.Add(2)
		go func() { defer wg.Done(); errs[0] = n.ConsumeAll(ctx, DefaultTransport) }()
		go func() { defer wg.Done(); errs[1] = n.ConsumeAll(ctx, "second") }()
		wg.Wait()

		for i, err := range errs {
			if err != nil {
				t.Fatalf("ConsumeAll %d returned error: %v", i, err)
			}
		}

		mu.Lock()
		defer mu.Unlock()
		if len(processed) != len(jobIDs) {
			t.Fatalf("expected %d jobs processed, got %d", len(jobIDs), len(processed))
		}
		for id := range jobIDs {
			if !processed[id] {
				t.Fatalf("job %s was not processed", id)
			}
		}
	})

	t.Run("ConsumeAllSurvivesContractViolation", func(t *testing.T) {
		n := NewNakusp(nil, map[string]models.Transport{
			DefaultTransport: &noCloseTransport{FakeTransport: transports.NewFake()},
		})

		done := make(chan error, 1)
		go func() {
			done <- n.ConsumeAll(context.Background(), DefaultTransport)
		}()

		select {
		case err := <-done:
			if err == nil || err.Error() != "boom" {
				t.Fatalf("expected 'boom' error, got %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("ConsumeAll hung on a transport that returned without closing jobQueue")
		}
	})

	t.Run("ScheduleDirectPublish", func(t *testing.T) {
		var nt nakuspTest
		nt.setup(t)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		executionChan := make(chan struct{}, 10)
		handler := models.Handler{
			MaxRetry: 1,
			Func: func(job *models.Job) error {
				executionChan <- struct{}{}
				return nil
			},
		}
		if err := nt.n.AddHandler("test-task", handler); err != nil {
			t.Fatalf("AddHandler returned error: %v", err)
		}

		// Start worker in background
		go func() {
			_ = nt.n.StartWorker(ctx, DefaultTransport)
		}()

		// Give worker time to start
		time.Sleep(50 * time.Millisecond)

		// Directly publish a job
		if _, err := nt.n.Publish(ctx, "test-task", "payload"); err != nil {
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
		if err := nt.n.AddHandler("scheduled-task", handler); err != nil {
			t.Fatalf("AddHandler returned error: %v", err)
		}

		// Add schedule with short interval for testing
		if err := nt.n.AddSchedule("scheduled-task", 100*time.Millisecond); err != nil {
			t.Fatalf("AddSchedule returned error: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		// Start worker in background (it will run until test ends)
		go func() {
			_ = nt.n.StartWorker(ctx, DefaultTransport)
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
		taskTimeWithHandlers := map[int]models.Handler{
			50:  handler1,
			100: handler2,
			150: handler3,
		}
		for taskTime, handler := range taskTimeWithHandlers {
			taskName := fmt.Sprintf("task-%dms", taskTime)
			if err := nt.n.AddHandler(taskName, handler); err != nil {
				t.Fatalf("AddHandler returned error: %v", err)
			}
			// Add schedules with different intervals
			if err := nt.n.AddSchedule(taskName, time.Duration(taskTime)*time.Millisecond); err != nil {
				t.Fatalf("AddSchedule returned error: %v", err)
			}
		}

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		// Start worker in background
		go func() {
			_ = nt.n.StartWorker(ctx, DefaultTransport)
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

		buf := &safeBuffer{}
		logger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		origLogger := slog.Default()
		slog.SetDefault(logger)
		defer slog.SetDefault(origLogger)

		// Add schedule without registering handler
		if err := nt.n.AddSchedule("missing-handler", 50*time.Millisecond); err != nil {
			t.Fatalf("AddSchedule returned error: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		// Start worker in background
		go func() {
			_ = nt.n.StartWorker(ctx, DefaultTransport)
		}()

		// Give worker time to start
		time.Sleep(50 * time.Millisecond)

		// Let it run briefly - should not panic even without handler
		time.Sleep(150 * time.Millisecond)

		logs := buf.String()
		if !strings.Contains(logs, "handler 'missing-handler' not found") {
			t.Fatalf("expected log to mention missing handler, got: %s", logs)
		}
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
		if err := nt.n.AddHandler("timer-task", handler); err != nil {
			t.Fatalf("AddHandler returned error: %v", err)
		}

		if err := nt.n.AddSchedule("timer-task", 30*time.Millisecond); err != nil {
			t.Fatalf("AddSchedule returned error: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		// Start worker in background
		go func() {
			_ = nt.n.StartWorker(ctx, DefaultTransport)
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

	t.Run("StartWorkerStopsOnContextCancel", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var nt nakuspTest
			nt.setup(t)

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- nt.n.StartWorker(ctx, DefaultTransport) }()

			synctest.Wait()
			cancel()

			if err := <-done; err != nil {
				t.Fatalf("StartWorker returned error: %v", err)
			}
		})
	})

	t.Run("StartWorkerGracefulShutdownTimeout", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			fake := transports.NewFake()
			n := NewNakusp(
				&models.Config{MaxWorkers: 5, DefaultTaskRuntime: 600, GracefulTimeout: 50 * time.Millisecond},
				map[string]models.Transport{DefaultTransport: fake},
			)
			release := make(chan struct{})
			if err := n.AddHandler("slow-task", models.Handler{
				MaxRetry: 0,
				Func:     func(*models.Job) error { <-release; return nil },
			}); err != nil {
				t.Fatalf("AddHandler returned error: %v", err)
			}
			if err := fake.Publish(context.Background(), &models.Job{ID: idx.NewID(), Name: "slow-task"}); err != nil {
				t.Fatalf("Publish returned error: %v", err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- n.StartWorker(ctx, DefaultTransport) }()

			synctest.Wait() // job picked up; handler blocked on release
			cancel()

			// wg can't drain while the handler blocks — StartWorker must return
			// via the 50ms GracefulTimeout on the fake clock.
			if err := <-done; err != nil {
				t.Fatalf("StartWorker returned error: %v", err)
			}
			close(release)
			synctest.Wait()
		})
	})

	t.Run("StartWorkerUnknownTransport", func(t *testing.T) {
		var nt nakuspTest
		nt.setup(t)

		err := nt.n.StartWorker(context.Background(), "nonexistent")
		if err == nil {
			t.Fatal("expected error for unknown transport, got nil")
		}
		if !strings.Contains(err.Error(), `transport "nonexistent" not found`) {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}
