package nakusp

import (
	"context"
	"errors"
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
	nt.n = NewNakusp(nil, map[string]models.Transport{
		DefaultTransport: nt.fakeTransport,
	})
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
}
