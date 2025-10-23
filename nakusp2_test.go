package main

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/ieshan/nakusp/models"
	"github.com/ieshan/nakusp/transports"
)

func newTestNakusp2(t *testing.T) (*Nakusp2, *transports.FakeTransport) {
	t.Helper()

	fakeTransport := transports.NewFake()
	n := NewNakusp2(nil, map[string]models.Transport{
		DefaultTransport: fakeTransport,
	})

	return n, fakeTransport
}

func TestNakusp2PublishAddsJobToDefaultTransport(t *testing.T) {
	ctx := context.Background()

	n, fakeTransport := newTestNakusp2(t)

	if err := n.Publish(ctx, "test-task", "payload"); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}

	jobQueue := make(chan *models.Job, 1)
	if err := fakeTransport.Fetch(ctx, n.ID(), jobQueue); err != nil {
		t.Fatalf("Fetch returned error: %v", err)
	}

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
	default:
		t.Fatal("expected job in queue after publish")
	}
}

func TestNakusp2ExecuteJobCompleted(t *testing.T) {
	ctx := context.Background()

	n, fakeTransport := newTestNakusp2(t)

	handler := models.Handler{
		MaxRetry: 1,
		Func: func(job *models.Job) error {
			return nil
		},
	}
	n.AddHandler("test-task", handler)

	job := &models.Job{
		ID:      uuid.NewString(),
		Name:    "test-task",
		Payload: "payload",
	}
	if err := fakeTransport.Publish(ctx, job); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}

	if err := n.ExecuteJob(ctx, fakeTransport, job); err != nil {
		t.Fatalf("ExecuteJob returned error: %v", err)
	}

	jobQueue := make(chan *models.Job, 1)
	if err := fakeTransport.Fetch(ctx, n.ID(), jobQueue); err != nil {
		t.Fatalf("Fetch returned error: %v", err)
	}

	select {
	case <-jobQueue:
		t.Fatal("expected no jobs after completion")
	default:
	}
}

func TestNakusp2ExecuteJobRequeueOnError(t *testing.T) {
	ctx := context.Background()

	n, fakeTransport := newTestNakusp2(t)

	handler := models.Handler{
		MaxRetry: 2,
		Func: func(job *models.Job) error {
			return errors.New("requeue")
		},
	}
	n.AddHandler("test-task", handler)

	job := &models.Job{
		ID:      uuid.NewString(),
		Name:    "test-task",
		Payload: "payload",
	}

	if err := n.ExecuteJob(ctx, fakeTransport, job); err != nil {
		t.Fatalf("ExecuteJob returned error: %v", err)
	}

	if job.RetryCount != 1 {
		t.Fatalf("expected retry count to increment to 1, got %d", job.RetryCount)
	}

	jobQueue := make(chan *models.Job, 1)
	if err := fakeTransport.Fetch(ctx, n.ID(), jobQueue); err != nil {
		t.Fatalf("Fetch returned error: %v", err)
	}

	select {
	case fetched := <-jobQueue:
		if fetched.ID != job.ID {
			t.Fatalf("expected requeued job ID %s, got %s", job.ID, fetched.ID)
		}
		if fetched.RetryCount != 1 {
			t.Fatalf("expected requeued job retry count 1, got %d", fetched.RetryCount)
		}
	default:
		t.Fatal("expected job to be requeued")
	}
}

func TestNakusp2ExecuteJobSendToDLQAtMaxRetry(t *testing.T) {
	ctx := context.Background()

	n, fakeTransport := newTestNakusp2(t)

	handler := models.Handler{
		MaxRetry: 1,
		Func: func(job *models.Job) error {
			return errors.New("dlq")
		},
	}
	n.AddHandler("test-task", handler)

	job := &models.Job{
		ID:         uuid.NewString(),
		Name:       "test-task",
		Payload:    "payload",
		RetryCount: 1,
	}
	if err := fakeTransport.Publish(ctx, job); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}

	if err := n.ExecuteJob(ctx, fakeTransport, job); err != nil {
		t.Fatalf("ExecuteJob returned error: %v", err)
	}

	jobQueue := make(chan *models.Job, 1)
	if err := fakeTransport.Fetch(ctx, n.ID(), jobQueue); err != nil {
		t.Fatalf("Fetch returned error: %v", err)
	}

	select {
	case <-jobQueue:
		t.Fatal("expected no jobs after sending to DLQ")
	default:
	}

	if len(fakeTransport.Dlq) != 1 {
		t.Fatalf("expected 1 job in DLQ, got %d", len(fakeTransport.Dlq))
	}

	if fakeTransport.Dlq[0].ID != job.ID {
		t.Fatalf("expected job %s in DLQ, got %s", job.ID, fakeTransport.Dlq[0].ID)
	}
}
