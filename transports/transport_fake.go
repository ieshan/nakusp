package transports

import (
	"context"

	"github.com/ieshan/nakusp/models"
)

// FakeTransport is an in-memory transport for Nakusp, used primarily for testing.
// It simulates the behavior of real transport without any external dependencies.
type FakeTransport struct {
	Jobs []*models.Job
	Dlq  []*models.Job
}

// NewFake creates a new FakeTransport.
func NewFake() *FakeTransport {
	return &FakeTransport{}
}

// Publish adds a job to the in-memory queue.
func (t *FakeTransport) Publish(_ context.Context, job *models.Job) error {
	t.Jobs = append(t.Jobs, job)
	return nil
}

// Heartbeat blocks until the context is cancelled, mirroring long-running transports.
func (t *FakeTransport) Heartbeat(ctx context.Context, _ string) error {
	<-ctx.Done()
	return ctx.Err()
}

// Fetch continually drains the in-memory queue, waiting briefly when no jobs are available.
func (t *FakeTransport) Fetch(_ context.Context, _ string, jobQueue chan *models.Job) error {
	for _, job := range t.Jobs {
		jobQueue <- job
	}
	return nil
}

// Requeue adds a job back to the in-memory queue.
func (t *FakeTransport) Requeue(ctx context.Context, job *models.Job) error {
	return t.Publish(ctx, job)
}

// SendToDLQ moves a job from the main queue to the in-memory DLQ.
func (t *FakeTransport) SendToDLQ(_ context.Context, job *models.Job) error {
	nonDeletedJobs := make([]*models.Job, 0, len(t.Jobs))
	for i := range t.Jobs {
		if t.Jobs[i].ID != job.ID {
			nonDeletedJobs = append(nonDeletedJobs, t.Jobs[i])
		}
	}
	t.Jobs = nonDeletedJobs
	t.Dlq = append(t.Dlq, job)
	return nil
}

// Completed removes a job from the in-memory queue.
func (t *FakeTransport) Completed(_ context.Context, job *models.Job) error {
	nonDeletedJobs := make([]*models.Job, 0, len(t.Jobs))
	for i := range t.Jobs {
		if t.Jobs[i].ID != job.ID {
			nonDeletedJobs = append(nonDeletedJobs, t.Jobs[i])
		}
	}
	t.Jobs = nonDeletedJobs
	return nil
}
