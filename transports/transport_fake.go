package transports

import (
	"context"
	"sync"

	"github.com/ieshan/nakusp/models"
)

// FakeTransport is an in-memory transport for Nakusp, used primarily for testing.
// It simulates the behavior of real transport without any external dependencies.
// It is safe for concurrent use.
type FakeTransport struct {
	mu   sync.Mutex
	Jobs []*models.Job
	Dlq  []*models.Job
}

// NewFake creates a new FakeTransport.
func NewFake() *FakeTransport {
	return &FakeTransport{}
}

// Publish adds a job to the in-memory queue.
func (t *FakeTransport) Publish(_ context.Context, job *models.Job) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Jobs = append(t.Jobs, job)
	return nil
}

// Heartbeat blocks until the context is cancelled, mirroring long-running transports.
func (t *FakeTransport) Heartbeat(ctx context.Context, _ string) error {
	<-ctx.Done()
	return ctx.Err()
}

// Consume continually drains the in-memory queue, waiting briefly when no jobs are available.
func (t *FakeTransport) Consume(ctx context.Context, _ string, jobQueue chan *models.Job) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			t.mu.Lock()
			if len(t.Jobs) > 0 {
				jobs := make([]*models.Job, len(t.Jobs))
				copy(jobs, t.Jobs)
				t.Jobs = nil
				t.mu.Unlock()

				for _, job := range jobs {
					select {
					case jobQueue <- job:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			} else {
				t.mu.Unlock()
			}
		}
	}
}

// ConsumeAll sends all jobs from the in-memory queue to the jobQueue and then closes the channel.
func (t *FakeTransport) ConsumeAll(ctx context.Context, _ string, jobQueue chan *models.Job) error {
	t.mu.Lock()
	jobs := make([]*models.Job, len(t.Jobs))
	copy(jobs, t.Jobs)
	t.Jobs = nil
	t.mu.Unlock()

	for _, job := range jobs {
		select {
		case jobQueue <- job:
		case <-ctx.Done():
			close(jobQueue)
			return ctx.Err()
		}
	}

	close(jobQueue)
	return nil
}

// Requeue adds a job back to the in-memory queue.
func (t *FakeTransport) Requeue(ctx context.Context, job *models.Job) error {
	return t.Publish(ctx, job)
}

// removeJob removes a job with the given ID from the queue.
// Must be called with mutex held.
func (t *FakeTransport) removeJob(jobID string) {
	filtered := make([]*models.Job, 0, len(t.Jobs))
	for _, j := range t.Jobs {
		if j.ID != jobID {
			filtered = append(filtered, j)
		}
	}
	t.Jobs = filtered
}

// SendToDLQ moves a job from the main queue to the in-memory DLQ.
func (t *FakeTransport) SendToDLQ(_ context.Context, job *models.Job) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.removeJob(job.ID)
	t.Dlq = append(t.Dlq, job)
	return nil
}

// Completed removes a job from the in-memory queue.
func (t *FakeTransport) Completed(_ context.Context, job *models.Job) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.removeJob(job.ID)
	return nil
}

// Close closes the transport
func (t *FakeTransport) Close(_ context.Context) error {
	return nil
}
