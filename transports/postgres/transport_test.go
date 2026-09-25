package postgres

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ieshan/idx"
	"github.com/ieshan/nakusp/models"
	gormpostgres "gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestPostgresTransport(t *testing.T) {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set, skipping Postgres transport tests")
	}

	ctx := context.Background()

	newTransport := func(t *testing.T) *PostgresTransport {
		t.Helper()
		transport, err := NewPostgres(dsn, &PostgresConfig{
			HeartbeatInterval: 50 * time.Millisecond,
			FetchInterval:     50 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("failed to create postgres transport: %v", err)
		}
		t.Cleanup(func() { _ = transport.Close(context.Background()) })
		if err := transport.db.Exec("TRUNCATE jobs, workers").Error; err != nil {
			t.Fatalf("failed to truncate tables: %v", err)
		}
		return transport
	}

	t.Run("publish_and_consume", func(t *testing.T) {
		transport := newTransport(t)
		var wg sync.WaitGroup
		job := &models.Job{ID: idx.NewID(), Name: "test-job", Payload: "test-payload"}
		if err := transport.Publish(ctx, job); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}

		jobQueue := make(chan *models.Job, 1)
		consumeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := transport.Consume(consumeCtx, idx.NewID(), jobQueue); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("Consume() returned an unexpected error: %v", err)
			}
		}()

		select {
		case fetchedJob := <-jobQueue:
			if fetchedJob.ID != job.ID {
				t.Errorf("consumed job ID does not match: got %v, want %v", fetchedJob.ID, job.ID)
			}
			cancel()
		case <-consumeCtx.Done():
			t.Fatal("expected a job from Consume(), but got none within the timeout")
		}
		wg.Wait()
	})

	t.Run("postgres_atomic_claim_single_delivery", func(t *testing.T) {
		transport := newTransport(t)
		job := &models.Job{ID: idx.NewID(), Name: "race-job", Payload: "payload"}
		if err := transport.Publish(ctx, job); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}

		jobQueue := make(chan *models.Job, 2)
		consumeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		var wg sync.WaitGroup
		var received int32

		consumeOnce := func() {
			defer wg.Done()
			_ = transport.Consume(consumeCtx, idx.NewID(), jobQueue)
		}

		wg.Add(2)
		go consumeOnce()
		go consumeOnce()

		select {
		case <-consumeCtx.Done():
			t.Fatalf("did not receive job: %v", consumeCtx.Err())
		case <-jobQueue:
			atomic.AddInt32(&received, 1)
			cancel()
		}

		wg.Wait()
		if got := atomic.LoadInt32(&received); got != 1 {
			t.Fatalf("expected exactly one delivery, got %d", got)
		}
	})

	t.Run("consumeall_delivers_all_exactly_once", func(t *testing.T) {
		publisher := newTransport(t)
		consumer := newTransport(t)

		jobs := []*models.Job{
			{ID: idx.NewID(), Name: "job-1", Payload: "payload-1"},
			{ID: idx.NewID(), Name: "job-2", Payload: "payload-2"},
			{ID: idx.NewID(), Name: "job-3", Payload: "payload-3"},
		}
		for _, job := range jobs {
			if err := publisher.Publish(ctx, job); err != nil {
				t.Fatalf("Publish() error = %v", err)
			}
		}

		type result struct {
			jobs map[string]bool
			err  error
		}
		consumers := []models.Transport{publisher, consumer}
		results := make([]result, len(consumers))

		var wg sync.WaitGroup
		wg.Add(len(consumers))
		for i, tr := range consumers {
			go func(i int, tr models.Transport) {
				defer wg.Done()
				jobQueue := make(chan *models.Job, len(jobs))
				err := tr.ConsumeAll(ctx, idx.NewID(), jobQueue)
				received := make(map[string]bool)
				for job := range jobQueue {
					received[job.ID.String()] = true
				}
				results[i] = result{jobs: received, err: err}
			}(i, tr)
		}
		wg.Wait()

		seen := make(map[string]bool)
		for i, res := range results {
			if res.err != nil {
				t.Fatalf("ConsumeAll %d returned error: %v", i, res.err)
			}
			for id := range res.jobs {
				if seen[id] {
					t.Fatalf("job %s delivered by more than one ConsumeAll", id)
				}
				seen[id] = true
			}
		}

		if len(seen) != len(jobs) {
			t.Fatalf("expected %d jobs delivered, got %d", len(jobs), len(seen))
		}
		for _, job := range jobs {
			if !seen[job.ID.String()] {
				t.Errorf("job with ID %s was not consumed", job.ID)
			}
		}
	})

	t.Run("postgres_dlq_not_requeued", func(t *testing.T) {
		transport := newTransport(t)
		job := &models.Job{ID: idx.NewID(), Name: "dlq-job", Payload: "payload"}
		if err := transport.Publish(ctx, job); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
		if err := transport.SendToDLQ(ctx, job); err != nil {
			t.Fatalf("SendToDLQ() error = %v", err)
		}

		jobQueue := make(chan *models.Job, 1)
		if err := transport.ConsumeAll(ctx, idx.NewID(), jobQueue); err != nil {
			t.Fatalf("ConsumeAll() error = %v", err)
		}

		if job, ok := <-jobQueue; ok {
			t.Fatalf("expected no jobs from DLQ, got %v", job)
		}
	})

	t.Run("requeue_returns_job_to_queue", func(t *testing.T) {
		transport := newTransport(t)
		job := &models.Job{ID: idx.NewID(), Name: "requeue-job", Payload: "payload"}
		if err := transport.Publish(ctx, job); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}

		// Claim the job, then requeue it — it must become consumable again.
		claimed, err := transport.claimOne(ctx, idx.NewID())
		if err != nil {
			t.Fatalf("claimOne() error = %v", err)
		}
		if claimed == nil || claimed.ID != job.ID {
			t.Fatalf("expected to claim job %v, got %v", job.ID, claimed)
		}

		claimed.RetryCount = 1
		if err := transport.Requeue(ctx, claimed); err != nil {
			t.Fatalf("Requeue() error = %v", err)
		}

		jobQueue := make(chan *models.Job, 1)
		if err := transport.ConsumeAll(ctx, idx.NewID(), jobQueue); err != nil {
			t.Fatalf("ConsumeAll() error = %v", err)
		}

		got, ok := <-jobQueue
		if !ok {
			t.Fatal("expected requeued job to be delivered, got none")
		}
		if got.ID != job.ID || got.RetryCount != 1 {
			t.Errorf("requeued job mismatch: got %v (retry %d), want %v (retry 1)", got.ID, got.RetryCount, job.ID)
		}
	})

	t.Run("completed_removes_job", func(t *testing.T) {
		transport := newTransport(t)
		job := &models.Job{ID: idx.NewID(), Name: "completed-job", Payload: "payload"}
		if err := transport.Publish(ctx, job); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
		if err := transport.Completed(ctx, job); err != nil {
			t.Fatalf("Completed() error = %v", err)
		}

		var count int64
		if err := transport.db.Model(&jobRow{}).Where("id = ?", job.ID).Count(&count).Error; err != nil {
			t.Fatalf("count query error = %v", err)
		}
		if count != 0 {
			t.Fatalf("expected job row to be deleted, found %d", count)
		}
	})

	t.Run("consumeall_cancel_requeues_undelivered", func(t *testing.T) {
		transport := newTransport(t)

		for range 3 {
			job := &models.Job{ID: idx.NewID(), Name: "cancel-job", Payload: "payload"}
			if err := transport.Publish(ctx, job); err != nil {
				t.Fatalf("Publish() error = %v", err)
			}
		}

		jobQueue := make(chan *models.Job) // unbuffered: sends block until read
		consumeCtx, cancel := context.WithCancel(ctx)

		done := make(chan error, 1)
		go func() {
			done <- transport.ConsumeAll(consumeCtx, idx.NewID(), jobQueue)
		}()

		first := <-jobQueue
		if first == nil {
			t.Fatal("expected first job, got nil")
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("ConsumeAll() error = %v, want context.Canceled", err)
		}

		// The remaining two jobs must have been returned to 'queued'.
		retry := make(chan *models.Job, 2)
		if err := transport.ConsumeAll(ctx, idx.NewID(), retry); err != nil {
			t.Fatalf("ConsumeAll() retry error = %v", err)
		}
		got := 0
		for job := range retry {
			if job.ID == first.ID {
				t.Errorf("delivered job %s was requeued", job.ID)
			}
			got++
		}
		if got != 2 {
			t.Fatalf("expected 2 undelivered jobs requeued, got %d", got)
		}
	})

	t.Run("new_postgres_from_db", func(t *testing.T) {
		db, err := gorm.Open(gormpostgres.Open(dsn), &gorm.Config{})
		if err != nil {
			t.Fatalf("failed to open gorm db: %v", err)
		}
		sqlDB, err := db.DB()
		if err != nil {
			t.Fatalf("failed to get sql.DB: %v", err)
		}
		t.Cleanup(func() { _ = sqlDB.Close() })

		transport, err := NewPostgresFromDB(db, nil)
		if err != nil {
			t.Fatalf("NewPostgresFromDB() error = %v", err)
		}
		if err := transport.db.Exec("TRUNCATE jobs, workers").Error; err != nil {
			t.Fatalf("failed to truncate tables: %v", err)
		}

		job := &models.Job{ID: idx.NewID(), Name: "from-db-job", Payload: "payload"}
		if err := transport.Publish(ctx, job); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
		jobQueue := make(chan *models.Job, 1)
		if err := transport.ConsumeAll(ctx, idx.NewID(), jobQueue); err != nil {
			t.Fatalf("ConsumeAll() error = %v", err)
		}
		if got, ok := <-jobQueue; !ok || got.ID != job.ID {
			t.Fatalf("expected job %v, got %v", job.ID, got)
		}

		// Close must not tear down the caller-owned connection pool.
		if err := transport.Close(ctx); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
		if err := sqlDB.Ping(); err != nil {
			t.Fatalf("caller-owned pool closed by transport: %v", err)
		}
	})

	t.Run("heartbeat_writes_worker_row", func(t *testing.T) {
		transport := newTransport(t)
		workerID := idx.NewID()

		heartbeatCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- transport.Heartbeat(heartbeatCtx, workerID) }()

		deadline := time.Now().Add(10 * time.Second)
		for {
			var count int64
			if err := transport.db.Model(&workerRow{}).Where("id = ?", workerID).Count(&count).Error; err != nil {
				t.Fatalf("count query error = %v", err)
			}
			if count == 1 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("worker heartbeat row was not written")
			}
			time.Sleep(20 * time.Millisecond)
		}

		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("Heartbeat() error = %v, want context.Canceled", err)
		}
	})
}
