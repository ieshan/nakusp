package transports

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ieshan/nakusp/models"
	"github.com/redis/go-redis/v9"
)

func TestTransport(t *testing.T) {
	transportBuilders := map[string]func(t *testing.T) models.Transport{
		"fake": func(t *testing.T) models.Transport {
			return NewFake()
		},
		"sqlite": func(t *testing.T) models.Transport {
			sqlite, err := NewSQLite(":memory:", &SQLiteConfig{HeartbeatInternal: 5 * time.Minute, FetchInterval: 2 * time.Second})
			if err != nil {
				t.Fatalf("failed to create sqlite transport: %v", err)
			}
			return sqlite
		},
	}

	redisURI := os.Getenv("REDIS_URI")
	if redisURI != "" {
		transportBuilders["redis"] = func(t *testing.T) models.Transport {
			opt, err := redis.ParseURL(redisURI)
			if err != nil {
				t.Fatalf("failed to parse redis uri: %v", err)
			}
			client := redis.NewClient(opt)
			// Flush all keys before each test to ensure clean state
			if err = client.FlushAll(context.Background()).Err(); err != nil {
				t.Fatalf("failed to flush redis: %v", err)
			}
			return NewRedis(client, nil, nil)
		}
	}

	for name, buildTransport := range transportBuilders {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			t.Run("PublishAndConsume", func(t *testing.T) {
				transport := buildTransport(t)
				var wg sync.WaitGroup
				job := &models.Job{ID: uuid.NewString(), Name: "test-job", Payload: "test-payload"}
				if err := transport.Publish(ctx, job); err != nil {
					t.Fatalf("Publish() error = %v", err)
				}

				jobQueue := make(chan *models.Job, 1)
				consumeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()

				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := transport.Consume(consumeCtx, "test-worker", jobQueue); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
						t.Errorf("Consume() returned an unexpected error: %v", err)
					}
				}()

				select {
				case fetchedJob := <-jobQueue:
					if fetchedJob.ID != job.ID {
						t.Errorf("consumed job ID does not match: got %v, want %v", fetchedJob.ID, job.ID)
					}
				case <-consumeCtx.Done():
					t.Fatal("expected a job from Consume(), but got none within the timeout")
				}
				wg.Wait()
			})

			t.Run("Completed", func(t *testing.T) {
				transport := buildTransport(t)
				job := &models.Job{ID: uuid.NewString(), Name: "completed-job", Payload: "completed-payload"}
				if err := transport.Publish(ctx, job); err != nil {
					t.Fatalf("Publish() error = %v", err)
				}
				if err := transport.Completed(ctx, job); err != nil {
					t.Fatalf("Completed() error = %v", err)
				}
			})

			t.Run("Requeue", func(t *testing.T) {
				transport := buildTransport(t)
				job := &models.Job{ID: uuid.NewString(), Name: "requeue-job", Payload: "requeue-payload"}
				if err := transport.Publish(ctx, job); err != nil {
					t.Fatalf("Publish() error = %v", err)
				}
				if err := transport.Requeue(ctx, job); err != nil {
					t.Fatalf("Requeue() error = %v", err)
				}
			})

			t.Run("SendToDLQ", func(t *testing.T) {
				transport := buildTransport(t)
				job := &models.Job{ID: uuid.NewString(), Name: "dlq-job", Payload: "dlq-payload"}
				if err := transport.Publish(ctx, job); err != nil {
					t.Fatalf("Publish() error = %v", err)
				}
				if err := transport.SendToDLQ(ctx, job); err != nil {
					t.Fatalf("SendToDLQ() error = %v", err)
				}
			})

			t.Run("ConsumeAll", func(t *testing.T) {
				transport := buildTransport(t)
				jobs := []*models.Job{
					{ID: uuid.NewString(), Name: "job-1", Payload: "payload-1"},
					{ID: uuid.NewString(), Name: "job-2", Payload: "payload-2"},
					{ID: uuid.NewString(), Name: "job-3", Payload: "payload-3"},
				}

				for _, job := range jobs {
					if err := transport.Publish(ctx, job); err != nil {
						t.Fatalf("Publish() error = %v", err)
					}
				}

				jobQueue := make(chan *models.Job, len(jobs))
				consumeCtx, cancel := context.WithCancel(ctx)
				defer cancel()

				var wg sync.WaitGroup
				wg.Add(1)
				var consumeErr error
				go func() {
					defer wg.Done()
					consumeErr = transport.ConsumeAll(consumeCtx, "test-worker", jobQueue)
				}()

				receivedJobs := make(map[string]bool)
				for i := 0; i < len(jobs); i++ {
					select {
					case job := <-jobQueue:
						receivedJobs[job.ID] = true
					case <-time.After(5 * time.Second):
						t.Fatal("timed out waiting for job")
					}
				}

				cancel()
				wg.Wait()

				if consumeErr != nil && !errors.Is(consumeErr, context.Canceled) {
					t.Errorf("ConsumeAll() returned an unexpected error: %v", consumeErr)
				}

				if len(receivedJobs) != len(jobs) {
					t.Errorf("expected to consume %d jobs, but got %d", len(jobs), len(receivedJobs))
				}

				for _, job := range jobs {
					if !receivedJobs[job.ID] {
						t.Errorf("job with ID %s was not consumed", job.ID)
					}
				}
			})
		})
	}
}
