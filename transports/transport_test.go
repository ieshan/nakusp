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
			if err := client.FlushAll(context.Background()).Err(); err != nil {
				t.Fatalf("failed to flush redis: %v", err)
			}
			return NewRedis(client, nil, nil)
		}
	}

	for name, buildTransport := range transportBuilders {
		t.Run(name, func(t *testing.T) {
			transport := buildTransport(t)
			ctx := context.Background()

			t.Run("PublishAndFetch", func(t *testing.T) {
				var wg sync.WaitGroup
				job := &models.Job{ID: uuid.NewString(), Name: "test-job", Payload: "test-payload"}
				if err := transport.Publish(ctx, job); err != nil {
					t.Fatalf("Publish() error = %v", err)
				}

				jobQueue := make(chan *models.Job, 1)
				fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()

				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := transport.Fetch(fetchCtx, "test-worker", jobQueue); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
						t.Errorf("Fetch() returned an unexpected error: %v", err)
					}
				}()

				select {
				case fetchedJob := <-jobQueue:
					if fetchedJob.ID != job.ID {
						t.Errorf("fetched job ID does not match: got %v, want %v", fetchedJob.ID, job.ID)
					}
				case <-fetchCtx.Done():
					t.Fatal("expected a job from Fetch(), but got none within the timeout")
				}
				wg.Wait()
			})

			t.Run("Completed", func(t *testing.T) {
				job := &models.Job{ID: uuid.NewString(), Name: "completed-job", Payload: "completed-payload"}
				if err := transport.Publish(ctx, job); err != nil {
					t.Fatalf("Publish() error = %v", err)
				}
				if err := transport.Completed(ctx, job); err != nil {
					t.Fatalf("Completed() error = %v", err)
				}
			})

			t.Run("Requeue", func(t *testing.T) {
				job := &models.Job{ID: uuid.NewString(), Name: "requeue-job", Payload: "requeue-payload"}
				if err := transport.Publish(ctx, job); err != nil {
					t.Fatalf("Publish() error = %v", err)
				}
				if err := transport.Requeue(ctx, job); err != nil {
					t.Fatalf("Requeue() error = %v", err)
				}
			})

			t.Run("SendToDLQ", func(t *testing.T) {
				job := &models.Job{ID: uuid.NewString(), Name: "dlq-job", Payload: "dlq-payload"}
				if err := transport.Publish(ctx, job); err != nil {
					t.Fatalf("Publish() error = %v", err)
				}
				if err := transport.SendToDLQ(ctx, job); err != nil {
					t.Fatalf("SendToDLQ() error = %v", err)
				}
			})
		})
	}
}
