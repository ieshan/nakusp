package transports

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/ieshan/nakusp/models"
	"github.com/redis/go-redis/v9"
)

func TestTransport(t *testing.T) {
	transports := map[string]models.Transport{
		"fake": NewFake(),
	}

	redisURI := os.Getenv("REDIS_URI")
	if redisURI != "" {
		opt, err := redis.ParseURL(redisURI)
		if err != nil {
			t.Fatalf("failed to parse redis uri: %v", err)
		}
		transports["redis"] = NewRedis(redis.NewClient(opt), nil, nil)
	}

	sqlite, err := NewSQLite(":memory:")
	if err != nil {
		t.Fatalf("failed to create sqlite transport: %v", err)
	}
	transports["sqlite"] = sqlite

	for name, transport := range transports {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			t.Run("PublishAndFetch", func(t *testing.T) {
				job := &models.Job{ID: uuid.NewString(), Name: "test-job", Payload: "test-payload"}
				if err := transport.Publish(ctx, job); err != nil {
					t.Fatalf("Publish() error = %v", err)
				}

				jobQueue := make(chan *models.Job, 1)
				if err := transport.Fetch(ctx, "test-worker", jobQueue); err != nil {
					t.Fatalf("Fetch() error = %v", err)
				}

				select {
				case fetchedJob := <-jobQueue:
					if fetchedJob.ID != job.ID {
						t.Errorf("fetched job ID does not match: got %v, want %v", fetchedJob.ID, job.ID)
					}
				default:
					t.Fatal("expected a job from Fetch(), but got none")
				}
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
