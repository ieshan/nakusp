package transports

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/ieshan/nakusp/models"
	_ "github.com/mattn/go-sqlite3"
)

type SQLiteConfig struct {
	HeartbeatInternal time.Duration
	FetchInterval     time.Duration
}

// SQLiteTransport is a transport layer for Nakusp that uses SQLite as a backend.
// It provides a persistent job queue for background task processing.
type SQLiteTransport struct {
	db     *sql.DB
	config *SQLiteConfig
}

// NewSQLite creates a new SQLiteTransport.
// It takes a path to the SQLite database file, initializes the database connection,
// and creates the necessary tables for jobs and workers if they don't already exist.
func NewSQLite(dsn string, config *SQLiteConfig) (*SQLiteTransport, error) {
	if config == nil {
		config = &SQLiteConfig{
			HeartbeatInternal: 5 * time.Minute,
			FetchInterval:     5 * time.Second,
		}
	}

	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}

	// Create jobs table
	_, err = db.Exec(`
	CREATE TABLE IF NOT EXISTS jobs (
		id TEXT PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		payload TEXT,
		retry_count INTEGER NOT NULL,
		status TEXT NOT NULL,
		worker_id TEXT,
		locked_until DATETIME,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to create jobs table: %w", err)
	}

	// Create workers table
	_, err = db.Exec(`
	CREATE TABLE IF NOT EXISTS workers (
		id TEXT PRIMARY KEY,
		last_heartbeat DATETIME NOT NULL
	);
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to create workers table: %w", err)
	}

	return &SQLiteTransport{db: db, config: config}, nil
}

// Publish adds a new job to the queue.
// It inserts a new record into the 'jobs' table with a 'queued' status.
func (t *SQLiteTransport) Publish(ctx context.Context, job *models.Job) error {
	_, err := t.db.ExecContext(
		ctx,
		"INSERT INTO jobs (id, name, payload, retry_count, status) VALUES (?, ?, ?, ?, 'queued')",
		job.ID, job.Name, job.Payload, job.RetryCount,
	)
	return err
}

// Heartbeat updates the worker's last seen time.
// This is used to monitor worker health and re-queue jobs from workers that have gone offline.
func (t *SQLiteTransport) Heartbeat(ctx context.Context, id string) error {
	ticker := time.NewTicker(t.config.HeartbeatInternal)
	defer ticker.Stop()

	for {
		if _, err := t.db.ExecContext(
			ctx,
			"INSERT INTO workers (id, last_heartbeat) VALUES (?, ?) ON CONFLICT(id) DO UPDATE SET last_heartbeat = excluded.last_heartbeat",
			id, time.Now(),
		); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Consume retrieves a batch of jobs from the queue and assigns them to a worker.
// It locks the jobs to prevent other workers from processing them.
func (t *SQLiteTransport) Consume(ctx context.Context, id string, jobQueue chan *models.Job) error {
	ticker := time.NewTicker(t.config.FetchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fetched, err := t.fetchOnce(ctx, id, jobQueue)
		if err != nil {
			return err
		}
		if fetched {
			continue
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (t *SQLiteTransport) fetchOnce(ctx context.Context, id string, jobQueue chan *models.Job) (fetched bool, err error) {
	tx, err := t.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	rows, err := tx.QueryContext(ctx, "SELECT id, name, payload, retry_count FROM jobs WHERE status = 'queued' ORDER BY created_at ASC LIMIT 1")
	if err != nil {
		return false, err
	}

	var jobsToUpdate []string
	for rows.Next() {
		var job models.Job
		if err = rows.Scan(&job.ID, &job.Name, &job.Payload, &job.RetryCount); err != nil {
			return false, err
		}
		jobCopy := job
		jobQueue <- &jobCopy
		jobsToUpdate = append(jobsToUpdate, job.ID)
		fetched = true
	}
	if err = rows.Err(); err != nil {
		return false, err
	}
	if err = rows.Close(); err != nil {
		return false, err
	}

	if fetched {
		stmt, stmtErr := tx.PrepareContext(ctx, "UPDATE jobs SET status = 'in_progress', worker_id = ?, locked_until = ? WHERE id = ?")
		if stmtErr != nil {
			return false, stmtErr
		}
		lockedUntil := time.Now().Add(5 * time.Minute) // 5-minute lock
		for _, jobID := range jobsToUpdate {
			if _, err = stmt.ExecContext(ctx, id, lockedUntil, jobID); err != nil {
				_ = stmt.Close()
				return false, err
			}
		}
		if err = stmt.Close(); err != nil {
			return false, err
		}
	}

	if err = tx.Commit(); err != nil {
		return false, err
	}

	return fetched, nil
}

// Requeue moves a job back to the 'queued' state, typically after a failed execution attempt.
func (t *SQLiteTransport) Requeue(ctx context.Context, job *models.Job) error {
	_, err := t.db.ExecContext(
		ctx,
		"UPDATE jobs SET status = 'queued', retry_count = ?, worker_id = NULL, locked_until = NULL WHERE id = ?",
		job.RetryCount, job.ID,
	)
	return err
}

// SendToDLQ moves a job to the Dead Letter Queue after it has exceeded its max retry count.
func (t *SQLiteTransport) SendToDLQ(ctx context.Context, job *models.Job) error {
	_, err := t.db.ExecContext(ctx, "UPDATE jobs SET status = 'dlq' WHERE id = ?", job.ID)
	return err
}

// Completed marks a job as completed by deleting it from the jobs table.
func (t *SQLiteTransport) Completed(ctx context.Context, job *models.Job) error {
	_, err := t.db.ExecContext(ctx, "DELETE FROM jobs WHERE id = ?", job.ID)
	return err
}

// ConsumeAll consumes all jobs from the queue and sends them to the job channel.
// It fetches all jobs with 'queued' status, sends them to the jobQueue, and then closes the channel.
// The method respects context cancellation and will stop processing if the context is cancelled.
func (t *SQLiteTransport) ConsumeAll(ctx context.Context, _ string, jobQueue chan *models.Job) error {
	defer close(jobQueue)

	rows, err := t.db.QueryContext(ctx, "SELECT id, name, payload, retry_count FROM jobs WHERE status = 'queued' ORDER BY created_at ASC")
	if err != nil {
		return fmt.Errorf("failed to query jobs: %w", err)
	}

	for rows.Next() {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var job models.Job
		if err = rows.Scan(&job.ID, &job.Name, &job.Payload, &job.RetryCount); err != nil {
			return fmt.Errorf("failed to scan job: %w", err)
		}

		select {
		case jobQueue <- &job:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("error iterating over rows: %w", err)
	}
	if err = rows.Close(); err != nil {
		return fmt.Errorf("error closing rows: %w", err)
	}

	return nil
}

// Close closes the transport
func (t *SQLiteTransport) Close(_ context.Context) error {
	return t.db.Close()
}
