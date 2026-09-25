package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/ieshan/idx"
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

	// Ensure a single underlying connection to avoid writer contention locks.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

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
func (t *SQLiteTransport) Heartbeat(ctx context.Context, id idx.ID) error {
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
func (t *SQLiteTransport) Consume(ctx context.Context, id idx.ID, jobQueue chan *models.Job) error {
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

func (t *SQLiteTransport) fetchOnce(ctx context.Context, id idx.ID, jobQueue chan *models.Job) (fetched bool, err error) {
	tx, err := t.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	lockedUntil := time.Now().Add(5 * time.Minute) // 5-minute lock
	row := tx.QueryRowContext(
		ctx,
		`WITH next_job AS (
			SELECT id, name, payload, retry_count
			FROM jobs
			WHERE status = 'queued'
			ORDER BY created_at ASC
			LIMIT 1
		)
		UPDATE jobs
		SET status = 'in_progress', worker_id = ?, locked_until = ?
		WHERE id IN (SELECT id FROM next_job)
		RETURNING id, name, payload, retry_count`,
		id, lockedUntil,
	)

	var job models.Job
	if scanErr := row.Scan(&job.ID, &job.Name, &job.Payload, &job.RetryCount); scanErr != nil {
		if errors.Is(scanErr, sql.ErrNoRows) {
			if err = tx.Commit(); err != nil {
				return false, err
			}
			return false, nil
		}
		return false, scanErr
	}

	select {
	case jobQueue <- &job:
		fetched = true
	case <-ctx.Done():
		return false, ctx.Err()
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
		"UPDATE jobs SET status = 'queued', retry_count = ?, worker_id = NULL, locked_until = NULL WHERE id = ? AND status = 'in_progress'",
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

// ConsumeAll claims all queued jobs under a per-call token and sends them to
// the jobQueue, then closes the channel. The claim is a single atomic UPDATE so
// concurrent consumers can never receive the same job. Jobs claimed but not yet
// sent when ctx is cancelled are returned to the 'queued' state.
func (t *SQLiteTransport) ConsumeAll(ctx context.Context, id idx.ID, jobQueue chan *models.Job) error {
	defer close(jobQueue)

	claimID := fmt.Sprintf("%s:%s", id, idx.NewID())
	lockedUntil := time.Now().Add(5 * time.Minute)

	rows, err := t.db.QueryContext(
		ctx,
		`WITH next_jobs AS (
			SELECT id FROM jobs WHERE status = 'queued' ORDER BY created_at ASC
		)
		UPDATE jobs
		SET status = 'in_progress', worker_id = ?, locked_until = ?
		WHERE id IN (SELECT id FROM next_jobs)
		RETURNING id, name, payload, retry_count`,
		claimID, lockedUntil,
	)
	if err != nil {
		return fmt.Errorf("failed to claim queued jobs: %w", err)
	}

	for rows.Next() {
		var job models.Job
		if err = rows.Scan(&job.ID, &job.Name, &job.Payload, &job.RetryCount); err != nil {
			_ = rows.Close()
			return fmt.Errorf("failed to scan job: %w", err)
		}
		select {
		case jobQueue <- &job:
		case <-ctx.Done():
			// Close rows first — it holds the only connection.
			_ = rows.Close()
			t.requeueClaimed(context.WithoutCancel(ctx), claimID)
			return ctx.Err()
		}
	}
	if err = rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("error iterating over rows: %w", err)
	}
	return rows.Close()
}

// requeueClaimed returns a claim's unprocessed in_progress jobs to queued.
func (t *SQLiteTransport) requeueClaimed(ctx context.Context, claimID string) {
	_, _ = t.db.ExecContext(
		ctx,
		`UPDATE jobs SET status = 'queued', worker_id = NULL, locked_until = NULL WHERE status = 'in_progress' AND worker_id = ?`,
		claimID,
	)
}

// Close closes the transport
func (t *SQLiteTransport) Close(_ context.Context) error {
	return t.db.Close()
}
