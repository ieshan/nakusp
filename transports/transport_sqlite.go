package transports

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/ieshan/nakusp/models"
	_ "github.com/mattn/go-sqlite3"
)

// SQLiteTransport is a transport layer for Nakusp that uses SQLite as a backend.
// It provides a persistent job queue for background task processing.
type SQLiteTransport struct {
	db *sql.DB
}

// NewSQLite creates a new SQLiteTransport.
// It takes a path to the SQLite database file, initializes the database connection,
// and creates the necessary tables for jobs and workers if they don't already exist.
func NewSQLite(path string) (*SQLiteTransport, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}

	// Create jobs table
	_, err = db.Exec(`
	CREATE TABLE IF NOT EXISTS jobs (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		payload TEXT,
		retry_count INTEGER NOT NULL,
		status TEXT NOT NULL,
		worker_id TEXT,
		locked_until DATETIME
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

	return &SQLiteTransport{db: db}, nil
}

// Publish adds a new job to the queue.
// It inserts a new record into the 'jobs' table with a 'queued' status.
func (t *SQLiteTransport) Publish(ctx context.Context, job *models.Job) error {
	_, err := t.db.ExecContext(ctx, "INSERT INTO jobs (id, name, payload, retry_count, status) VALUES (?, ?, ?, ?, 'queued')",
		job.ID, job.Name, job.Payload, job.RetryCount)
	return err
}

// Heartbeat updates the worker's last seen time.
// This is used to monitor worker health and re-queue jobs from workers that have gone offline.
func (t *SQLiteTransport) Heartbeat(ctx context.Context, id string) error {
	_, err := t.db.ExecContext(ctx, "INSERT INTO workers (id, last_heartbeat) VALUES (?, ?) ON CONFLICT(id) DO UPDATE SET last_heartbeat = excluded.last_heartbeat", id, time.Now())
	return err
}

// Fetch retrieves a batch of jobs from the queue and assigns them to a worker.
// It locks the jobs to prevent other workers from processing them.
func (t *SQLiteTransport) Fetch(ctx context.Context, id string, jobQueue chan *models.Job) error {
	tx, err := t.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() // Rollback on any error

	rows, err := tx.QueryContext(ctx, "SELECT id, name, payload, retry_count FROM jobs WHERE status = 'queued' LIMIT 1")
	if err != nil {
		return err
	}
	defer rows.Close()

	var jobsToUpdate []string
	for rows.Next() {
		var job models.Job
		if err := rows.Scan(&job.ID, &job.Name, &job.Payload, &job.RetryCount); err != nil {
			return err
		}
		jobQueue <- &job
		jobsToUpdate = append(jobsToUpdate, job.ID)
	}

	if len(jobsToUpdate) > 0 {
		stmt, err := tx.PrepareContext(ctx, "UPDATE jobs SET status = 'in_progress', worker_id = ?, locked_until = ? WHERE id = ?")
		if err != nil {
			return err
		}
		defer stmt.Close()

		lockedUntil := time.Now().Add(5 * time.Minute) // 5-minute lock
		for _, jobID := range jobsToUpdate {
			if _, err := stmt.ExecContext(ctx, id, lockedUntil, jobID); err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

// Requeue moves a job back to the 'queued' state, typically after a failed execution attempt.
func (t *SQLiteTransport) Requeue(ctx context.Context, job *models.Job) error {
	_, err := t.db.ExecContext(ctx, "UPDATE jobs SET status = 'queued', retry_count = ?, worker_id = NULL, locked_until = NULL WHERE id = ?",
		job.RetryCount, job.ID)
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
