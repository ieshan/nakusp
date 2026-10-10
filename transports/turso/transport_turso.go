// Package turso provides a Turso-backed transport for Nakusp using the embedded
// turso.tech/database/tursogo engine (SQLite-compatible, no CGO required).
package turso

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ieshan/idx"
	"github.com/ieshan/nakusp/models"
	tursogo "turso.tech/database/tursogo"
)

const (
	statusQueued     = "queued"
	statusInProgress = "in_progress"
	statusDLQ        = "dlq"
)

// busyRetryInterval is how long claim retries wait after an MVCC write-write
// conflict surfaces as ErrTursoBusy (past the driver's internal busy timeout).
const busyRetryInterval = 50 * time.Millisecond

// TursoConfig holds configuration parameters for the TursoTransport.
type TursoConfig struct {
	// HeartbeatInterval is how often the worker's liveness is written to the workers table.
	HeartbeatInterval time.Duration

	// FetchInterval is how often Consume polls for new jobs when the queue is empty.
	FetchInterval time.Duration

	// LockDuration is how long a claimed job stays locked before it may be reclaimed.
	LockDuration time.Duration
}

func (c *TursoConfig) withDefaults() *TursoConfig {
	cfg := TursoConfig{}
	if c != nil {
		cfg = *c
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 5 * time.Minute
	}
	if cfg.FetchInterval <= 0 {
		cfg.FetchInterval = 5 * time.Second
	}
	if cfg.LockDuration <= 0 {
		cfg.LockDuration = 5 * time.Minute
	}
	return &cfg
}

// TursoTransport is a transport layer for Nakusp that uses an embedded Turso
// database as a backend. Turso uses MVCC rather than SQLite's single-writer
// locking, so claims commit before jobs are delivered — a losing write-write
// conflict can never produce a phantom delivery.
type TursoTransport struct {
	db     *sql.DB
	config *TursoConfig
}

var _ models.Transport = (*TursoTransport)(nil)

// NewTurso creates a new TursoTransport. The dsn is a local file path or
// ":memory:". It opens the database and creates the jobs and workers tables if
// they do not already exist.
func NewTurso(dsn string, config *TursoConfig) (*TursoTransport, error) {
	config = config.withDefaults()

	db, err := sql.Open("turso", dsn)
	if err != nil {
		return nil, fmt.Errorf("turso transport: open: %w", err)
	}

	// A single underlying connection keeps ":memory:" databases consistent
	// across the pool. Turso supports concurrent writes via MVCC, so this can
	// be relaxed for file-backed databases if contention becomes a bottleneck.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	// id and payload are BLOB: Turso TEXT values must be valid UTF-8, while
	// idx.ID binds as 16-byte binary and a job payload may hold arbitrary bytes.
	_, err = db.Exec(`
	CREATE TABLE IF NOT EXISTS jobs (
		id BLOB PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		payload BLOB,
		retry_count INTEGER NOT NULL,
		status TEXT NOT NULL,
		worker_id TEXT,
		locked_until DATETIME,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	`)
	if err != nil {
		return nil, fmt.Errorf("turso transport: create jobs table: %w", err)
	}

	_, err = db.Exec(`
	CREATE TABLE IF NOT EXISTS workers (
		id BLOB PRIMARY KEY,
		last_heartbeat DATETIME NOT NULL
	);
	`)
	if err != nil {
		return nil, fmt.Errorf("turso transport: create workers table: %w", err)
	}

	return &TursoTransport{db: db, config: config}, nil
}

// Publish adds a new job to the queue.
// It inserts a new record into the 'jobs' table with a 'queued' status.
func (t *TursoTransport) Publish(ctx context.Context, job *models.Job) error {
	_, err := t.db.ExecContext(
		ctx,
		"INSERT INTO jobs (id, name, payload, retry_count, status) VALUES (?, ?, ?, ?, ?)",
		job.ID, job.Name, []byte(job.Payload), job.RetryCount, statusQueued,
	)
	if err != nil {
		return fmt.Errorf("turso transport: publish: %w", err)
	}
	return nil
}

// Heartbeat updates the worker's last seen time.
// This is used to monitor worker health and re-queue jobs from workers that have gone offline.
// This method blocks until the context is cancelled.
func (t *TursoTransport) Heartbeat(ctx context.Context, id idx.ID) error {
	ticker := time.NewTicker(t.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		if _, err := t.db.ExecContext(
			ctx,
			"INSERT INTO workers (id, last_heartbeat) VALUES (?, ?) ON CONFLICT(id) DO UPDATE SET last_heartbeat = excluded.last_heartbeat",
			id, time.Now(),
		); err != nil {
			return fmt.Errorf("turso transport: heartbeat: %w", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Consume runs in a loop, continuously claiming queued jobs and sending them to
// jobQueue. Claims commit before delivery so concurrent consumers can never
// receive the same job. This method blocks until the context is cancelled.
func (t *TursoTransport) Consume(ctx context.Context, id idx.ID, jobQueue chan *models.Job) error {
	ticker := time.NewTicker(t.config.FetchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		job, err := t.claimOne(ctx, id)
		if err != nil {
			return err
		}
		if job != nil {
			select {
			case jobQueue <- job:
				continue
			case <-ctx.Done():
				// The claim already committed; return the job to the queue so
				// it is not stranded as in_progress.
				if err := t.Requeue(context.WithoutCancel(ctx), job); err != nil {
					return fmt.Errorf("turso transport: consume: %w", err)
				}
				return ctx.Err()
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// claimOne atomically marks the oldest queued job as in_progress under a
// per-call claim token and returns it, or nil if the queue is empty. The claim
// is a single autocommit UPDATE so it commits before the job is handed to the
// caller; an MVCC write-write conflict surfaces as ErrTursoBusy and is retried.
func (t *TursoTransport) claimOne(ctx context.Context, id idx.ID) (*models.Job, error) {
	claimID := fmt.Sprintf("%s:%s", id, idx.NewID())
	res, err := t.execClaim(
		ctx,
		`UPDATE jobs SET status = ?, worker_id = ?, locked_until = ?
		WHERE id = (SELECT id FROM jobs WHERE status = ? ORDER BY created_at ASC LIMIT 1)`,
		statusInProgress, claimID, time.Now().Add(t.config.LockDuration), statusQueued,
	)
	if err != nil {
		return nil, fmt.Errorf("turso transport: claim: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("turso transport: claim: %w", err)
	}
	if affected == 0 {
		return nil, nil
	}

	var job models.Job
	err = t.db.QueryRowContext(
		ctx,
		"SELECT id, name, payload, retry_count FROM jobs WHERE worker_id = ? AND status = ?",
		claimID, statusInProgress,
	).Scan(&job.ID, &job.Name, &job.Payload, &job.RetryCount)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("turso transport: claim: %w", err)
	}
	return &job, nil
}

// Requeue moves a job back to the 'queued' state, typically after a failed execution attempt.
func (t *TursoTransport) Requeue(ctx context.Context, job *models.Job) error {
	_, err := t.db.ExecContext(
		ctx,
		"UPDATE jobs SET status = ?, retry_count = ?, worker_id = NULL, locked_until = NULL WHERE id = ? AND status = ?",
		statusQueued, job.RetryCount, job.ID, statusInProgress,
	)
	if err != nil {
		return fmt.Errorf("turso transport: requeue: %w", err)
	}
	return nil
}

// SendToDLQ moves a job to the Dead Letter Queue after it has exceeded its max retry count.
func (t *TursoTransport) SendToDLQ(ctx context.Context, job *models.Job) error {
	_, err := t.db.ExecContext(ctx, "UPDATE jobs SET status = ? WHERE id = ?", statusDLQ, job.ID)
	if err != nil {
		return fmt.Errorf("turso transport: send to dlq: %w", err)
	}
	return nil
}

// Completed marks a job as completed by deleting it from the jobs table.
func (t *TursoTransport) Completed(ctx context.Context, job *models.Job) error {
	_, err := t.db.ExecContext(ctx, "DELETE FROM jobs WHERE id = ?", job.ID)
	if err != nil {
		return fmt.Errorf("turso transport: completed: %w", err)
	}
	return nil
}

// ConsumeAll claims all queued jobs under a per-call token and sends them to
// the jobQueue, then closes the channel. The claim is a single atomic UPDATE so
// concurrent consumers can never receive the same job. Jobs claimed but not yet
// sent when ctx is cancelled are returned to the 'queued' state; already
// delivered jobs keep their claim.
func (t *TursoTransport) ConsumeAll(ctx context.Context, id idx.ID, jobQueue chan *models.Job) error {
	defer close(jobQueue)

	claimID := fmt.Sprintf("%s:%s", id, idx.NewID())
	lockedUntil := time.Now().Add(t.config.LockDuration)

	res, err := t.execClaim(
		ctx,
		`UPDATE jobs SET status = ?, worker_id = ?, locked_until = ?
		WHERE id IN (SELECT id FROM jobs WHERE status = ? ORDER BY created_at ASC)`,
		statusInProgress, claimID, lockedUntil, statusQueued,
	)
	if err != nil {
		return fmt.Errorf("turso transport: consume all: claim: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("turso transport: consume all: claim: %w", err)
	}
	if affected == 0 {
		return nil
	}

	rows, err := t.db.QueryContext(
		ctx,
		"SELECT id, name, payload, retry_count FROM jobs WHERE worker_id = ? AND status = ? ORDER BY created_at ASC",
		claimID, statusInProgress,
	)
	if err != nil {
		t.requeueUndelivered(context.WithoutCancel(ctx), claimID, nil)
		return fmt.Errorf("turso transport: consume all: %w", err)
	}

	var delivered []idx.ID
	for rows.Next() {
		var job models.Job
		if err = rows.Scan(&job.ID, &job.Name, &job.Payload, &job.RetryCount); err != nil {
			_ = rows.Close()
			t.requeueUndelivered(context.WithoutCancel(ctx), claimID, delivered)
			return fmt.Errorf("turso transport: consume all: scan: %w", err)
		}
		select {
		case jobQueue <- &job:
			delivered = append(delivered, job.ID)
		case <-ctx.Done():
			// Close rows first — it holds the only connection.
			_ = rows.Close()
			t.requeueUndelivered(context.WithoutCancel(ctx), claimID, delivered)
			return ctx.Err()
		}
	}
	if err = rows.Err(); err != nil {
		_ = rows.Close()
		t.requeueUndelivered(context.WithoutCancel(ctx), claimID, delivered)
		return fmt.Errorf("turso transport: consume all: iterate: %w", err)
	}
	return rows.Close()
}

// execClaim runs a claim UPDATE, retrying while the engine reports a busy /
// MVCC write-write conflict. The driver already applies a busy timeout
// internally, so a returned ErrTursoBusy means the conflict outlasted it.
func (t *TursoTransport) execClaim(ctx context.Context, query string, args ...any) (sql.Result, error) {
	for {
		res, err := t.db.ExecContext(ctx, query, args...)
		if !errors.Is(err, tursogo.ErrTursoBusy) {
			return res, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(busyRetryInterval):
		}
	}
}

// requeueUndelivered returns a claim's not-yet-delivered in_progress jobs to
// the queued state. Errors are ignored on a best-effort basis.
func (t *TursoTransport) requeueUndelivered(ctx context.Context, claimID string, delivered []idx.ID) {
	query := "UPDATE jobs SET status = ?, worker_id = NULL, locked_until = NULL WHERE worker_id = ? AND status = ?"
	args := []any{statusQueued, claimID, statusInProgress}
	if len(delivered) > 0 {
		// #nosec G202 -- only '?' placeholders are concatenated; all values are bound args.
		query += " AND id NOT IN (" + strings.TrimSuffix(strings.Repeat("?,", len(delivered)), ",") + ")"
		for _, d := range delivered {
			args = append(args, d)
		}
	}
	_, _ = t.db.ExecContext(ctx, query, args...)
}

// Close closes the transport.
func (t *TursoTransport) Close(_ context.Context) error {
	return t.db.Close()
}
