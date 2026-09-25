// Package postgres provides a PostgreSQL-backed transport for Nakusp using gorm.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ieshan/idx"
	"github.com/ieshan/nakusp/models"
	"github.com/ieshan/timi"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

const (
	statusQueued     = "queued"
	statusInProgress = "in_progress"
	statusDLQ        = "dlq"
)

// PostgresConfig holds configuration parameters for the PostgresTransport.
type PostgresConfig struct {
	// HeartbeatInterval is how often the worker's liveness is written to the workers table.
	HeartbeatInterval time.Duration

	// FetchInterval is how often Consume polls for new jobs when the queue is empty.
	FetchInterval time.Duration

	// LockDuration is how long a claimed job stays locked before it may be reclaimed.
	LockDuration time.Duration

	// Logger is the gorm logger used by the transport. Nil defaults to silent.
	Logger logger.Interface
}

func (c *PostgresConfig) withDefaults() *PostgresConfig {
	cfg := PostgresConfig{}
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
	if cfg.Logger == nil {
		cfg.Logger = logger.Default.LogMode(logger.Silent)
	}
	return &cfg
}

type jobRow struct {
	ID          idx.ID    `gorm:"primaryKey;type:bytea"`
	Name        string    `gorm:"type:varchar(255);not null"`
	Payload     string    `gorm:"type:text"`
	RetryCount  int       `gorm:"column:retry_count;not null"`
	Status      string    `gorm:"type:text;not null;index:idx_jobs_status_created,priority:1"`
	WorkerID    *string   `gorm:"column:worker_id;type:text"`
	LockedUntil timi.Time `gorm:"column:locked_until;type:timestamptz"`
	CreatedOn   timi.Time `gorm:"column:created_at;type:timestamptz;not null;index:idx_jobs_status_created,priority:2"`
}

func (jobRow) TableName() string { return "jobs" }

type workerRow struct {
	ID            idx.ID    `gorm:"primaryKey;type:bytea"`
	LastHeartbeat timi.Time `gorm:"column:last_heartbeat;type:timestamptz;not null"`
}

func (workerRow) TableName() string { return "workers" }

// PostgresTransport is a transport layer for Nakusp that uses PostgreSQL as a
// backend. Job claiming relies on SELECT ... FOR UPDATE SKIP LOCKED so that
// concurrent consumers can never receive the same job.
type PostgresTransport struct {
	db     *gorm.DB
	config *PostgresConfig
	ownsDB bool
}

var _ models.Transport = (*PostgresTransport)(nil)

// NewPostgres creates a new PostgresTransport from a PostgreSQL DSN such as
// "postgres://user:pass@host:5432/dbname?sslmode=disable". It opens its own
// connection pool and creates the jobs and workers tables if they do not
// exist.
func NewPostgres(dsn string, config *PostgresConfig) (*PostgresTransport, error) {
	config = config.withDefaults()
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger:                 config.Logger,
		SkipDefaultTransaction: true,
	})
	if err != nil {
		return nil, fmt.Errorf("postgres transport: open: %w", err)
	}
	return newTransport(db, config, true)
}

// NewPostgresFromDB creates a new PostgresTransport wrapping an existing
// *gorm.DB. The caller keeps ownership of the connection pool: Close is a
// no-op. The config's Logger is applied to the session used by the transport.
func NewPostgresFromDB(db *gorm.DB, config *PostgresConfig) (*PostgresTransport, error) {
	if db == nil {
		return nil, errors.New("postgres transport: db must not be nil")
	}
	config = config.withDefaults()
	db = db.Session(&gorm.Session{Logger: config.Logger})
	return newTransport(db, config, false)
}

func newTransport(db *gorm.DB, config *PostgresConfig, ownsDB bool) (*PostgresTransport, error) {
	if err := db.AutoMigrate(&jobRow{}, &workerRow{}); err != nil {
		return nil, fmt.Errorf("postgres transport: migrate: %w", err)
	}
	return &PostgresTransport{db: db, config: config, ownsDB: ownsDB}, nil
}

// Publish adds a new job to the queue.
// It inserts a new row into the 'jobs' table with a 'queued' status.
func (t *PostgresTransport) Publish(ctx context.Context, job *models.Job) error {
	row := jobRow{
		ID:          job.ID,
		Name:        job.Name,
		Payload:     job.Payload,
		RetryCount:  job.RetryCount,
		Status:      statusQueued,
		CreatedOn:   timi.Now(),
		LockedUntil: timi.NilTime,
	}
	if err := t.db.WithContext(ctx).Create(&row).Error; err != nil {
		return fmt.Errorf("postgres transport: publish: %w", err)
	}
	return nil
}

// Heartbeat updates the worker's last seen time.
// This is used to monitor worker health and re-queue jobs from workers that
// have gone offline. This method blocks until the context is cancelled.
func (t *PostgresTransport) Heartbeat(ctx context.Context, id idx.ID) error {
	ticker := time.NewTicker(t.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		row := workerRow{ID: id, LastHeartbeat: timi.Now()}
		err := t.db.WithContext(ctx).
			Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "id"}},
				DoUpdates: clause.AssignmentColumns([]string{"last_heartbeat"}),
			}).
			Create(&row).Error
		if err != nil {
			return fmt.Errorf("postgres transport: heartbeat: %w", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Consume runs in a loop, continuously claiming queued jobs and sending them to
// jobQueue. It uses SELECT ... FOR UPDATE SKIP LOCKED so concurrent consumers
// never claim the same job. This method blocks until the context is cancelled.
func (t *PostgresTransport) Consume(ctx context.Context, id idx.ID, jobQueue chan *models.Job) error {
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
					return fmt.Errorf("postgres transport: consume: %w", err)
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

// claimOne atomically marks the oldest queued job as in_progress for the given
// worker and returns it, or nil if the queue is empty. The claim commits before
// the job is handed to the caller, so no connection is held while the caller
// blocks on the job channel.
func (t *PostgresTransport) claimOne(ctx context.Context, id idx.ID) (job *models.Job, err error) {
	var row jobRow
	err = t.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if terr := tx.
			Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("status = ?", statusQueued).
			Order("created_at").
			Take(&row).Error; terr != nil {
			return terr
		}
		wid := id.String()
		return tx.Model(&row).Updates(map[string]any{
			"status":       statusInProgress,
			"worker_id":    wid,
			"locked_until": timi.Now().Add(t.config.LockDuration),
		}).Error
	})
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("postgres transport: claim: %w", err)
	}
	return &models.Job{ID: row.ID, Name: row.Name, Payload: row.Payload, RetryCount: row.RetryCount}, nil
}

// Requeue moves a job back to the 'queued' state, typically after a failed
// execution attempt.
func (t *PostgresTransport) Requeue(ctx context.Context, job *models.Job) error {
	err := t.db.WithContext(ctx).Model(&jobRow{}).
		Where("id = ? AND status = ?", job.ID, statusInProgress).
		Updates(map[string]any{
			"status":       statusQueued,
			"retry_count":  job.RetryCount,
			"worker_id":    nil,
			"locked_until": timi.NilTime,
		}).Error
	if err != nil {
		return fmt.Errorf("postgres transport: requeue: %w", err)
	}
	return nil
}

// SendToDLQ moves a job to the Dead Letter Queue after it has exceeded its max
// retry count.
func (t *PostgresTransport) SendToDLQ(ctx context.Context, job *models.Job) error {
	err := t.db.WithContext(ctx).Model(&jobRow{}).
		Where("id = ?", job.ID).
		Update("status", statusDLQ).Error
	if err != nil {
		return fmt.Errorf("postgres transport: send to dlq: %w", err)
	}
	return nil
}

// Completed marks a job as completed by deleting it from the jobs table.
func (t *PostgresTransport) Completed(ctx context.Context, job *models.Job) error {
	err := t.db.WithContext(ctx).Where("id = ?", job.ID).Delete(&jobRow{}).Error
	if err != nil {
		return fmt.Errorf("postgres transport: completed: %w", err)
	}
	return nil
}

// ConsumeAll claims all queued jobs under a per-call token and sends them to
// jobQueue, then closes the channel. The claim is a single transaction locking
// rows with FOR UPDATE SKIP LOCKED, so concurrent consumers can never receive
// the same job. Jobs claimed but not yet sent when ctx is cancelled are
// returned to the 'queued' state; already delivered jobs keep their claim.
func (t *PostgresTransport) ConsumeAll(ctx context.Context, id idx.ID, jobQueue chan *models.Job) error {
	defer close(jobQueue)

	claimID := fmt.Sprintf("%s:%s", id, idx.NewID())

	var ids []idx.ID
	err := t.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var rows []jobRow
		if terr := tx.
			Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("status = ?", statusQueued).
			Order("created_at").
			Find(&rows).Error; terr != nil {
			return terr
		}
		if len(rows) == 0 {
			return nil
		}
		ids = make([]idx.ID, len(rows))
		for i, r := range rows {
			ids[i] = r.ID
		}
		return tx.Model(&jobRow{}).
			Where("id IN ?", ids).
			Updates(map[string]any{
				"status":       statusInProgress,
				"worker_id":    claimID,
				"locked_until": timi.Now().Add(t.config.LockDuration),
			}).Error
	})
	if err != nil {
		return fmt.Errorf("postgres transport: consume all: claim: %w", err)
	}
	if len(ids) == 0 {
		return nil
	}

	rows, err := t.db.WithContext(ctx).Raw(
		"SELECT id, name, payload, retry_count FROM jobs WHERE worker_id = ? AND status = ? ORDER BY created_at",
		claimID, statusInProgress,
	).Rows()
	if err != nil {
		return fmt.Errorf("postgres transport: consume all: %w", err)
	}

	var delivered []idx.ID
	for rows.Next() {
		var job models.Job
		if err = rows.Scan(&job.ID, &job.Name, &job.Payload, &job.RetryCount); err != nil {
			_ = rows.Close()
			t.requeueUndelivered(context.WithoutCancel(ctx), claimID, delivered)
			return fmt.Errorf("postgres transport: consume all: scan: %w", err)
		}
		select {
		case jobQueue <- &job:
			delivered = append(delivered, job.ID)
		case <-ctx.Done():
			// Close rows first so the requeue cannot starve on a small pool.
			_ = rows.Close()
			t.requeueUndelivered(context.WithoutCancel(ctx), claimID, delivered)
			return ctx.Err()
		}
	}
	if err = rows.Err(); err != nil {
		_ = rows.Close()
		t.requeueUndelivered(context.WithoutCancel(ctx), claimID, delivered)
		return fmt.Errorf("postgres transport: consume all: iterate: %w", err)
	}
	return rows.Close()
}

// requeueUndelivered returns a claim's not-yet-delivered in_progress jobs to
// the queued state. Errors are ignored on a best-effort basis.
func (t *PostgresTransport) requeueUndelivered(ctx context.Context, claimID string, delivered []idx.ID) {
	tx := t.db.WithContext(ctx).Model(&jobRow{}).
		Where("worker_id = ? AND status = ?", claimID, statusInProgress)
	if len(delivered) > 0 {
		tx = tx.Not("id", delivered)
	}
	_ = tx.Updates(map[string]any{
		"status":       statusQueued,
		"worker_id":    nil,
		"locked_until": timi.NilTime,
	}).Error
}

// Close closes the transport. It is a no-op for transports created with
// NewPostgresFromDB, since the caller owns the connection pool.
func (t *PostgresTransport) Close(_ context.Context) error {
	if !t.ownsDB {
		return nil
	}
	sqlDB, err := t.db.DB()
	if err != nil {
		return fmt.Errorf("postgres transport: close: %w", err)
	}
	return sqlDB.Close()
}
