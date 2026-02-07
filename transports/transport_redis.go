package transports

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ieshan/idx"
	"github.com/ieshan/nakusp/models"
	"github.com/redis/go-redis/v9"
)

type RedisConfig struct {
	MaxWorkers         int
	DefaultTaskRuntime int
	HeartbeatInternal  time.Duration
	FetchInterval      time.Duration
}

// RedisKeys defines the set of keys used by the RedisTransport in a Redis database.
// This allows for namespacing and prevents key collisions.
type RedisKeys struct {
	Lock               string
	InProgressTasks    string
	ToDoQueue          string
	DeadLetterQueue    string
	InProgressQueue    string
	TaskInfo           string
	TaskAttachedWorker string
	OnlineWorkers      string
}

// RedisTransport is a transport layer for Nakusp that uses Redis as a backend.
// It leverages Redis lists and hashes to create a robust job queue.
type RedisTransport struct {
	keys   *RedisKeys
	client *redis.Client
	config *RedisConfig
}

// NewRedis creates a new RedisTransport.
// It requires a Redis client, a set of keys for Redis operations, and a configuration.
// If keys or config are nil, it falls back to default values.
func NewRedis(client *redis.Client, keys *RedisKeys, config *RedisConfig) *RedisTransport {
	if keys == nil {
		keys = &RedisKeys{
			Lock:               "nakusp:lock",
			InProgressTasks:    "nakusp:in_progress_tasks",
			ToDoQueue:          "nakusp:todo_queue",
			DeadLetterQueue:    "nakusp:dead_letter_queue",
			InProgressQueue:    "nakusp:in_progress_queue",
			TaskInfo:           "nakusp:task_info",
			TaskAttachedWorker: "nakusp:task_attached_worker",
			OnlineWorkers:      "nakusp:online_workers",
		}
	}
	if config == nil {
		config = &RedisConfig{
			MaxWorkers:         5,
			DefaultTaskRuntime: 600,
			HeartbeatInternal:  5 * time.Minute,
			FetchInterval:      5 * time.Second,
		}
	}
	return &RedisTransport{
		keys:   keys,
		client: client,
		config: config,
	}
}

// Publish adds a new job to the Redis 'todo' queue.
func (t *RedisTransport) Publish(ctx context.Context, job *models.Job) error {
	return t.client.RPush(ctx, t.keys.ToDoQueue, GetPayload(job)).Err()
}

// Heartbeat updates a worker's status in Redis, indicating that it is still alive.
// It runs in a loop, sending heartbeats at intervals defined by HeartbeatPeriod.
// The method blocks until the context is cancelled.
func (t *RedisTransport) Heartbeat(ctx context.Context, id idx.ID) error {
	ticker := time.NewTicker(t.config.HeartbeatInternal)
	defer ticker.Stop()

	for {
		if err := t.sendHeartbeat(ctx, id); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// sendHeartbeat sends a single heartbeat to Redis.
func (t *RedisTransport) sendHeartbeat(ctx context.Context, id idx.ID) error {
	_, err := t.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSetNX(ctx, t.keys.OnlineWorkers, id.String(), id)
		pipe.HExpire(ctx, t.keys.OnlineWorkers, 20*time.Minute, id.String())
		return nil
	})
	return err
}

// Consume retrieves jobs from the 'todo' queue using a Lua script for atomicity.
// It continuously polls for jobs and sends them to the jobQueue channel.
// The method blocks until the context is cancelled.
func (t *RedisTransport) Consume(ctx context.Context, id idx.ID, jobQueue chan *models.Job) error {
	const fetchInterval = 200 * time.Millisecond
	ticker := time.NewTicker(fetchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fetched, err := t.fetchAndProcessTasks(ctx, id.String(), jobQueue)
		if err != nil {
			return err
		}

		// If jobs were fetched, continue immediately to check for more
		if fetched > 0 {
			continue
		}

		// No jobs available, wait before polling again
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// ConsumeAll fetches all available jobs from the 'todo' queue and sends them to the jobQueue.
// It continues to fetch jobs until the queue is empty, and then closes the jobQueue channel.
func (t *RedisTransport) ConsumeAll(ctx context.Context, id idx.ID, jobQueue chan *models.Job) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fetched, err := t.fetchAndProcessTasks(ctx, id.String(), jobQueue)
		if err != nil {
			return err
		}

		// If jobs were fetched, continue immediately to check for more
		if fetched > 0 {
			continue
		} else {
			break
		}
	}
	close(jobQueue)
	return nil
}

// fetchAndProcessTasks fetches jobs from Redis and sends them to the job queue.
// Returns the number of jobs fetched.
func (t *RedisTransport) fetchAndProcessTasks(ctx context.Context, id string, jobQueue chan *models.Job) (int, error) {
	var err error
	var job *models.Job
	tasks, err := fetchTasks.Run(
		ctx,
		t.client,
		[]string{
			t.keys.Lock,
			t.keys.InProgressTasks,
			t.keys.ToDoQueue,
			t.keys.InProgressQueue,
			t.keys.TaskAttachedWorker,
		},
		[]interface{}{
			id,
			t.config.MaxWorkers,
		},
	).StringSlice()
	if err != nil {
		return 0, err
	}

	count := 0
	for _, task := range tasks {
		job, err = parseJobPayload(task)
		if err != nil {
			// Skip malformed jobs
			continue
		}
		jobQueue <- job
		count++
	}

	return count, nil
}

// parseJobPayload parses a job payload string into a Job struct.
func parseJobPayload(payload string) (*models.Job, error) {
	parts := strings.SplitN(payload, ":", 3)
	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid job payload format: %s", payload)
	}
	jobId, err := idx.FromString(parts[0])
	if err != nil {
		return nil, err
	}
	return &models.Job{
		ID:      jobId,
		Name:    parts[1],
		Payload: parts[2],
	}, nil
}

// Requeue moves a job from the 'in_progress' queue back to the 'todo' queue.
func (t *RedisTransport) Requeue(ctx context.Context, job *models.Job) error {
	payload := GetPayload(job)
	_, err := t.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Decr(ctx, t.keys.InProgressTasks)
		pipe.RPush(ctx, t.keys.ToDoQueue, payload)
		pipe.LRem(ctx, t.keys.InProgressQueue, 1, payload)
		pipe.HDel(ctx, t.keys.TaskAttachedWorker, job.ID.String())
		return nil
	})
	return err
}

// SendToDLQ moves a job to the Dead Letter Queue in Redis.
func (t *RedisTransport) SendToDLQ(ctx context.Context, job *models.Job) error {
	payload := GetPayload(job)
	_, err := t.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Decr(ctx, t.keys.InProgressTasks)
		pipe.RPush(ctx, t.keys.DeadLetterQueue, payload)
		pipe.LRem(ctx, t.keys.InProgressQueue, 1, payload)
		pipe.HDel(ctx, t.keys.TaskAttachedWorker, job.ID.String())
		return nil
	})
	return err
}

// Completed removes a job from the 'in_progress' queue and its associated data from Redis.
func (t *RedisTransport) Completed(ctx context.Context, job *models.Job) error {
	payload := GetPayload(job)
	_, err := t.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Decr(ctx, t.keys.InProgressTasks)
		pipe.LRem(ctx, t.keys.InProgressQueue, 1, payload)
		pipe.HDel(ctx, t.keys.TaskAttachedWorker, job.ID.String())
		return nil
	})
	return err
}

// Close closes the transport
func (t *RedisTransport) Close(_ context.Context) error {
	return nil
}

var fetchTasks = redis.NewScript(`
local lock_key = KEYS[1]
local in_progress_tasks = KEYS[2]
local todo_queue = KEYS[3]
local in_progress_queue = KEYS[4]
local attached_worker_hash = KEYS[5]

local worker_id = ARGV[1]
local max_tasks = tonumber(ARGV[2])
local tasks = {}

local function getTaskId(str)
	if not str then return nil end
	local pos = str:find(":", 1)
	if not pos then return nil end
	return str:sub(1, pos - 1)
end

-- Acquire lock with 10 second expiration
if redis.call("SET", lock_key, "1", "NX", "EX", 10) then
	local task_len = redis.call("GET", in_progress_tasks)
	if not task_len then
		redis.call("SET", in_progress_tasks, "0")
		task_len = 0
	else
		task_len = tonumber(task_len)
	end
	
	if task_len < max_tasks then
		local lock_incr = 0
		for i = 1, max_tasks - task_len do
			local task = redis.call("LMOVE", todo_queue, in_progress_queue, "LEFT", "RIGHT")
			if task then
				lock_incr = lock_incr + 1
				table.insert(tasks, task)
				local task_id = getTaskId(task)
				if task_id then
					redis.call("HSET", attached_worker_hash, task_id, worker_id)
				end
			end
		end
		
		if lock_incr > 0 then
			redis.call("INCRBY", in_progress_tasks, lock_incr)
		end
	end
	
	-- Release lock
	redis.call("DEL", lock_key)
end

return tasks
`)
