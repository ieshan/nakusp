package transports

import (
	"context"
	"strings"
	"time"

	"github.com/ieshan/nakusp/models"
	"github.com/redis/go-redis/v9"
)

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
	config *models.Config
}

// NewRedis creates a new RedisTransport.
// It requires a Redis client, a set of keys for Redis operations, and a configuration.
// If keys or config are nil, it falls back to default values.
func NewRedis(client *redis.Client, keys *RedisKeys, config *models.Config) *RedisTransport {
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
		config = &models.Config{
			MaxWorkers:         5,
			DefaultTaskRuntime: 600,
			HeartbeatPeriod:    5 * time.Minute,
			GracefulTimeout:    10 * time.Second,
		}
	}
	return &RedisTransport{
		keys:   keys,
		client: client,
		config: config,
	}
}

// Publish adds a new job to the Redis 'todo' queue and stores its details in a hash.
func (t *RedisTransport) Publish(ctx context.Context, job *models.Job) error {
	_, err := t.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, t.keys.TaskInfo, job.ID, GetPayload(job))
		pipe.RPush(ctx, t.keys.ToDoQueue, job.ID)
		return nil
	})
	return err
}

// Heartbeat updates a worker's status in Redis, indicating that it is still alive.
// This is done by periodically updating a key with an expiration time.
func (t *RedisTransport) Heartbeat(ctx context.Context, id string) error {
	_, err := t.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSetNX(ctx, t.keys.OnlineWorkers, id, id)
		pipe.HExpire(ctx, t.keys.OnlineWorkers, 20*time.Minute, id)
		return nil
	})
	return err
}

// Fetch retrieves jobs from the 'todo' queue using a Lua script for atomicity.
// It moves jobs to an 'in_progress' queue and assigns them to the worker.
func (t *RedisTransport) Fetch(ctx context.Context, id string, jobQueue chan *models.Job) error {
	tasks, err := fetchTasks.Run(
		ctx,
		t.client,
		[]string{
			t.keys.Lock,
			t.keys.InProgressTasks,
			t.keys.ToDoQueue,
			t.keys.InProgressQueue,
			t.keys.TaskInfo,
			t.keys.TaskAttachedWorker,
		},
		[]interface{}{
			id,
			t.config.MaxWorkers,
			t.config.DefaultTaskRuntime,
		},
	).StringSlice()
	if err != nil {
		return err
	}

	for _, task := range tasks {
		parts := strings.Split(task, ":")
		jobQueue <- &models.Job{
			ID:      parts[0],
			Name:    parts[1],
			Payload: parts[2],
		}
	}
	return nil
}

// Requeue moves a job from the 'in_progress' queue back to the 'todo' queue.
func (t *RedisTransport) Requeue(ctx context.Context, job *models.Job) error {
	_, err := t.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Decr(ctx, t.keys.InProgressTasks)
		pipe.RPush(ctx, t.keys.ToDoQueue, job.ID)
		pipe.LRem(ctx, t.keys.InProgressQueue, 1, job.ID)
		pipe.HSet(ctx, t.keys.TaskInfo, job.ID, GetPayload(job))
		pipe.HDel(ctx, t.keys.TaskAttachedWorker, job.ID)
		return nil
	})
	return err
}

// SendToDLQ moves a job to the Dead Letter Queue in Redis.
func (t *RedisTransport) SendToDLQ(ctx context.Context, job *models.Job) error {
	_, err := t.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Decr(ctx, t.keys.InProgressTasks)
		pipe.RPush(ctx, t.keys.DeadLetterQueue, GetPayload(job))
		pipe.LRem(ctx, t.keys.InProgressQueue, 1, job.ID)
		pipe.HDel(ctx, t.keys.TaskInfo, job.ID)
		pipe.HDel(ctx, t.keys.TaskAttachedWorker, job.ID)
		return nil
	})
	return err
}

// Completed removes a job from the 'in_progress' queue and its associated data from Redis.
func (t *RedisTransport) Completed(ctx context.Context, job *models.Job) error {
	_, err := t.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Decr(ctx, t.keys.InProgressTasks)
		pipe.LRem(ctx, t.keys.InProgressQueue, 1, job.ID)
		pipe.HDel(ctx, t.keys.TaskInfo, job.ID)
		pipe.HDel(ctx, t.keys.TaskAttachedWorker, job.ID)
		return nil
	})
	return err
}

var fetchTasks = redis.NewScript(`
local lock_key = KEYS[1]
local in_progress_tasks = KEYS[2]
local todo_queue = KEYS[3]
local in_progress_queue = KEYS[4]
local task_info_hash = KEYS[5]
local attached_worker_hash = KEYS[6]

local worker_id = ARGV[1]
local max_tasks = tonumber(ARGV[2])
local task_runtime = ARGV[3]
local tasks = {}

local function getTaskId(str)
	local pos = str:find(":", 1)
	if not pos then
		return
	end
	return str:sub(1, str:find(":", 1) - 1)
end

-- If we can acquire lock, execute the logic
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
			local task_id = redis.call("LMOVE", todo_queue, in_progress_queue, "RIGHT", "RIGHT")
			if task_id then
				lock_incr = lock_incr + 1
				redis.log(redis.LOG_NOTICE, "task_info_hash", task_info_hash, "task_id", task_id)
				local task = redis.call("HGET", task_info_hash, task_id)
				table.insert(tasks, task)
				--local task_id = getTaskId(task)
				redis.call("HSET", attached_worker_hash, task_id, worker_id)
				redis.call("HEXPIRE", attached_worker_hash, task_runtime, "FIELDS", 1, task_id)
			end
		end

		if lock_incr > 0 then
			redis.call("INCRBY", in_progress_tasks, lock_incr)
		end
	end
	-- release lock
	redis.call("DEL", lock_key)
end

return tasks
`)
