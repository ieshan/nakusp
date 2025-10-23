package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Handler struct {
	MaxRetry int
	Func     func(payload string) error
}

type NakuspConfig struct {
	MaxWorkers         int
	DefaultTaskRuntime int
	Logger             *slog.Logger
	HeartbeatPeriod    time.Duration
	GracefulTimeout    time.Duration
}

type NakuspKeys struct {
	Lock               string
	InProgressTasks    string
	ToDoQueue          string
	DeadLetterQueue    string
	InProgressQueue    string
	TaskInfo           string
	TaskAttachedWorker string
	OnlineWorkers      string
}

type Nakusp struct {
	id       string
	client   *redis.Client
	handlers map[string]Handler
	config   *NakuspConfig
	jobQueue chan Job
	wg       sync.WaitGroup
	Keys     NakuspKeys
}

type Job struct {
	ID         string
	Name       string
	RetryCount int
	Payload    string
}

func New(client *redis.Client, config *NakuspConfig) *Nakusp {
	if config == nil {
		config = &NakuspConfig{
			MaxWorkers:         10,
			DefaultTaskRuntime: 600,
			Logger:             slog.New(slog.NewJSONHandler(os.Stdout, nil)),
			HeartbeatPeriod:    time.Minute * 5,
			GracefulTimeout:    time.Second * 5,
		}
	}
	workerId := uuid.NewString()
	return &Nakusp{
		id:       workerId,
		client:   client,
		handlers: make(map[string]Handler),
		config:   config,
		jobQueue: make(chan Job, config.MaxWorkers),
		wg:       sync.WaitGroup{},
		Keys: NakuspKeys{
			Lock:               fmt.Sprintf("nakusp:worker:%s:lock", workerId),
			InProgressTasks:    fmt.Sprintf("nakusp:worker:%s:workers", workerId),
			ToDoQueue:          "nakusp:queue:todo",
			DeadLetterQueue:    "nakusp:queue:dlq",
			InProgressQueue:    "nakusp:queue:doing",
			TaskInfo:           "nakusp:tasks",
			TaskAttachedWorker: "nakusp:task:attached",
			OnlineWorkers:      "nakusp:workers",
		},
	}
}

func (n *Nakusp) ID() string {
	return n.id
}

func (n *Nakusp) Publish(ctx context.Context, jobName, payload string) (Job, error) {
	job := Job{
		ID:         uuid.NewString(),
		Name:       jobName,
		RetryCount: 0,
		Payload:    payload,
	}
	_, err := n.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, n.Keys.TaskInfo, job.ID, n.GetPayload(job))
		pipe.RPush(ctx, n.Keys.ToDoQueue, job.ID)
		return nil
	})
	return job, err
}

func (n *Nakusp) GetPayload(job Job) string {
	return fmt.Sprintf("%s:%s:%d:%s", job.ID, job.Name, job.RetryCount, job.Payload)
}

func (n *Nakusp) Add(taskName string, maxRetry int, handlerFunc func(payload string) error) error {
	n.handlers[taskName] = Handler{
		MaxRetry: maxRetry,
		Func:     handlerFunc,
	}
	return nil
}

func (n *Nakusp) RunUntilCancelled(ctx context.Context, handlerFn func(context.Context) (time.Duration, error)) {
	defer n.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			duration, err := handlerFn(ctx)
			if err != nil {
				n.config.Logger.Error("error in RunUntilCancelled", slog.String("error", err.Error()))
			}
			time.Sleep(duration)
		}
	}
}

func (n *Nakusp) Heartbeat(ctx context.Context) (time.Duration, error) {
	_, err := n.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSetNX(ctx, n.Keys.OnlineWorkers, n.id, n.id)
		pipe.HExpire(ctx, n.Keys.OnlineWorkers, time.Duration(n.config.DefaultTaskRuntime)*time.Second, n.id)
		return nil
	})
	if err != nil {
		return n.config.HeartbeatPeriod, err
	}

	return n.config.HeartbeatPeriod, nil
}

func (n *Nakusp) ProcessCompleted(ctx context.Context, job Job) error {
	_, err := n.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Decr(ctx, n.Keys.InProgressTasks)
		pipe.LRem(ctx, n.Keys.InProgressQueue, 1, job.ID)
		pipe.HDel(ctx, n.Keys.TaskInfo, job.ID)
		pipe.HDel(ctx, n.Keys.TaskAttachedWorker, job.ID)
		return nil
	})
	return err
}

func (n *Nakusp) RequeueJob(ctx context.Context, job Job) error {
	// Increase retry count

	_, err := n.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Decr(ctx, n.Keys.InProgressTasks)
		pipe.RPush(ctx, n.Keys.ToDoQueue, job.ID)
		pipe.LRem(ctx, n.Keys.InProgressQueue, 1, job.ID)
		pipe.HSet(ctx, n.Keys.TaskInfo, job.ID, n.GetPayload(job))
		pipe.HDel(ctx, n.Keys.TaskAttachedWorker, job.ID)
		return nil
	})
	return err
}

func (n *Nakusp) SendToDLQ(ctx context.Context, job Job) error {
	_, err := n.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Decr(ctx, n.Keys.InProgressTasks)
		pipe.RPush(ctx, n.Keys.DeadLetterQueue, n.GetPayload(job))
		pipe.LRem(ctx, n.Keys.InProgressQueue, 1, job.ID)
		pipe.HDel(ctx, n.Keys.TaskInfo, job.ID)
		pipe.HDel(ctx, n.Keys.TaskAttachedWorker, job.ID)
		return nil
	})
	return err
}

func (n *Nakusp) ExecuteJob(ctx context.Context, job Job) error {
	var err error
	var redisErr error

	select {
	case <-ctx.Done():
		redisErr = n.RequeueJob(ctx, job)
		return redisErr
	default:
		hf, ok := n.handlers[job.Name]
		if !ok {
			n.config.Logger.Error("handler not found", slog.String("task", job.Name))
			return nil
		}

		err = hf.Func(job.Payload)
		if err != nil {
			if job.RetryCount < hf.MaxRetry {
				job.RetryCount++
				redisErr = n.RequeueJob(ctx, job)
			} else {
				redisErr = n.SendToDLQ(ctx, job)
			}
		} else {
			redisErr = n.ProcessCompleted(ctx, job)
		}
		return redisErr
	}
}

func (n *Nakusp) FetchAndLock(ctx context.Context) (time.Duration, error) {
	duration := time.Second
	tasks, err := fetchTasks.Run(
		ctx,
		n.client,
		[]string{
			n.Keys.Lock,
			n.Keys.InProgressTasks,
			n.Keys.ToDoQueue,
			n.Keys.InProgressQueue,
			n.Keys.TaskInfo,
			n.Keys.TaskAttachedWorker,
		},
		[]interface{}{
			n.id,
			n.config.MaxWorkers,
			n.config.DefaultTaskRuntime,
		},
	).StringSlice()
	if err != nil {
		return duration, err
	}

	for _, task := range tasks {
		parts := strings.Split(task, ":")
		n.jobQueue <- Job{
			ID:      parts[0],
			Name:    parts[1],
			Payload: parts[2],
		}
	}
	return time.Duration(n.config.MaxWorkers-len(tasks)) * time.Second, nil
}

func (n *Nakusp) StartWorker() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGABRT, syscall.SIGKILL, syscall.SIGTERM)

	n.wg.Add(2)
	go n.RunUntilCancelled(ctx, n.Heartbeat)
	go n.RunUntilCancelled(ctx, n.FetchAndLock)

	for {
		select {
		case job := <-n.jobQueue:
			n.wg.Add(1)
			go func(job Job) {
				defer n.wg.Done()
				if err := n.ExecuteJob(ctx, job); err != nil {
					n.config.Logger.Error("job execution error", slog.String("error", err.Error()))
				}
			}(job)
		case <-sigChan:
			n.config.Logger.Debug("got exit signal")
			cancel()
			n.wg.Wait()
			close(n.jobQueue)
			return nil
		}
	}
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
	local task_len = redis.call("SET", in_progress_tasks, "0", "NX", "GET")
	if not task_len then
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
