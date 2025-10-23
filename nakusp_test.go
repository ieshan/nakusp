package main

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"os"
	"testing"
)

func getClient(t *testing.T) *redis.Client {
	opt, err := redis.ParseURL(os.Getenv("REDIS_URI"))
	if err != nil {
		t.Errorf("error was not expected in client: %s", err)
	}
	return redis.NewClient(opt)
}

func redisTeardown(client *redis.Client) {
	client.FlushAll(context.TODO())
}

func addInProgressTestJob(client *redis.Client, n *Nakusp, job Job) error {
	ctx := context.TODO()
	_, err := client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, n.Keys.InProgressTasks, 1, redis.KeepTTL)
		pipe.LPush(ctx, n.Keys.InProgressQueue, job.ID)
		pipe.HSet(ctx, n.Keys.TaskInfo, job.ID, n.GetPayload(job))
		pipe.HSet(ctx, n.Keys.TaskAttachedWorker, job.ID, n.id)
		return nil
	})
	return err
}

func TestPublish(t *testing.T) {
	client := getClient(t)

	ctx := context.TODO()
	n := New(client, nil)
	job, err := n.Publish(ctx, "test", "test")
	if err != nil {
		t.Errorf("error was not expected: %s", err)
	}

	payload, err := client.HGet(ctx, n.Keys.TaskInfo, job.ID).Result()
	if err != nil {
		t.Errorf("error was not expected: %s", err)
	}
	if payload != n.GetPayload(job) {
		t.Errorf("payload mismatched")
	}
	actualJobId, err := client.LPop(ctx, n.Keys.ToDoQueue).Result()
	if err != nil {
		t.Errorf("error was not expected: %s", err)
	}
	if actualJobId != job.ID {
		t.Errorf("job id mismatched, expected: %s, got: %s", job.ID, actualJobId)
	}

	redisTeardown(client)
}

func TestHeartbeat(t *testing.T) {
	client := getClient(t)
	ctx := context.TODO()
	n := New(client, nil)

	_, err := n.Heartbeat(ctx)
	if err != nil {
		t.Errorf("error was not expected: %s", err)
	}

	workerId, err := client.HGet(ctx, n.Keys.OnlineWorkers, n.id).Result()
	if err != nil {
		t.Errorf("error was not expected: %s", err)
	}
	if workerId != n.id {
		t.Errorf("worker id mismatched, expected: %s, got: %s", n.id, workerId)
	}

	redisTeardown(client)
}

func TestProcessCompleted(t *testing.T) {
	client := getClient(t)
	ctx := context.TODO()
	n := New(client, nil)

	job := Job{
		ID:         uuid.NewString(),
		Name:       "test",
		RetryCount: 0,
		Payload:    "test",
	}
	if err := addInProgressTestJob(client, n, job); err != nil {
		t.Errorf("error was not expected in creating in-progress test job: %s", err)
	}

	err := n.ProcessCompleted(ctx, job)
	if err != nil {
		t.Errorf("error was not expected: %s", err)
	}

	numInProgressTasks, err := client.Get(ctx, n.Keys.InProgressTasks).Result()
	if err != nil {
		t.Errorf("error was not expected: %s", err)
	}
	if numInProgressTasks != "0" {
		t.Errorf("number of in progress tasks mismatched, expected: 0, got: %s", numInProgressTasks)
	}

	inProgressCount, err := client.LLen(ctx, n.Keys.InProgressQueue).Result()
	if err != nil {
		t.Errorf("error was not expected: %s", err)
	}
	if inProgressCount != 0 {
		t.Errorf("in progress count mismatched, expected: 0, got: %d", inProgressCount)
	}

	_, err = client.HGet(ctx, n.Keys.TaskInfo, job.ID).Result()
	if !errors.Is(err, redis.Nil) {
		t.Errorf("error was not expected: %s", err)
	}

	_, err = client.HGet(ctx, n.Keys.TaskAttachedWorker, job.ID).Result()
	if !errors.Is(err, redis.Nil) {
		t.Errorf("error was not expected: %s", err)
	}

	redisTeardown(client)
}

func TestRequeueFailedJob(t *testing.T) {
	client := getClient(t)
	ctx := context.TODO()
	n := New(client, nil)

	job := Job{
		ID:         uuid.NewString(),
		Name:       "test",
		RetryCount: 0,
		Payload:    "test",
	}
	if err := addInProgressTestJob(client, n, job); err != nil {
		t.Errorf("error was not expected in creating in-progress test job: %s", err)
	}

	err := n.RequeueJob(ctx, job)
	if err != nil {
		t.Errorf("error was not expected: %s", err)
	}

	numInProgressTasks, err := client.Get(ctx, n.Keys.InProgressTasks).Result()
	if err != nil {
		t.Errorf("error was not expected: %s", err)
	}
	if numInProgressTasks != "0" {
		t.Errorf("number of in progress tasks mismatched, expected: 0, got: %s", numInProgressTasks)
	}

	actualJobId, err := client.LPop(ctx, n.Keys.ToDoQueue).Result()
	if err != nil {
		t.Errorf("error was not expected: %s", err)
	}
	if actualJobId != job.ID {
		t.Errorf("job id mismatched, expected: %s, got: %s", job.ID, actualJobId)
	}

	inProgressCount, err := client.LLen(ctx, n.Keys.InProgressQueue).Result()
	if err != nil {
		t.Errorf("error was not expected: %s", err)
	}
	if inProgressCount != 0 {
		t.Errorf("in progress count mismatched, expected: 0, got: %d", inProgressCount)
	}

	taskInfo, err := client.HGet(ctx, n.Keys.TaskInfo, job.ID).Result()
	if err != nil {
		t.Errorf("error was not expected: %s", err)
	}
	if taskInfo != n.GetPayload(job) {
		t.Errorf("task info mismatched, expected: %s, got: %s", n.GetPayload(job), taskInfo)
	}

	_, err = client.HGet(ctx, n.Keys.TaskAttachedWorker, job.ID).Result()
	if !errors.Is(err, redis.Nil) {
		t.Errorf("error was not expected: %s", err)
	}

	redisTeardown(client)
}

func TestSendToDLQ(t *testing.T) {
	client := getClient(t)
	ctx := context.TODO()
	n := New(client, nil)

	job := Job{
		ID:         uuid.NewString(),
		Name:       "test",
		RetryCount: 0,
		Payload:    "test",
	}
	if err := addInProgressTestJob(client, n, job); err != nil {
		t.Errorf("error was not expected in creating in-progress test job: %s", err)
	}

	if err := n.SendToDLQ(ctx, job); err != nil {
		t.Errorf("error was not expected: %s", err)
	}

	numInProgressTasks, err := client.Get(ctx, n.Keys.InProgressTasks).Result()
	if err != nil {
		t.Errorf("error was not expected: %s", err)
	}
	if numInProgressTasks != "0" {
		t.Errorf("number of in progress tasks mismatched, expected: 0, got: %s", numInProgressTasks)
	}

	inProgressCount, err := client.LLen(ctx, n.Keys.InProgressQueue).Result()
	if err != nil {
		t.Errorf("error was not expected: %s", err)
	}
	if inProgressCount != 0 {
		t.Errorf("in progress count mismatched, expected: 0, got: %d", inProgressCount)
	}

	dlqCount, err := client.LLen(ctx, n.Keys.DeadLetterQueue).Result()
	if err != nil {
		t.Errorf("error was not expected: %s", err)
	}
	if dlqCount != 1 {
		t.Errorf("in progress count mismatched, expected: 1, got: %d", dlqCount)
	}

	payload := n.GetPayload(job)
	actualPayload, err := client.LPop(ctx, n.Keys.DeadLetterQueue).Result()
	if err != nil {
		t.Errorf("error was not expected: %s", err)
	}
	if actualPayload != payload {
		t.Errorf("payload mismatched, expected: %s, got: %s", payload, actualPayload)
	}

	_, err = client.HGet(ctx, n.Keys.TaskInfo, job.ID).Result()
	if !errors.Is(err, redis.Nil) {
		t.Errorf("error was not expected: %s", err)
	}

	_, err = client.HGet(ctx, n.Keys.TaskAttachedWorker, job.ID).Result()
	if !errors.Is(err, redis.Nil) {
		t.Errorf("error was not expected: %s", err)
	}

	redisTeardown(client)
}

func TestExecuteJobRequeueIfJobFailed(t *testing.T) {
	client := getClient(t)
	ctx := context.TODO()
	n := New(client, nil)

	job := Job{
		ID:         uuid.NewString(),
		Name:       "test",
		RetryCount: 0,
		Payload:    "test",
	}
	if err := addInProgressTestJob(client, n, job); err != nil {
		t.Errorf("error was not expected in creating in-progress test job: %s", err)
	}

	_ = n.Add("test", 1, func(payload string) error {
		return errors.New("test error")
	})
	if err := n.ExecuteJob(ctx, job); err != nil {
		t.Errorf("error was not expected during job execution: %s", err)
	}

	numInProgressTasks, err := client.Get(ctx, n.Keys.InProgressTasks).Result()
	if err != nil {
		t.Errorf("error was not expected: %s", err)
	}
	if numInProgressTasks != "0" {
		t.Errorf("number of in progress tasks mismatched, expected: 0, got: %s", numInProgressTasks)
	}

	actualJobId, err := client.LPop(ctx, n.Keys.ToDoQueue).Result()
	if err != nil {
		t.Errorf("error was not expected: %s", err)
	}
	if actualJobId != job.ID {
		t.Errorf("job id mismatched, expected: %s, got: %s", job.ID, actualJobId)
	}

	inProgressCount, err := client.LLen(ctx, n.Keys.InProgressQueue).Result()
	if err != nil {
		t.Errorf("error was not expected: %s", err)
	}
	if inProgressCount != 0 {
		t.Errorf("in progress count mismatched, expected: 0, got: %d", inProgressCount)
	}

	taskInfo, err := client.HGet(ctx, n.Keys.TaskInfo, job.ID).Result()
	if err != nil {
		t.Errorf("error was not expected: %s", err)
	}
	retryPayload := n.GetPayload(Job{
		ID:         job.ID,
		Name:       job.Name,
		RetryCount: 1,
		Payload:    job.Payload,
	})
	if taskInfo != retryPayload {
		t.Errorf("task info mismatched, expected: %s, got: %s", retryPayload, taskInfo)
	}

	_, err = client.HGet(ctx, n.Keys.TaskAttachedWorker, job.ID).Result()
	if !errors.Is(err, redis.Nil) {
		t.Errorf("error was not expected: %s", err)
	}

	redisTeardown(client)
}

func TestExecuteJobSendToDLQIfMaxRetry(t *testing.T) {
	client := getClient(t)
	ctx := context.TODO()
	n := New(client, nil)

	job := Job{
		ID:         uuid.NewString(),
		Name:       "test",
		RetryCount: 1,
		Payload:    "test",
	}
	if err := addInProgressTestJob(client, n, job); err != nil {
		t.Errorf("error was not expected in creating in-progress test job: %s", err)
	}

	_ = n.Add("test", 1, func(payload string) error {
		return errors.New("test error")
	})
	if err := n.ExecuteJob(ctx, job); err != nil {
		t.Errorf("error was not expected during job execution: %s", err)
	}

	numInProgressTasks, err := client.Get(ctx, n.Keys.InProgressTasks).Result()
	if err != nil {
		t.Errorf("error was not expected: %s", err)
	}
	if numInProgressTasks != "0" {
		t.Errorf("number of in progress tasks mismatched, expected: 0, got: %s", numInProgressTasks)
	}

	inProgressCount, err := client.LLen(ctx, n.Keys.InProgressQueue).Result()
	if err != nil {
		t.Errorf("error was not expected: %s", err)
	}
	if inProgressCount != 0 {
		t.Errorf("in progress count mismatched, expected: 0, got: %d", inProgressCount)
	}

	dlqCount, err := client.LLen(ctx, n.Keys.DeadLetterQueue).Result()
	if err != nil {
		t.Errorf("error was not expected: %s", err)
	}
	if dlqCount != 1 {
		t.Errorf("in progress count mismatched, expected: 1, got: %d", dlqCount)
	}

	payload := n.GetPayload(job)
	actualPayload, err := client.LPop(ctx, n.Keys.DeadLetterQueue).Result()
	if err != nil {
		t.Errorf("error was not expected: %s", err)
	}
	if actualPayload != payload {
		t.Errorf("payload mismatched, expected: %s, got: %s", payload, actualPayload)
	}

	_, err = client.HGet(ctx, n.Keys.TaskInfo, job.ID).Result()
	if !errors.Is(err, redis.Nil) {
		t.Errorf("error was not expected: %s", err)
	}

	_, err = client.HGet(ctx, n.Keys.TaskAttachedWorker, job.ID).Result()
	if !errors.Is(err, redis.Nil) {
		t.Errorf("error was not expected: %s", err)
	}

	redisTeardown(client)
}

func TestExecuteJobCompleteJobIfSuccessful(t *testing.T) {
	client := getClient(t)
	ctx := context.TODO()
	n := New(client, nil)

	job := Job{
		ID:         uuid.NewString(),
		Name:       "test",
		RetryCount: 1,
		Payload:    "test",
	}
	if err := addInProgressTestJob(client, n, job); err != nil {
		t.Errorf("error was not expected in creating in-progress test job: %s", err)
	}

	_ = n.Add("test", 1, func(payload string) error {
		return nil
	})
	if err := n.ExecuteJob(ctx, job); err != nil {
		t.Errorf("error was not expected during job execution: %s", err)
	}

	numInProgressTasks, err := client.Get(ctx, n.Keys.InProgressTasks).Result()
	if err != nil {
		t.Errorf("error was not expected: %s", err)
	}
	if numInProgressTasks != "0" {
		t.Errorf("number of in progress tasks mismatched, expected: 0, got: %s", numInProgressTasks)
	}

	inProgressCount, err := client.LLen(ctx, n.Keys.InProgressQueue).Result()
	if err != nil {
		t.Errorf("error was not expected: %s", err)
	}
	if inProgressCount != 0 {
		t.Errorf("in progress count mismatched, expected: 0, got: %d", inProgressCount)
	}

	_, err = client.HGet(ctx, n.Keys.TaskInfo, job.ID).Result()
	if !errors.Is(err, redis.Nil) {
		t.Errorf("error was not expected: %s", err)
	}

	_, err = client.HGet(ctx, n.Keys.TaskAttachedWorker, job.ID).Result()
	if !errors.Is(err, redis.Nil) {
		t.Errorf("error was not expected: %s", err)
	}

	redisTeardown(client)
}

func TestFetchAndLock(t *testing.T) {
	client := getClient(t)
	ctx := context.TODO()
	n := New(client, nil)

	job, err := n.Publish(ctx, "test", "test")
	if err != nil {
		t.Errorf("error was not expected: %s", err)
	}

	if _, err := n.FetchAndLock(ctx); err != nil {
		t.Errorf("error was not expected: %s", err)
	}

	inProgressCount, err := client.LLen(ctx, n.Keys.InProgressQueue).Result()
	if err != nil {
		t.Errorf("error was not expected: %s", err)
	}
	if inProgressCount != 1 {
		t.Errorf("to do count mismatched, expected: 1, got: %d", inProgressCount)
	}

	actualJobId, err := client.LPop(ctx, n.Keys.InProgressQueue).Result()
	if err != nil {
		t.Errorf("error was not expected: %s", err)
	}
	if actualJobId != job.ID {
		t.Errorf("job id mismatched, expected: %s, got: %s", job.ID, actualJobId)
	}

	taskInfo, err := client.HGet(ctx, n.Keys.TaskInfo, job.ID).Result()
	if err != nil {
		t.Errorf("error was not expected: %s", err)
	}

	expectedPayload := n.GetPayload(Job{
		ID:         job.ID,
		Name:       job.Name,
		RetryCount: 0,
		Payload:    job.Payload,
	})
	if taskInfo != expectedPayload {
		t.Errorf("task info mismatched, expected: %s, got: %s", expectedPayload, taskInfo)
	}

	workerId, err := client.HGet(ctx, n.Keys.TaskAttachedWorker, job.ID).Result()
	if err != nil {
		t.Errorf("error was not expected: %s", err)
	}
	if workerId != n.ID() {
		t.Errorf("worker id mismatched, expected: %s, got: %s", n.ID(), workerId)
	}

	redisTeardown(client)
}
