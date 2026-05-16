package queue

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type Queue struct {
	client *redis.Client
}

func NewQueue(redisURL string) (*Queue, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse redis url: %w", err)
	}

	client := redis.NewClient(opts)

	err = client.Ping(context.Background()).Err()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	return &Queue{client: client}, nil
}

func (q *Queue) Enqueue(ctx context.Context, jobID string, priority int, scheduledAt time.Time) error {
	score := float64(priority)*-1e10 + float64(scheduledAt.Unix())

	err := q.client.ZAdd(ctx, "jobs:queue", redis.Z{
		Score:  score,
		Member: jobID,
	}).Err()
	if err != nil {
		return fmt.Errorf("failed to enqueue job: %w", err)
	}
	return nil
}

func (q *Queue) Dequeue(ctx context.Context) (string, error) {
	result, err := q.client.ZPopMin(ctx, "jobs:queue", 1).Result()
	if err != nil {
		return "", fmt.Errorf("failed to dequeue: %w", err)
	}
	if len(result) == 0 {
		return "", nil
	}
	return result[0].Member.(string), nil
}

func (q *Queue) SetHeartbeat(ctx context.Context, workerID string) error {
	err := q.client.Set(ctx, "worker:"+workerID, "alive", 30*time.Second).Err()
	if err != nil {
		return fmt.Errorf("failed to set heartbeat: %w", err)
	}
	return nil
}

func (q *Queue) GetActiveWorkers(ctx context.Context) ([]string, error) {
	keys, err := q.client.Keys(ctx, "worker:*").Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get workers: %w", err)
	}

	workers := []string{}
	for _, key := range keys {
		workerID := strings.TrimPrefix(key, "worker:")
		workers = append(workers, workerID)
	}

	return workers, nil
}

func (q *Queue) AcquireLock(ctx context.Context, jobID string, workerID string) (bool, error) {
	key := "lock:job:" + jobID
	result, err := q.client.SetNX(ctx, key, workerID, 60*time.Second).Result()
	if err != nil {
		return false, fmt.Errorf("failed t acquire lock: %w", err)
	}

	return result, nil
}

func (q *Queue) ReleaseLock(ctx context.Context, jobID string, workerID string) error {
	key := "lock:job" + jobID
	val, err := q.client.Get(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("failed to get lock: %w", err)
	}

	if val != workerID {
		return fmt.Errorf("lock not owned by this worker")
	}

	err = q.client.Del(ctx, key).Err()
	if err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}
	return nil
}

func (q *Queue) RenewLock(ctx context.Context, jobID string, workerID string) error {
	key := "lock:job:" + jobID
	val, err := q.client.Get(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("lock not found: %w", err)
	}
	if val != workerID {
		return fmt.Errorf("lock not owned by this worker")
	}
	err = q.client.Expire(ctx, key, 60*time.Second).Err()
	if err != nil {
		return fmt.Errorf("failed to renew lock: %w", err)
	}
	return nil
}
