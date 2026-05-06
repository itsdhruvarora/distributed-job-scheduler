package queue

import (
	"context"
	"fmt"
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
