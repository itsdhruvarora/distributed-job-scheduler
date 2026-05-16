package worker

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/itsdhruvarora/job-scheduler/internal/queue"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Worker struct {
	id    string
	db    *pgxpool.Pool
	queue *queue.Queue
}

func NewWorker(db *pgxpool.Pool, q *queue.Queue, id string) *Worker {
	return &Worker{id: id, db: db, queue: q}
}

func (w *Worker) ProcessNext(ctx context.Context) error {
	jobID, err := w.queue.Dequeue(ctx)
	if err != nil {
		return fmt.Errorf("failed to dequeue: %w", err)
	}
	if jobID == "" {
		return nil
	}

	acquired, err := w.queue.AcquireLock(ctx, jobID, w.id)
	if err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}
	if !acquired {
		log.Printf("job %s already locked by another worker, skipping", jobID)
		return nil
	}
	defer w.queue.ReleaseLock(ctx, jobID, w.id)

	// renew lock every 15s while executing
	lockCtx, cancelLock := context.WithCancel(ctx)
	defer cancelLock()
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-lockCtx.Done():
				return
			case <-ticker.C:
				w.queue.RenewLock(ctx, jobID, w.id)
			}
		}
	}()

	log.Printf("processing job: %s", jobID)

	_, err = w.db.Exec(ctx, `
		UPDATE jobs 
		SET status = 'RUNNING', attempts = attempts + 1, updated_at = NOW()
		WHERE id = $1
	`, jobID)
	if err != nil {
		return fmt.Errorf("failed to update job to running: %w", err)
	}

	err = w.execute(ctx, jobID)
	if err != nil {
		w.handleFailure(ctx, jobID)
		return fmt.Errorf("job %s failed: %w", jobID, err)
	}

	_, err = w.db.Exec(ctx, `
		UPDATE jobs SET status = 'DONE', updated_at = NOW()
		WHERE id = $1
	`, jobID)
	if err != nil {
		return fmt.Errorf("failed to update job to done: %w", err)
	}

	log.Printf("job %s completed successfully", jobID)
	return nil
}

func (w *Worker) handleFailure(ctx context.Context, jobID string) {
	var attempts, maxRetries int
	err := w.db.QueryRow(ctx, `
		SELECT attempts, max_retries FROM jobs WHERE id = $1
	`, jobID).Scan(&attempts, &maxRetries)
	if err != nil {
		log.Printf("failed to get job retry info: %v", err)
		return
	}

	if attempts >= maxRetries {
		w.db.Exec(ctx, `
        UPDATE jobs SET status = 'FAILED', updated_at = NOW()
        WHERE id = $1
    `, jobID)

		var payload []byte
		var jobType string
		w.db.QueryRow(ctx, `
        SELECT payload, type FROM jobs WHERE id = $1
    `, jobID).Scan(&payload, &jobType)

		w.db.Exec(ctx, `
        INSERT INTO dead_letter_queue (id, job_id, error, payload, job_type)
        VALUES ($1, $2, $3, $4, $5)
    `,
			uuid.New().String(),
			jobID,
			"job exhausted all retries",
			payload,
			jobType,
		)

		log.Printf("job %s exhausted retries, moved to DLQ", jobID)
		return
	}

	backoff := time.Duration(math.Pow(2, float64(attempts))) * time.Second
	jitter := time.Duration(rand.Intn(1000)) * time.Millisecond
	nextRun := time.Now().Add(backoff + jitter)

	w.db.Exec(ctx, `
		UPDATE jobs SET status = 'PENDING', scheduled_at = $1, updated_at = NOW()
		WHERE id = $2
	`, nextRun, jobID)

	w.queue.Enqueue(ctx, jobID, 5, nextRun)
	log.Printf("job %s failed, retrying at %v (attempt %d/%d)", jobID, nextRun, attempts, maxRetries)
}

func (w *Worker) execute(ctx context.Context, jobID string) error {
	log.Printf("executing job %s", jobID)
	return nil
}
