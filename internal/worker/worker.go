package worker

import (
	"context"
	"fmt"
	"log"

	"github.com/itsdhruvarora/job-scheduler/internal/queue"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Worker struct {
	db    *pgxpool.Pool
	queue *queue.Queue
}

func NewWorker(db *pgxpool.Pool, q *queue.Queue) *Worker {
	return &Worker{db: db, queue: q}
}

func (w *Worker) ProcessNext(ctx context.Context) error {
	jobID, err := w.queue.Dequeue(ctx)
	if err != nil {
		return fmt.Errorf("failed to dequeue: %w", err)
	}
	if jobID == "" {
		return nil
	}

	log.Printf("processing job: %s", jobID)

	_, err = w.db.Exec(ctx, `
		UPDATE jobs SET status = 'RUNNING', updated_at = NOW()
		WHERE id = $1
	`, jobID)
	if err != nil {
		return fmt.Errorf("failed to update job status to running: %w", err)
	}

	// execute the job
	err = w.execute(ctx, jobID)

	if err != nil {
		w.db.Exec(ctx, `
			UPDATE jobs SET status = 'FAILED', updated_at = NOW()
			WHERE id = $1
		`, jobID)
		return fmt.Errorf("job %s failed: %w", jobID, err)
	}

	_, err = w.db.Exec(ctx, `
		UPDATE jobs SET status = 'DONE', updated_at = NOW()
		WHERE id = $1
	`, jobID)
	if err != nil {
		return fmt.Errorf("failed to update job status to done: %w", err)
	}

	log.Printf("job %s completed successfully", jobID)
	return nil
}

func (w *Worker) execute(ctx context.Context, jobID string) error {
	log.Printf("executing job %s", jobID)
	return nil
}
