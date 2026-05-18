package scheduler

import (
	"context"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/itsdhruvarora/job-scheduler/internal/job"
	"github.com/itsdhruvarora/job-scheduler/internal/queue"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"
)

type Scheduler struct {
	db    *pgxpool.Pool
	queue *queue.Queue
	store *job.Store
}

func NewScheduler(db *pgxpool.Pool, q *queue.Queue, store *job.Store) *Scheduler {
	return &Scheduler{db: db, queue: q, store: store}
}

func (s *Scheduler) Start(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	log.Println("Scheduler started, polling every 5s")

	for {
		select {
		case <-ctx.Done():
			log.Println("Scheduler Stopped")
			return
		case <-ticker.C:
			s.enqueueDueJobs(ctx)
		}
	}
}
func (s *Scheduler) enqueueDueJobs(ctx context.Context) {
	rows, err := s.db.Query(ctx, `
		SELECT id, priority, next_run_at, cron_expression
		FROM jobs
		WHERE cron_expression IS NOT NULL
		AND next_run_at <= NOW()
		AND status = 'DONE'
	`)
	if err != nil {
		log.Printf("scheduler: failed to query due jobs: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var id string
		var priority int
		var nextRunAt time.Time
		var cronExpr string

		err := rows.Scan(&id, &priority, &nextRunAt, &cronExpr)
		if err != nil {
			log.Printf("scheduler: failed to scan job: %v", err)
			continue
		}

		schedule, err := cron.ParseStandard(cronExpr)
		if err != nil {
			log.Printf("scheduler: invalid cron expression %s: %v", cronExpr, err)
			continue
		}

		nextRun := schedule.Next(time.Now())

		newJobID := uuid.New().String()
		now := time.Now()
		newJob := job.Job{
			ID:             newJobID,
			Type:           "cron",
			Payload:        nil,
			Status:         job.StatusPending,
			Priority:       priority,
			MaxRetries:     3,
			DependsOn:      []string{},
			CronExpression: nil,
			NextRunAt:      nil,
			ScheduledAt:    nextRun,
			CreatedAt:      now,
			UpdatedAt:      now,
		}

		err = s.store.Create(ctx, newJob)
		if err != nil {
			log.Printf("scheduler: failed to create job: %v", err)
			continue
		}

		s.queue.Enqueue(ctx, newJobID, priority, nextRun)
		s.db.Exec(ctx, `
		UPDATE jobs 
		SET next_run_at = $1, updated_at = NOW()
		WHERE id = $2
		`, nextRun, id)
		log.Printf("scheduler: enqueued cron job %s, next run at %v", newJobID, nextRun)
	}
}
