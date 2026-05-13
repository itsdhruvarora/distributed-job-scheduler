package monitor

import (
	"context"
	"log"
	"time"

	"github.com/itsdhruvarora/job-scheduler/internal/queue"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Monitor struct {
	db    *pgxpool.Pool
	queue *queue.Queue
}

func NewMonitor(db *pgxpool.Pool, q *queue.Queue) *Monitor {
	return &Monitor{db: db, queue: q}
}

func (m *Monitor) Start(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Monitor stopped")
			return
		case <-ticker.C:
			m.reclaim(ctx)
		}
	}
}

func (m *Monitor) reclaim(ctx context.Context) {
	activeWorkers, err := m.queue.GetActiveWorkers(ctx)
	if err != nil {
		log.Printf("monitor: failed to get active workers: %v", err)
		return
	}

	rows, err := m.db.Query(ctx, `
		SELECT id, priority, scheduled_at 
		FROM jobs 
		WHERE status = 'RUNNING'
		AND updated_at < NOW() - INTERVAL '35 seconds'
	`)
	if err != nil {
		log.Printf("monitor: failed to query running jobs: %v", err)
		return
	}
	defer rows.Close()

	activeSet := make(map[string]bool)
	for _, w := range activeWorkers {
		activeSet[w] = true
	}

	for rows.Next() {
		var id string
		var priority int
		var scheduledAt time.Time

		rows.Scan(&id, &priority, &scheduledAt)

		m.db.Exec(ctx, `
			UPDATE jobs SET status = 'PENDING', updated_at = NOW()
			WHERE id = $1
		`, id)

		m.queue.Enqueue(ctx, id, priority, scheduledAt)
		log.Printf("monitor: reclaimed orphaned job %s", id)
	}
}
