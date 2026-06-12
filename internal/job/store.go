package job

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	db *pgxpool.Pool
}

func NewStore(db *pgxpool.Pool) *Store {
	return &Store{db: db}
}

func (s *Store) Create(ctx context.Context, j Job) error {
	_, err := s.db.Exec(ctx, `
        INSERT INTO jobs (id, type, payload, status, priority, max_retries, depends_on, scheduled_at, cron_expression, next_run_at, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
    `,
		j.ID,
		j.Type,
		j.Payload,
		j.Status,
		j.Priority,
		j.MaxRetries,
		j.DependsOn,
		j.ScheduledAt,
		j.CronExpression,
		j.NextRunAt,
		j.CreatedAt,
		j.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to create job: %w", err)
	}
	return nil
}

func (s *Store) GetByID(ctx context.Context, id string) (*Job, error) {
	row := s.db.QueryRow(ctx, `
		SELECT id, type, payload, status, priority, max_retries, attempts, depends_on, scheduled_at, created_at, updated_at
		FROM jobs 
		WHERE id = $1
		`, id)

	var j Job
	err := row.Scan(
		&j.ID,
		&j.Type,
		&j.Payload,
		&j.Status,
		&j.Priority,
		&j.MaxRetries,
		&j.Attempts,
		&j.DependsOn,
		&j.ScheduledAt,
		&j.CreatedAt,
		&j.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	return &j, nil
}

func (s *Store) List(ctx context.Context, status string) ([]Job, error) {
	query := `
		SELECT id, type, payload, status, priority, max_retries, attempts, depends_on, scheduled_at, created_at, updated_at
		FROM jobs
	`
	args := []any{}

	if status != "" {
		query += "WHERE status = $1"
		args = append(args, status)
	}

	query += " ORDER BY priority DESC, scheduled_at ASC"

	rows, err := s.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list jobs: %w", err)
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		var j Job
		err := rows.Scan(
			&j.ID,
			&j.Type,
			&j.Payload,
			&j.Status,
			&j.Priority,
			&j.MaxRetries,
			&j.Attempts,
			&j.DependsOn,
			&j.ScheduledAt,
			&j.CreatedAt,
			&j.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan job: %w", err)
		}
		jobs = append(jobs, j)
	}

	return jobs, nil
}

func (s *Store) UnblockDependents(ctx context.Context, completedJobID string) ([]Job, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id, priority, scheduled_at
		FROM jobs
		WHERE status = 'WAITING'
		AND $1 = ANY(depends_on)
	`, completedJobID)

	if err != nil {
		return nil, fmt.Errorf("failed to find dependents: %w", err)
	}

	defer rows.Close()

	var unblocked []Job
	for rows.Next() {
		var j Job
		err := rows.Scan(&j.ID, &j.Priority, &j.ScheduledAt)
		if err != nil {
			continue
		}

		var blockers int
		err = s.db.QueryRow(ctx, `
			SELECT COUNT(*) FROM jobs
			WHERE id = ANY(
				SELECT unnest(depends_on) FROM jobs WHERE id = $1
			)
			AND status != 'DONE'
		`, j.ID).Scan(&blockers)

		if err != nil {
			continue
		}

		if blockers == 0 {
			s.db.Exec(ctx, `
				UPDATE jobs SET status = 'PENDING', updated_at = NOW()
				WHERE id = $1
			`, j.ID)
			unblocked = append(unblocked, j)
		}
	}

	return unblocked, nil
}
func (s *Store) CountBlockers(ctx context.Context, dependsOn []string) (int, error) {
    if len(dependsOn) == 0 {
        return 0, nil
    }
    var count int
    err := s.db.QueryRow(ctx, `
        SELECT COUNT(*) FROM jobs
        WHERE id = ANY($1)
        AND status != 'DONE'
    `, dependsOn).Scan(&count)
    if err != nil {
        return 0, fmt.Errorf("failed to count blockers: %w", err)
    }
    return count, nil
}
