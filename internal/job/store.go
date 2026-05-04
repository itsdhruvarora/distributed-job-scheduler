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
        INSERT INTO jobs (id, type, payload, status, priority, max_retries, depends_on, scheduled_at, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
    `,
		j.ID,
		j.Type,
		j.Payload,
		j.Status,
		j.Priority,
		j.MaxRetries,
		j.DependsOn,
		j.ScheduledAt,
		j.CreatedAt,
		j.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to create job: %w", err)
	}
	return nil
}
