package job

import (
	"time"
)

type Status string

const (
	StatusPending Status = "PENDING"
	StatusRunning Status = "RUNNING"
	StatusDone    Status = "DONE"
	StatusFailed  Status = "FAILED"
	StatusWaiting Status = "WAITING"
)

type Job struct {
	ID          string
	Type        string
	Payload     []byte
	Status      Status
	Priority    int
	MaxRetries  int
	Attempts    int
	DependsOn   []string
	ScheduledAt time.Time
	CreatedAt   time.Time
	UpdatedAt   time.Time
}
