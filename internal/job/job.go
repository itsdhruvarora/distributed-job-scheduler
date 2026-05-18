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
    ID             string     `json:"id"`
    Type           string     `json:"type"`
    Payload        []byte     `json:"payload"`
    Status         Status     `json:"status"`
    Priority       int        `json:"priority"`
    MaxRetries     int        `json:"max_retries"`
    Attempts       int        `json:"attempts"`
    DependsOn      []string   `json:"depends_on"`
    CronExpression *string    `json:"cron_expression,omitempty"`
    NextRunAt      *time.Time `json:"next_run_at,omitempty"`
    ScheduledAt    time.Time  `json:"scheduled_at"`
    CreatedAt      time.Time  `json:"created_at"`
    UpdatedAt      time.Time  `json:"updated_at"`
}