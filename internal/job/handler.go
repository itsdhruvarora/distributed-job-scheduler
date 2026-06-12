package job

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

type Handler struct {
	store *Store
	queue Queue
}

type Queue interface {
	Enqueue(ctx context.Context, jobID string, priority int, scheduledAt time.Time) error
}

func NewHandler(store *Store, queue Queue) *Handler {
	return &Handler{store: store, queue: queue}
}

type CreateJobRequest struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	DependsOn      []string        `json:"depends_on"`
	CronExpression *string         `json:"cron_expression"`
}

func (h *Handler) CreateJob(w http.ResponseWriter, r *http.Request) {
	var req CreateJobRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	log.Printf("cron_expression received: %v", req.CronExpression)

	status := StatusPending
	if len(req.DependsOn) > 0 {
		blockers, err := h.store.CountBlockers(r.Context(), req.DependsOn)
		if err != nil || blockers > 0 {
			status = StatusWaiting
		}
	}

	j := Job{
		ID:             uuid.New().String(),
		Type:           req.Type,
		Payload:        []byte(req.Payload),
		Status:         status,
		Priority:       req.Priority,
		MaxRetries:     3,
		DependsOn:      req.DependsOn,
		CronExpression: req.CronExpression,
		ScheduledAt:    time.Now(),
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}

	if req.CronExpression != nil {
		schedule, err := cron.ParseStandard(*req.CronExpression)
		if err != nil {
			http.Error(w, "invalid cron expression", http.StatusBadRequest)
			return
		}
		nextRun := schedule.Next(time.Now())
		j.NextRunAt = &nextRun
	}

	err = h.store.Create(r.Context(), j)
	if err != nil {
		http.Error(w, "failed to create job", http.StatusInternalServerError)
		return
	}

	if status == StatusPending {
		err = h.queue.Enqueue(r.Context(), j.ID, j.Priority, j.ScheduledAt)
		if err != nil {
			http.Error(w, "failed to enqueue job", http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"id": j.ID})
}

func (h *Handler) GetJob(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		http.Error(w, "missing job id", http.StatusBadRequest)
		return
	}

	j, err := h.store.GetByID(r.Context(), id)
	if err != nil {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(j)
}

func (h *Handler) ListJobs(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")

	jobs, err := h.store.List(r.Context(), status)
	if err != nil {
		http.Error(w, "failed to list jobs", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobs)
}
