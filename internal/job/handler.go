package job

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"
)

type Handler struct {
	store *Store
	queue Queue
}

type Queue interface {
	Enqueue(ctx context.Context, jobID string, priority int, scheduledAt time.Time) error
}

func NewHandler(store *Store, queue Queue) *Handler{
	return &Handler{store : store , queue: queue}
}

type CreateJobRequest struct {
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	Priority  int             `json:"priority"`
	DependsOn []string        `json:"depends_on"`	
}

func (h *Handler) CreateJob(w http.ResponseWriter, r *http.Request){
	var req CreateJobRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w,"invalid request body", http.StatusBadRequest)
		return
	}

	j := Job {
		ID: uuid.New().String(),
		Type: req.Type,
		Payload: []byte(req.Payload),
		Status:      StatusPending,
		Priority:    req.Priority,
		MaxRetries:  3,
		DependsOn:   req.DependsOn,
		ScheduledAt: time.Now(),
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	err = h.store.Create(r.Context(), j)
	if err != nil {
		http.Error(w, "failed to create job", http.StatusInternalServerError)
		return
	}

	err = h.queue.Enqueue(r.Context(), j.ID, j.Priority, j.ScheduledAt)
	if err != nil {
		http.Error(w, "failed to enqueue job", http.StatusInternalServerError)
		return
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