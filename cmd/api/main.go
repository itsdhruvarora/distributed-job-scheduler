package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/itsdhruvarora/job-scheduler/config"
	"github.com/itsdhruvarora/job-scheduler/internal/db"
	"github.com/itsdhruvarora/job-scheduler/internal/job"
)

func main() {
	cfg := config.Load()

	pool, err := db.NewPool(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	store := job.NewStore(pool)

	testJob := job.Job{
		ID:          "test-001",
		Type:        "send-email",
		Payload:     []byte(`{"to": "test@gmail.com", "subject": "Hello"}`),
		Status:      job.StatusPending,
		Priority:    5,
		MaxRetries:  3,
		DependsOn:   []string{},
		ScheduledAt: time.Now(),
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	fmt.Println("About to create job...")
	err = store.Create(context.Background(), testJob)
	if err != nil {
		log.Fatalf("failed to create job: %v", err)
	}
	fmt.Println("Job created successfully")
	err = store.Create(context.Background(), testJob)
	if err != nil {
		log.Fatalf("failed to create job: %v", err)
	}

	fmt.Println("Job created successfully")

	fmt.Println("Connected to database successfully")

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "ok")
	})

	err = http.ListenAndServe(":"+cfg.Port, nil)
	if err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
