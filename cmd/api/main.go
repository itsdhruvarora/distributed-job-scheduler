package main

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/itsdhruvarora/job-scheduler/config"
	"github.com/itsdhruvarora/job-scheduler/internal/db"
	"github.com/itsdhruvarora/job-scheduler/internal/job"
	"github.com/itsdhruvarora/job-scheduler/internal/monitor"
	"github.com/itsdhruvarora/job-scheduler/internal/queue"
	"github.com/itsdhruvarora/job-scheduler/internal/scheduler"
)

func main() {
	cfg := config.Load()

	pool, err := db.NewPool(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	q, err := queue.NewQueue(cfg.RedisURL)
	if err != nil {
		log.Fatalf("failed to connect to redis: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := job.NewStore(pool)
	handler := job.NewHandler(store, q)
	m := monitor.NewMonitor(pool, q)
	s := scheduler.NewScheduler(pool, q, store)
	go s.Start(ctx)
	go m.Start(ctx)
	fmt.Println("Connected to database successfully")
	fmt.Println("Connected to Redis successfully")

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "ok")
	})
	http.HandleFunc("/jobs", handler.CreateJob)
	http.HandleFunc("/job", handler.GetJob)
	http.HandleFunc("/jobs/list", handler.ListJobs)

	err = http.ListenAndServe(":"+cfg.Port, nil)
	if err != nil {
		log.Fatalf("server failed: %v", err)
	}

}
