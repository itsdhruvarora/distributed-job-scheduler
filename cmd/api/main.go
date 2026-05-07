package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/itsdhruvarora/job-scheduler/config"
	"github.com/itsdhruvarora/job-scheduler/internal/db"
	"github.com/itsdhruvarora/job-scheduler/internal/job"
	"github.com/itsdhruvarora/job-scheduler/internal/queue"
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

	store := job.NewStore(pool)
	handler := job.NewHandler(store, q)

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
