package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/itsdhruvarora/job-scheduler/config"
	"github.com/itsdhruvarora/job-scheduler/internal/db"
	"github.com/itsdhruvarora/job-scheduler/internal/queue"
	"github.com/itsdhruvarora/job-scheduler/internal/worker"
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

	fmt.Println("Worker starting...")

	w := worker.NewWorker(pool, q)
	
	ctx := context.Background()
	for {
		err := w.ProcessNext(ctx)
		if err != nil {
			log.Printf("worker error: %v", err)
		}
		time.Sleep(1 * time.Second)
	}
}