package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/itsdhruvarora/job-scheduler/config"
	"github.com/itsdhruvarora/job-scheduler/internal/db"
	"github.com/itsdhruvarora/job-scheduler/internal/job"
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	workerID := uuid.New().String()
	store := job.NewStore(pool)
	w := worker.NewWorker(pool, q, store, workerID)

	go func() {
		<-quit
		fmt.Println("\nShutdown signal received, finishing current job...")
		cancel()
	}()

	fmt.Printf("Worker ID: %s\n", workerID)

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				q.SetHeartbeat(context.Background(), workerID)
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Worker stopped gracefully")
			return
		default:
			err := w.ProcessNext(ctx)
			if err != nil {
				log.Printf("worker error: %v", err)
			}
			time.Sleep(1 * time.Second)
		}
	}
}
