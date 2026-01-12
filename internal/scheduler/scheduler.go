package internal

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/robfig/cron"
	"github.com/vinaymanala/nationpulse-cron-data-ingestion-schedular-svc/internal/client"
	"github.com/vinaymanala/nationpulse-cron-data-ingestion-schedular-svc/internal/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var jobLock sync.Mutex

type Scheduler struct {
	cron   *cron.Cron
	config *config.Config
}

func New(cfg *config.Config) *Scheduler {
	return &Scheduler{
		cron:   cron.New(),
		config: cfg,
	}
}

func (s *Scheduler) Start() {
	log.Println("Initializing cron jobs...")

	err := s.cron.AddFunc(s.config.CronSchedule, func() {
		if !tryAcquireLock() {
			log.Println("Already job running. Skipping")
			return
		}
		defer releaseLock()

		log.Println("Cron job triggered")
		s.runDataIngestionJob()
	})

	if err != nil {
		log.Fatalf("Failed to add cron job: %v", err)
	}
	s.cron.Start()
	log.Printf("Cron schedular started with schedule: %s", s.config.CronSchedule)
}

func (s *Scheduler) Stop() {
	log.Println("Stopping cron schedular...")
	s.cron.Stop()
}

func tryAcquireLock() bool {
	jobLock.Lock()
	defer jobLock.Unlock()

	// Check if lock file exists
	lockFile := filepath.Join(os.TempDir(), "cron-job.lock")
	if _, err := os.Stat(lockFile); err == nil {
		// Lock file exists - job is already running
		return false
	}

	// Create lock file
	if err := os.WriteFile(lockFile, []byte("locked"), 0644); err != nil {
		log.Printf("Failed to create lock file: %v", err)
		return false
	}

	return true
}

func releaseLock() {
	lockFile := filepath.Join(os.TempDir(), "cron-job.lock")
	os.Remove(lockFile)
}

func (s *Scheduler) runDataIngestionJob() {
	ctx := context.Background()

	conn, err := grpc.NewClient(
		s.config.GRPCServerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Printf("Failed to connect with gRPC server: %v", err)
		return
	}
	defer conn.Close()

	grpcClient := client.NewGRPCClient(conn)

	jobCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	if err := grpcClient.GetNotify(jobCtx); err != nil {
		log.Printf("Job failed: %v", err)
		return
	}
	log.Println("Cron job completed successfully")
}
