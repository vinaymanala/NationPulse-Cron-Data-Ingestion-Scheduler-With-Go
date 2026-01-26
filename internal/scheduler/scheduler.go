package scheduler

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/vinaymanala/nationpulse-cron-data-ingestion-schedular-svc/internal/client"
	"github.com/vinaymanala/nationpulse-cron-data-ingestion-schedular-svc/internal/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// var jobLock sync.Mutex

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

	entryID, err := s.cron.AddFunc(s.config.CronSchedule, func() {
		log.Println("Cron job triggered")
		s.runDataIngestionJob()
	})

	if err != nil {
		log.Fatalf("Failed to add cron job: %v", err)
	}

	s.cron.Start()
	log.Printf("Cron schedular started with id: %d, schedule: %s", entryID, s.config.CronSchedule)
}

func (s *Scheduler) Stop() {
	log.Println("Stopping cron schedular...")
	s.cron.Stop()
}

func (s *Scheduler) runDataIngestionJob() {
	ctx := context.Background()
	fmt.Println("Connecting GRPC client")
	conn, err := grpc.DialContext(ctx, s.config.GRPCServerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Printf("Failed to connect with gRPC server: %v", err)
		return
	}
	defer conn.Close()

	grpcClient := client.NewGRPCClient(conn)

	jobCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	if err := grpcClient.GetNotify(jobCtx); err != nil {
		log.Printf("Job failed: %v", err)
		return
	}
	log.Println("Cron job completed successfully")
}
