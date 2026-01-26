package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
	"github.com/vinaymanala/nationpulse-cron-data-ingestion-schedular-svc/internal/config"
	s "github.com/vinaymanala/nationpulse-cron-data-ingestion-schedular-svc/internal/scheduler"
)

func main() {
	log.Println("Starting cron schedular service..")

	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found")
	}

	cfg := config.Load()

	sch := s.New(cfg)
	sch.Start()
	defer sch.Stop()
	// graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	//cleanup and shut down services
	log.Println("Schedular stopped")
}
