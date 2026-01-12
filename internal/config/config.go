package config

import "os"

type Config struct {
	GRPCServerAddr string
	CronSchedule   string
}

func Load() *Config {
	return &Config{
		GRPCServerAddr: getEnv("GRPC_SERVER_ADDR", "localhost:50051"),
		CronSchedule:   getEnv("CRON_SCHEDULE", "@every 15d"),
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
