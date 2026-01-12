package client

import (
	"context"
	"io"
	"log"
	"time"

	"github.com/vinaymanala/nationpulse-cron-data-ingestion-schedular-svc/pb"
	"google.golang.org/grpc"
)

type GRPCClient struct {
	client pb.DataIngestionClient
}

func NewGRPCClient(conn *grpc.ClientConn) *GRPCClient {
	return &GRPCClient{
		client: pb.NewDataIngestionClient(conn),
	}
}

func (g *GRPCClient) GetNotify(ctx context.Context) error {
	log.Println("Starting GetNotify..")

	streamCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	stream, err := g.client.NotifyBFF(streamCtx, &pb.NotifyBFFRequest{})
	if err != nil {
		log.Printf("Failed to start stream: %v", err)
		return err
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			log.Println("Stream ended")
			break
		}
		if err != nil {
			log.Printf("Stream error: %v", err)
			return err
		}
		log.Printf("Received: %v", resp)
	}
	log.Println("Completed GetNotify")
	return nil

}
