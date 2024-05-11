package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	pb "github.com/tobias-piotr/leshy/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func run() error {
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("creating grpc client: %w", err)
	}
	defer conn.Close()

	client := pb.NewMessageServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream, err := client.ReadMessages(ctx)
	if err != nil {
		return fmt.Errorf("opening grpc stream: %w", err)
	}
	time.Sleep(5 * time.Second)

	stream.Send(&pb.MessageStreamRequest{Queue: "tasks"})

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("receiving message: %w", err)
		}
		slog.Info("Received message", "msg", msg)
	}
}

func main() {
	if err := run(); err != nil {
		slog.Error("Error starting gRPC listener", "err", err)
		os.Exit(1)
	}
}
