package main

import (
	"context"
	"encoding/json"
	"fmt"
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

	data := map[string]any{"a": 1, "b": "asd"}
	b, _ := json.Marshal(data)
	msg := pb.MessageRequest{Queue: "tasks", Data: b}

	resp, err := client.PublishMessage(ctx, &msg)
	if err != nil {
		return fmt.Errorf("publishing message: %w", err)
	}

	slog.Info("Got response from 'PublishMessage'", "response", resp)

	return nil
}

func main() {
	if err := run(); err != nil {
		slog.Error("Error publishing gRPC message", "err", err)
		os.Exit(1)
	}
}
