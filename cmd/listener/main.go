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

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	stream, err := client.ReadMessages(ctx)
	if err != nil {
		return fmt.Errorf("opening grpc stream: %w", err)
	}

	err = stream.Send(&pb.MessageStreamRequest{Queue: "tasks"})
	if err != nil {
		return fmt.Errorf("sending initial message: %w", err)
	}

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("receiving message: %w", err)
		}
		slog.Info("Received message", "msg", msg)

		go func() {
			slog.Info("Preparing to ack", "id", msg.Id)

			err := stream.Send(&pb.MessageStreamRequest{Id: msg.Id})
			if err != nil {
				slog.Error("Error acking", "id", msg.Id, "err", err)
			}

			slog.Info("Done acking", "id", msg.Id)
		}()
	}
}

func main() {
	if err := run(); err != nil {
		slog.Error("Error starting gRPC listener", "err", err)
		os.Exit(1)
	}
}
