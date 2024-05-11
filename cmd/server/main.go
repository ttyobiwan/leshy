package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"

	"github.com/tobias-piotr/leshy/messages"
	pb "github.com/tobias-piotr/leshy/proto"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedMessageServiceServer
	broadcaster *messages.MessageBroadcaster
}

func (s *server) PublishMessage(ctx context.Context, in *pb.MessageRequest) (*pb.MessageResponse, error) {
	return s.broadcaster.PublishMessage(in)
}

func (s *server) ReadMessages(srv pb.MessageService_ReadMessagesServer) error {
	ctx := srv.Context()
	listener := make(messages.Listener)

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		fmt.Println("Waiting for the first message")
		req, err := srv.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		c, err := s.broadcaster.ReadMessages(req, listener)
		if err != nil {
			return err
		}

		listener = c
	}

	for msg := range listener {
		err := srv.Send(msg)
		if err != nil {
			return fmt.Errorf("sending message: %w", err)
		}
	}

	return nil
}

func run() error {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		return fmt.Errorf("listening: %w", err)
	}

	s := grpc.NewServer()
	pb.RegisterMessageServiceServer(s, &server{broadcaster: messages.NewMessageBroadcaster()})

	slog.Info("Starting gRPC server", "addr", lis.Addr())
	if err := s.Serve(lis); err != nil {
		return fmt.Errorf("serving: %w", err)
	}

	return nil
}

func main() {
	if err := run(); err != nil {
		slog.Error("Error starting gRPC server", "err", err)
		os.Exit(1)
	}
}
