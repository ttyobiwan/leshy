package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"time"

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
	var listener *messages.Listener
	defer func() {
		if listener == nil {
			return
		}
		slog.Info("Disconnecting listener", "id", listener.ID, "queue", listener.Queue)
		s.broadcaster.RemoveListener(listener)
	}()

	initialMsg := make(chan struct {
		queue string
		err   error
	}, 1)

	go func() {
		// Recv is blocking but it will raise an error when we make return on initialCtx
		req, err := srv.Recv()
		initialMsg <- struct {
			queue string
			err   error
		}{req.GetQueue(), err}
	}()

	initialCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	select {
	case <-initialCtx.Done():
		return initialCtx.Err()
	case msg := <-initialMsg:
		close(initialMsg)
		queue, err := msg.queue, msg.err
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		listener = messages.NewListener(messages.Queue(queue))
		err = s.broadcaster.ReadMessages(listener)
		if err != nil {
			return err
		}
	}

	// Prepare acks thread
	acks := make(chan struct {
		id  string
		err error
	})
	defer close(acks)

	go func() {
		for {
			msg, err := srv.Recv()
			select {
			case <-acks:
				return
			default:
				acks <- struct {
					id  string
					err error
				}{msg.GetId(), err}
			}
		}
	}()

	// Receive published messages and acks
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-listener.Chan:
			err := srv.Send(msg)
			if err != nil {
				return fmt.Errorf("sending message: %w", err)
			}
		case ack := <-acks:
			id, err := ack.id, ack.err
			if err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}
			err = s.broadcaster.Ack(listener.Queue, id)
			if err != nil {
				return fmt.Errorf("acking message: %w", err)
			}
		}
	}
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
