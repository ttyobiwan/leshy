package messages

import (
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	pb "github.com/tobias-piotr/leshy/proto"
)

type Queue string

// Listener is a representation of consumer for specific queue.
type Listener struct {
	ID    string
	Queue Queue
	Chan  chan *pb.MessageStreamResponse
}

func NewListener(queue Queue) *Listener {
	return &Listener{
		uuid.New().String(),
		queue,
		make(chan *pb.MessageStreamResponse),
	}
}

// MessageBroadcaster is managing messages persistance and delivery to current listeners.
type MessageBroadcaster struct {
	storage   *DistributedSQLStorage
	listeners map[Queue][]*Listener
}

func NewMessageBroadcaster() *MessageBroadcaster {
	return &MessageBroadcaster{
		storage:   NewDistributedSQLStorage(),
		listeners: make(map[Queue][]*Listener),
	}
}

// PublishMessage saves the message in a proper database and sends it to all listener channels.
func (mb *MessageBroadcaster) PublishMessage(rq *pb.MessageRequest) (*pb.MessageResponse, error) {
	id := uuid.New().String()
	queue := Queue(rq.Queue)

	err := mb.storage.Save(queue, id, rq.Data)
	if err != nil {
		return nil, fmt.Errorf("saving message: %w", err)
	}

	listeners, ok := mb.listeners[queue]
	if ok {
		go func() {
			slog.Info("Publishing message to listeners", "id", id, "listeners", len(listeners))
			for _, listener := range listeners {
				listener.Chan <- &pb.MessageStreamResponse{Id: id, Data: rq.Data}
			}
		}()
	}

	return &pb.MessageResponse{Id: id}, nil
}

// ReadMessages creates a new listener channel for given queue, and sends unread messages to it.
func (mb *MessageBroadcaster) ReadMessages(listener *Listener) error {
	slog.Info("Connecting new listener", "id", listener.ID, "queue", listener.Queue)

	msgs, err := mb.storage.GetByQueue(listener.Queue)
	if err != nil {
		return fmt.Errorf("getting messages: %w", err)
	}

	go func() {
		slog.Info("Sending messages to new listener", "messages", len(msgs))
		for _, msg := range msgs {
			listener.Chan <- &pb.MessageStreamResponse{Id: msg.ID, Data: msg.Data}
		}
	}()

	mb.listeners[listener.Queue] = append(mb.listeners[listener.Queue], listener)

	return nil
}

// Ack updates the ack status in the database.
func (mb *MessageBroadcaster) Ack(queue Queue, id string) error {
	return mb.storage.Ack(queue, id)
}

// RemoveListener removes the listener channel from the list for given queue.
func (mb *MessageBroadcaster) RemoveListener(listener *Listener) {
	listeners, ok := mb.listeners[listener.Queue]
	if !ok {
		return
	}
	if len(listeners) == 1 {
		delete(mb.listeners, listener.Queue)
		return
	}

	for i, l := range listeners {
		if l.ID == listener.ID {
			mb.listeners[listener.Queue] = append(listeners[:i], listeners[i+1:]...)
		}
	}
}
