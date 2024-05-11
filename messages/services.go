package messages

import (
	"fmt"

	"github.com/google/uuid"
	pb "github.com/tobias-piotr/leshy/proto"
)

type (
	Queue    string
	Listener chan *pb.MessageStreamResponse
)

type MessageBroadcaster struct {
	storage   *DistributedSQLStorage
	listeners map[Queue][]Listener
}

func NewMessageBroadcaster() *MessageBroadcaster {
	return &MessageBroadcaster{
		storage:   NewDistributedSQLStorage(),
		listeners: make(map[Queue][]Listener),
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
			fmt.Println("Sending to listeners")
			for _, listener := range listeners {
				listener <- &pb.MessageStreamResponse{Id: id, Data: rq.Data}
			}
			fmt.Println("Done sending")
		}()
	}

	return &pb.MessageResponse{Id: id}, nil
}

// ReadMessages creates a new listener channel for given queue, and sends unread messages to it.
func (mb *MessageBroadcaster) ReadMessages(rq *pb.MessageStreamRequest, listener Listener) (Listener, error) {
	msgs, err := mb.storage.GetByQueue(Queue(rq.Queue))
	if err != nil {
		return nil, fmt.Errorf("getting messages: %w", err)
	}

	// listener := make(Listener)
	go func() {
		fmt.Println("Sending to new listener")
		for _, msg := range msgs {
			listener <- &pb.MessageStreamResponse{Id: msg.ID, Data: msg.Data}
		}
		fmt.Println("Done sending")
	}()

	mb.listeners[Queue(rq.Queue)] = append(mb.listeners[Queue(rq.Queue)], listener)

	return listener, nil
}
