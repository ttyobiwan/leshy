package messages

// Message is a representation of the message that will be retrieved from the database,
// and sent to the final consumer.
type Message struct {
	ID   string
	Data []byte
}
