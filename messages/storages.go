package messages

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/tobias-piotr/leshy/internal/sqlite"
)

var defaultTTL = 1 * time.Minute

// Connection represents a database connection, with a life time limit.
type Connection struct {
	DB  *sql.DB
	TTL time.Time
}

// IncreaseTTL extends the TTL by a predefined amount of time.
func (c *Connection) IncreaseTTL() {
	c.TTL = time.Now().Add(defaultTTL)
}

// ConnectionMap is thread-safe map, that manages Connection objects, with their ttls.
type ConnectionMap struct {
	connMap map[Queue]map[Consumer]*Connection
	mu      sync.RWMutex
}

func NewConnectionMap() *ConnectionMap {
	return &ConnectionMap{connMap: make(map[Queue]map[Consumer]*Connection)}
}

func (m *ConnectionMap) Set(queue Queue, consumer Consumer, conn *Connection) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.connMap[queue]
	if !ok {
		m.connMap[queue] = make(map[Consumer]*Connection)
	}
	m.connMap[queue][consumer] = conn
}

func (m *ConnectionMap) SetMany(queue Queue, conns map[Consumer]*Connection) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.connMap[queue]
	if !ok {
		m.connMap[queue] = make(map[Consumer]*Connection)
	}
	for consumer, conn := range conns {
		m.connMap[queue][consumer] = conn
	}
}

func (m *ConnectionMap) Get(queue Queue, consumer Consumer) *Connection {
	m.mu.RLock()
	defer m.mu.RUnlock()

	queueMap, ok := m.connMap[queue]
	if !ok {
		return nil
	}
	conn := queueMap[consumer]
	if conn == nil {
		return nil
	}
	// Increase ttl
	// Technically speaking we are bypassing the write lock,
	// but this race condition is not dangerous (for now)
	conn.IncreaseTTL()
	return conn
}

func (m *ConnectionMap) Clean() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	removedCount := 0

	for queue, consMap := range m.connMap {
		for consumer, conn := range consMap {
			if conn.TTL.After(now) {
				continue
			}
			removedCount++
			// If there is only one connection, delete the map for the queue
			if len(consMap) == 1 {
				delete(m.connMap, queue)
				continue
			}
			// Remove the connection for given consumer
			delete(consMap, consumer)
		}
	}

	return removedCount
}

type DistributedSQLStorage struct{ connMap *ConnectionMap }

func NewDistributedSQLStorage(connMap *ConnectionMap) *DistributedSQLStorage {
	return &DistributedSQLStorage{connMap}
}

// Insert saves the message in every database for given queue.
func (dss *DistributedSQLStorage) Insert(queue Queue, id string, data []byte) error {
	conns, err := dss.getQueueConns(queue)
	if err != nil {
		return fmt.Errorf("getting queue dbs: %w", err)
	}

	for _, conn := range conns {
		_, err = conn.DB.Exec("INSERT INTO messages (id, data) VALUES (?, ?);", id, data)
		if err != nil {
			return fmt.Errorf("inserting message: %w", err)
		}
	}

	return nil
}

// GetAll retrieves all the messages for given queue + consumer combination.
func (dss *DistributedSQLStorage) GetAll(queue Queue, consumer Consumer) ([]Message, error) {
	conn, err := dss.getConsumerConn(queue, consumer)
	if err != nil {
		return nil, err
	}

	rows, err := conn.DB.Query("SELECT id, data FROM messages WHERE acked = 0 ORDER BY created_at ASC;")
	if err != nil {
		return nil, fmt.Errorf("querying messages: %w", err)
	}
	defer rows.Close()

	msgs := []Message{}
	for rows.Next() {
		var msg Message
		err = rows.Scan(&msg.ID, &msg.Data)
		if err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}
		msgs = append(msgs, msg)
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("reading rows: %w", err)
	}

	return msgs, nil
}

// Ack updates the acked status for message with given id, in database for specific queue + consumer combination.
func (dss *DistributedSQLStorage) Ack(queue Queue, consumer Consumer, id string) error {
	conn, err := dss.getConsumerConn(queue, consumer)
	if err != nil {
		return err
	}

	_, err = conn.DB.Exec("UPDATE messages SET acked = 1 WHERE id = ?;", id)
	if err != nil {
		return fmt.Errorf("updating message: %w", err)
	}

	return nil
}

// getQueueDBs gets connection for each database (consumer) for given queue.
func (dss *DistributedSQLStorage) getQueueConns(queue Queue) (map[Consumer]*Connection, error) {
	// Get filenames for given queue
	filenames, err := sqlite.GetDBFilenames(string(queue))
	if err != nil {
		return nil, fmt.Errorf("reading filenames: %w", err)
	}

	// If there are no files, it means it is a new queue
	if len(filenames) == 0 {
		filenames = append(filenames, string(queue))
	}

	// Get connection for each file
	// If connection is present in the map, use it
	conns := make(map[Consumer]*Connection, len(filenames))
	for _, f := range filenames {
		dbName := strings.Split(f, ".db")[0]
		// TODO: Maybe would also make sense to GetMany
		conn := dss.connMap.Get(queue, Consumer(dbName))
		if conn == nil {
			db, err := sqlite.GetDB(string(queue), dbName, false)
			if err != nil {
				return nil, fmt.Errorf("getting db: %w", err)
			}
			ttl := time.Now().Add(defaultTTL)
			conn = &Connection{db, ttl}
		}
		conns[Consumer(dbName)] = conn
	}
	// TODO: Potential unnecessary assignments, because some connections might already be there (retrieved by Get)
	dss.connMap.SetMany(queue, conns)

	return conns, nil
}

func (dss *DistributedSQLStorage) getConsumerConn(queue Queue, consumer Consumer) (*Connection, error) {
	// Default consumer to queue name (main)
	if consumer == "" {
		consumer = Consumer(queue)
	}

	// Get preused connection
	conn := dss.connMap.Get(queue, consumer)
	if conn != nil {
		return conn, nil
	}

	// Get a new consumer connection
	db, err := sqlite.GetDB(string(queue), string(consumer), true)
	if err != nil {
		return nil, fmt.Errorf("getting db: %w", err)
	}
	ttl := time.Now().Add(defaultTTL)
	conn = &Connection{db, ttl}
	dss.connMap.Set(queue, consumer, conn)

	// If this is a main connection, then no need to do anything else
	if consumer == Consumer(queue) {
		return conn, nil
	}

	// Get main connection, and save it in the map
	mainConn := dss.connMap.Get(queue, Consumer(queue))
	if mainConn == nil {
		db, err := sqlite.GetDB(string(queue), string(queue), false)
		if err != nil {
			return nil, fmt.Errorf("getting main db: %w", err)
		}
		mainConn = &Connection{db, ttl}
		dss.connMap.Set(queue, Consumer(queue), mainConn)
	}

	// Check if consumer db is empty
	row := db.QueryRow("SELECT 1 FROM messages LIMIT 1")
	var v int
	err = row.Scan(&v)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("scanning row: %w", err)
	}
	// If not, then consumer db is good to go
	if v == 1 {
		return conn, nil
	}

	// If consumer is empty, then it's (most likely) new, so we copy data from main
	err = sqlite.CopyDB(mainConn.DB, string(queue), string(consumer))
	if err != nil {
		return nil, fmt.Errorf("copying main to consumer: %w", err)
	}

	return conn, nil
}
