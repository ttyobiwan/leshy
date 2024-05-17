package messages

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/tobias-piotr/leshy/internal/sqlite"
)

type DBsMap map[Queue]map[Consumer]*sql.DB

type DistributedSQLStorage struct{ dbs DBsMap }

func NewDistributedSQLStorage() *DistributedSQLStorage {
	return &DistributedSQLStorage{make(DBsMap)}
}

func (dss *DistributedSQLStorage) Insert(queue Queue, id string, data []byte) error {
	dbs, err := dss.getQueueDBs(queue)
	if err != nil {
		return fmt.Errorf("getting queue dbs: %w", err)
	}

	for _, db := range dbs {
		_, err = db.Exec("INSERT INTO messages (id, data) VALUES (?, ?);", id, data)
		if err != nil {
			return fmt.Errorf("inserting message: %w", err)
		}
	}

	return nil
}

func (dss *DistributedSQLStorage) GetAll(queue Queue, consumer Consumer) ([]Message, error) {
	db, err := dss.getConsumerDB(queue, consumer)
	if err != nil {
		return nil, err
	}

	rows, err := db.Query("SELECT id, data FROM messages WHERE acked = 0 ORDER BY created_at ASC;")
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

func (dss *DistributedSQLStorage) Ack(queue Queue, consumer Consumer, id string) error {
	db, err := dss.getConsumerDB(queue, consumer)
	if err != nil {
		return err
	}

	_, err = db.Exec("UPDATE messages SET acked = 1 WHERE id = ?;", id)
	if err != nil {
		return fmt.Errorf("updating message: %w", err)
	}

	return nil
}

// getQueueDBs gets connection for each database (consumer) for given queue.
func (dss *DistributedSQLStorage) getQueueDBs(queue Queue) ([]*sql.DB, error) {
	// Get filenames for given queue
	filenames, err := sqlite.GetDBFilenames(string(queue))
	if err != nil {
		return nil, fmt.Errorf("reading filenames: %w", err)
	}

	// If there are no files, it means it is a new queue
	if len(filenames) == 0 {
		filenames = append(filenames, string(queue))
		dss.dbs[queue] = make(map[Consumer]*sql.DB)
	}

	dbs := make([]*sql.DB, len(filenames))

	// Get DB connection for each filename
	for i, f := range filenames {
		dbName := strings.Split(f, ".db")[0]
		db, ok := dss.dbs[queue][Consumer(dbName)]
		if !ok {
			db, err = sqlite.GetDB(string(queue), dbName, false)
			if err != nil {
				return nil, fmt.Errorf("getting db: %w", err)
			}
			dss.dbs[queue][Consumer(dbName)] = db
		}
		dbs[i] = db
	}

	return dbs, nil
}

func (dss *DistributedSQLStorage) getConsumerDB(queue Queue, consumer Consumer) (*sql.DB, error) {
	// Default consumer to queue name (main)
	if consumer == "" {
		consumer = Consumer(queue)
	}

	// Get preused connection
	dbsPerQueue, ok := dss.dbs[queue]
	if ok {
		db, ok := dbsPerQueue[consumer]
		if ok {
			return db, nil
		}
	} else {
		dss.dbs[queue] = make(map[Consumer]*sql.DB)
	}

	// Get consumer connection
	db, err := sqlite.GetDB(string(queue), string(consumer), true)
	if err != nil {
		return nil, fmt.Errorf("getting db: %w", err)
	}
	dss.dbs[queue][consumer] = db

	// If we use main, no need to do anything else
	if consumer == Consumer(queue) {
		return db, nil
	}

	// Get main connection
	mainDB, ok := dss.dbs[queue][Consumer(queue)]
	if !ok {
		mainDB, err = sqlite.GetDB(string(queue), string(queue), false)
		if err != nil {
			return nil, fmt.Errorf("getting main db: %w", err)
		}
		dss.dbs[queue][Consumer(queue)] = mainDB
	}

	// Check if consumer db is empty
	row := db.QueryRow("SELECT 1 FROM messages LIMIT 1")
	var v int
	err = row.Scan(&v)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("scanning row: %w", err)
	}
	if v == 1 {
		return db, nil
	}

	// Copy main to consumer
	err = sqlite.CopyDB(mainDB, string(queue), string(consumer))
	if err != nil {
		return nil, fmt.Errorf("copying main to consumer: %w", err)
	}

	return db, nil
}
