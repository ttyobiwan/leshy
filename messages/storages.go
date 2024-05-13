package messages

import (
	"database/sql"
	"fmt"

	"github.com/tobias-piotr/leshy/internal/sqlite"
)

type PerQueueDBs map[Queue]*sql.DB

type DistributedSQLStorage struct{ dbs PerQueueDBs }

func NewDistributedSQLStorage() *DistributedSQLStorage {
	return &DistributedSQLStorage{make(PerQueueDBs)}
}

func (dss *DistributedSQLStorage) Save(queue Queue, id string, data []byte) error {
	db, err := dss.getOrAddDB(queue)
	if err != nil {
		return err
	}

	_, err = db.Exec("INSERT INTO messages (id, data) VALUES (?, ?);", id, data)
	if err != nil {
		return fmt.Errorf("inserting message: %w", err)
	}

	return nil
}

func (dss *DistributedSQLStorage) GetByQueue(queue Queue) ([]Message, error) {
	db, err := dss.getOrAddDB(queue)
	if err != nil {
		return nil, err
	}

	rows, err := db.Query("SELECT id, data FROM messages ORDER BY created_at ASC;")
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

func (dss *DistributedSQLStorage) getOrAddDB(queue Queue) (*sql.DB, error) {
	db, ok := dss.dbs[queue]
	if ok {
		return db, nil
	}

	db, err := sqlite.GetDB(string(queue))
	if err != nil {
		return nil, fmt.Errorf("getting db: %w", err)
	}

	return db, nil
}
