package sqlite

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

var (
	dbDir       = "data"
	dbMigration = `
CREATE TABLE IF NOT EXISTS messages (
	id UUID PRIMARY KEY,
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
`
)

func GetDB(name string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", fmt.Sprintf("%s/%s.db", dbDir, name))
	if err != nil {
		return nil, fmt.Errorf("opening db: %w", err)
	}

	_, err = db.Exec(dbMigration)
	if err != nil {
		return nil, fmt.Errorf("migrating db: %w", err)
	}

	return db, nil
}
