package sqlite

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

var (
	dbDir       = "data"
	dbMigration = `
CREATE TABLE IF NOT EXISTS messages (
	id UUID PRIMARY KEY,
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	data BLOB,
	acked BOOLEAN NOT NULL CHECK (acked IN (0, 1)) DEFAULT 0
);
`
)

// GetDBFilenames gets names of all the database files in dbDir/path/.
// If it doesn't exist, GetDBFilenames creates all the directories.
func GetDBFilenames(path string) ([]string, error) {
	fullpath := filepath.Join(dbDir, path)
	err := os.MkdirAll(fullpath, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("making dir: %w", err)
	}

	files, err := os.ReadDir(fullpath)
	if err != nil {
		return nil, fmt.Errorf("reading dir: %w", err)
	}

	filenames := make([]string, len(files))
	for i, f := range files {
		filenames[i] = f.Name()
	}
	return filenames, nil
}

// GetDB connects to the SQLite database inside dbDir/path/name.db and executes the initial migration.
// When passing mkdir as true, GetDB will make sure that the directory exists.
func GetDB(path, name string, mkdir bool) (*sql.DB, error) {
	fullpath := filepath.Join(dbDir, path)
	if mkdir {
		err := os.MkdirAll(fullpath, os.ModePerm)
		if err != nil {
			return nil, fmt.Errorf("making dir: %w", err)
		}
	}

	db, err := sql.Open("sqlite3", fmt.Sprintf("%s/%s.db", fullpath, name))
	if err != nil {
		return nil, fmt.Errorf("opening db: %w", err)
	}

	_, err = db.Exec(dbMigration)
	if err != nil {
		return nil, fmt.Errorf("migrating db: %w", err)
	}

	return db, nil
}

// CopyDB copies entire messages table into the target dbDir/path/name.db database.
func CopyDB(db *sql.DB, path, name string) error {
	_, err := db.Exec(`
ATTACH DATABASE ? AS consumer_db;
INSERT INTO consumer_db.messages (id, created_at, data, acked)
SELECT id, created_at, data, 0 FROM messages;
DETACH DATABASE consumer_db;`,
		fmt.Sprintf("%s/%s/%s.db", dbDir, path, name),
	)
	return err
}
