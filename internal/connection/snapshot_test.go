package connection_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshot(t *testing.T) {
	dir, cleanup := newDir()
	defer cleanup()

	path := filepath.Join(dir, "test.db")

	// Create a database with some content.
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE foo (n INT); INSERT INTO foo VALUES(1)")
	require.NoError(t, err)
	db.Close()

	// Perform and restore the snapshot.
	database, wal, err := connection.Snapshot(path)
	require.NoError(t, err)
	require.NoError(t, connection.Restore(path, database, wal))

	// Check that the data actually matches our source database.
	db, err = sql.Open("sqlite3", path)
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT * FROM foo", nil)
	require.NoError(t, err)
	defer rows.Close()

	require.Equal(t, true, rows.Next())
	var n int
	assert.NoError(t, rows.Scan(&n))
	assert.Equal(t, 1, n)
}

func TestSnapshot_InvalidDir(t *testing.T) {
	// Perform and restore the snapshot.
	_, _, err := connection.Snapshot("/non/existing/path")
	msg := "source connection: open error for /non/existing/path: unable to open database file"
	assert.EqualError(t, err, msg)
}
