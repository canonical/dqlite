package connection

import (
	"github.com/CanonicalLtd/go-sqlite3"
	"github.com/pkg/errors"
)

// OpenLeader is a wrapper around SQLiteDriver.Open that opens connection in
// leader replication mode, and sets any additional dqlite-related options.
//
// The 'methods' argument is used to set the replication methods and the n one is
// the WAL frame size threshold after which auto-checkpoint will trigger.
func OpenLeader(dsn string, methods sqlite3.ReplicationMethods, n int) (*sqlite3.SQLiteConn, error) {
	conn, err := open(dsn)
	if err != nil {
		return nil, err
	}

	// Ensure WAL autocheckpoint is set, so the WAL and the raft log store
	// don't not grow indefitely.
	if n > 0 {
		sqlite3.ReplicationAutoCheckpoint(conn, n)
	}

	// Swith to leader replication mode for this connection.
	if err := sqlite3.ReplicationLeader(conn, methods); err != nil {
		return nil, err
	}

	return conn, nil

}

// CloseLeader closes the given leader connection and releases the associated
// methods C hooks memory allocated by go-sqlite3.
//
// FIXME: Perhaps this should be done in sqlite3 in a more explicit or nicer way.
func CloseLeader(conn *sqlite3.SQLiteConn) error {
	if _, err := sqlite3.ReplicationNone(conn); err != nil {
		return errors.Wrap(err, "failed to set replication mode back to none")
	}
	if err := conn.Close(); err != nil {
		return errors.Wrap(err, "failed to close leader connection")
	}
	return nil
}

// OpenFollower is a wrapper around SQLiteDriver.Open that opens connection in
// follower replication mode, and sets any additional dqlite-related options.
func OpenFollower(dsn string) (*sqlite3.SQLiteConn, error) {
	conn, err := open(dsn)
	if err != nil {
		return nil, err
	}

	// Ensure WAL autocheckpoint for followers is disabled, since
	// checkpoints are triggered by leader connections via Raft commands.
	if err := sqlite3.WalAutoCheckpointPragma(conn, 0); err != nil {
		return nil, err
	}

	// Switch to leader replication mode for this connection.
	if err := sqlite3.ReplicationFollower(conn); err != nil {
		return nil, err
	}

	return conn, nil
}

// Open a SQLite connection, setting anything that is common between leader and
// follower connections.
func open(dsn string) (*sqlite3.SQLiteConn, error) {
	// Open a plain connection.
	driver := &sqlite3.SQLiteDriver{}
	conn, err := driver.Open(dsn)
	if err != nil {
		return nil, errors.Wrapf(err, "open error for %s", dsn)
	}

	// Convert driver.Conn interface to concrete sqlite3.SQLiteConn.
	sqliteConn := conn.(*sqlite3.SQLiteConn)

	// Ensure journal mode is set to WAL, as this is a requirement for
	// replication.
	if err := sqlite3.JournalModePragma(sqliteConn, sqlite3.JournalWal); err != nil {
		return nil, err
	}

	// Ensure we don't truncate or checkpoint the WAL on exit, as this
	// would bork replication which must be in full control of the WAL
	// file.
	if err := sqlite3.JournalSizeLimitPragma(sqliteConn, -1); err != nil {
		return nil, err
	}
	if err := sqlite3.DatabaseNoCheckpointOnClose(sqliteConn); err != nil {
		return nil, err
	}

	return sqliteConn, nil
}
