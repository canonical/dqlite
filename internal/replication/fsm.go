package replication

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"path/filepath"

	"github.com/CanonicalLtd/dqlite/internal/commands"
	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/CanonicalLtd/dqlite/internal/logging"
	"github.com/CanonicalLtd/dqlite/internal/transaction"
	"github.com/CanonicalLtd/go-sqlite3x"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

// NewFSMLegacy creates a new Raft state machine for executing dqlite-specific
// commands.
func NewFSMLegacy(logger *log.Logger, conns *connection.Registry, txns *transaction.Registry) *FSM {
	return &FSM{
		logger:       logging.New(logger, logging.Trace, ""),
		connections:  conns,
		transactions: txns,
		indexes:      make(chan uint64, 10), // Should be enough for tests
	}
}

// NewFSM creates a new Raft state machine for executing dqlite-specific
// commands.
func NewFSM(dir string, l *logging.Logger, c *connection.Registry, t *transaction.Registry) *FSM {
	return &FSM{
		dir:          dir,
		logger:       l.Augment("fsm: "),
		connections:  c,
		transactions: t,
	}
}

// FSM implements the raft finite-state machine used to replicate
// SQLite data.
type FSM struct {
	dir          string
	logger       *logging.Logger
	connections  *connection.Registry  // Open connections (either leaders or followers).
	transactions *transaction.Registry // Ongoing write transactions.

	indexes chan uint64 // Buffered stream of log indexes that have been applied.
}

// Apply log is invoked once a log entry is committed.
// It returns a value which will be made available in the
// ApplyFuture returned by Raft.Apply method if that
// method was called on the same Raft node as the FSM.
func (f *FSM) Apply(log *raft.Log) interface{} {
	logger := f.logger.Augment(fmt.Sprintf("log %d: ", log.Index))
	logger.Tracef("start")

	cmd, err := commands.Unmarshal(log.Data)
	if err != nil {
		logger.Panicf("corrupted command: %v", err)
	}

	logger = logger.Augment(fmt.Sprintf("%s: ", cmd.Name()))
	logger.Tracef("start")
	defer func() {
		if result := recover(); result != nil {
			logger.Infof("\n%s\n%s", f.connections.Dump(), f.transactions.Dump())
			panic(result)
		}
	}()

	switch params := cmd.Params.(type) {
	case *commands.Command_Open:
		f.applyOpen(logger, params.Open)
	case *commands.Command_Begin:
		f.applyBegin(logger, params.Begin)
	case *commands.Command_WalFrames:
		f.applyWalFrames(logger, params.WalFrames)
	case *commands.Command_Undo:
		f.applyUndo(logger, params.Undo)
	case *commands.Command_End:
		f.applyEnd(logger, params.End)
	case *commands.Command_Checkpoint:
		f.applyCheckpoint(logger, params.Checkpoint)
	default:
		logger.Panicf("unhandled params type")
	}

	logger.Tracef("done")

	// Non-blocking notification of the last applied index. Used
	// by tests for waiting for followers to actually finish
	// committing logs.
	select {
	case f.indexes <- log.Index:
	default:
	}

	return nil
}

// Wait blocks until the command with the given index has been applied.
func (f *FSM) Wait(index uint64) {
	for {
		if <-f.indexes >= index {
			return
		}
	}
}

func (f *FSM) applyOpen(logger *logging.Logger, params *commands.Open) {
	logger.Tracef("add follower for %s", params.Name)
	uri := filepath.Join(f.dir, params.Name)
	conn, err := connection.OpenFollower(uri)
	if err != nil {
		logger.Panicf("failed to open follower connection: %v", err)
	}
	f.connections.AddFollower(params.Name, conn)
}

func (f *FSM) applyBegin(logger *logging.Logger, params *commands.Begin) {
	logger = logger.Augment(fmt.Sprintf("txn %s: ", params.Txid))

	txn := f.transactions.GetByID(params.Txid)
	if txn != nil {
		logger.Tracef("found %s, use it", txn)
		// We know about this txid, so the transaction must
		// have originated on this node and be a leader
		// transaction.
		if !txn.IsLeader() {
			logger.Panicf("is %s instead of leader", txn)
		}
	} else {
		logger.Tracef("not found, add follower")

		// This is must be a new follower transaction. Let's check
		// check that no leader transaction against this database is
		// happening on this node (since we're supposed purely
		// followers, and unreleased write locks would get on our the
		// way).
		for _, conn := range f.connections.Leaders(params.Name) {
			if txn := f.transactions.GetByConn(conn); txn != nil {
				logger.Panicf("found dangling transaction %s", txn)
			}
		}

		conn := f.connections.Follower(params.Name)
		txn = f.transactions.AddFollower(conn, params.Txid)
	}

	if err := txn.Do(txn.Begin); err != nil {
		logger.Panicf("failed to begin transaction %s: %s", txn, err)
	}
}

func (f *FSM) applyWalFrames(logger *logging.Logger, params *commands.WalFrames) {
	logger = logger.Augment(fmt.Sprintf("txn %s: ", params.Txid))

	txn := f.transactions.GetByID(params.Txid)
	if txn == nil {
		logger.Panicf("not found")
	}

	logger.Tracef(txn.String())
	logger.Tracef("%d frames, commit=%v", len(params.Pages), params.IsCommit)

	pages := sqlite3x.NewReplicationPages(len(params.Pages), int(params.PageSize))
	defer sqlite3x.DestroyReplicationPages(pages)

	for i, page := range params.Pages {
		pages[i].Fill(page.Data, uint16(page.Flags), page.Number)
	}

	framesParams := &sqlite3x.ReplicationWalFramesParams{
		PageSize:  int(params.PageSize),
		Pages:     pages,
		Truncate:  uint32(params.Truncate),
		IsCommit:  int(params.IsCommit),
		SyncFlags: uint8(params.SyncFlags),
	}

	if err := txn.Do(func() error { return txn.WalFrames(framesParams) }); err != nil {
		logger.Panicf("failed to write farames: %s", err)
	}
}

func (f *FSM) applyUndo(logger *logging.Logger, params *commands.Undo) {
	logger = logger.Augment(fmt.Sprintf("txn %s: ", params.Txid))
	txn := f.transactions.GetByID(params.Txid)
	if txn == nil {
		logger.Panicf("not found")
	}
	logger.Tracef(txn.String())

	if err := txn.Do(txn.Undo); err != nil {
		logger.Panicf("failed to undo transaction %s: %s", txn, err)
	}
}

func (f *FSM) applyEnd(logger *logging.Logger, params *commands.End) {
	logger = logger.Augment(fmt.Sprintf("txn %s: ", params.Txid))

	txn := f.transactions.GetByID(params.Txid)
	if txn == nil {
		logger.Panicf("not found")
	}
	logger.Tracef(txn.String())

	if err := txn.Do(txn.End); err != nil {
		logger.Panicf("failed to end transaction %s: %s", txn, err)
	}

	logger.Tracef("unregister")
	f.transactions.Remove(params.Txid)
}

func (f *FSM) applyCheckpoint(logger *logging.Logger, params *commands.Checkpoint) {
	conn := f.connections.Follower(params.Name)

	logger.Tracef("filename '%s'", params.Name)

	// See if we can use the leader connection that triggered the
	// checkpoint.
	for _, leaderConn := range f.connections.Leaders(params.Name) {
		// XXX TODO: choose correct leader connection, without
		//           assuming that there is at most one
		conn = leaderConn
		logger.Tracef("using leader connection %d", f.connections.Serial(conn))
		break
	}

	if txn := f.transactions.GetByConn(conn); txn != nil {
		// Something went really wrong, a checkpoint should never be issued
		// while a follower transaction is in flight.
		logger.Panicf("can't run with transaction %s %s", txn.ID(), txn)
	}

	// Run the checkpoint.
	logFrames := 0
	checkpointedFrames := 0
	err := sqlite3x.ReplicationCheckpoint(
		conn, sqlite3x.WalCheckpointTruncate, &logFrames, &checkpointedFrames)
	if err != nil {
		logger.Panicf("failure: %s", err)
	}
	if logFrames != 0 {
		logger.Panicf("%d frames are still in the WAL", logFrames)
	}
	if checkpointedFrames != 0 {
		logger.Panicf("only %d frames were checkpointed", checkpointedFrames)
	}
}

// Snapshot is used to support log compaction.
//
// From the raft's package documentation:
//
//   "This call should return an FSMSnapshot which can be used to save a
//   point-in-time snapshot of the FSM. Apply and Snapshot are not called in
//   multiple threads, but Apply will be called concurrently with Persist. This
//   means the FSM should be implemented in a fashion that allows for
//   concurrent updates while a snapshot is happening."
//
// In dqlite's case we do the following:
//
// - For each database that we track (i.e. that we have a follower connection
//   for), create a backup using sqlite3_backup() and then read the content of
//   the backup file and the current WAL file. Since nothing else is writing to
//   the database (FSM.Apply won't be called until FSM.Snapshot completes), we
//   could probably read the database bytes directly to increase efficiency,
//   but for now we do current-write-safe backup as good measure.
//
// - For each database we track, look for ongoing transactions and include
//   their ID in the FSM snapshot, so their state can be re-created upon
//   snapshot Restore.
//
// This is a bit heavy-weight but should be safe. Optimizations can be added as
// needed.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	logger := f.logger.Augment("snapshot: ")
	logger.Tracef("start")

	backups := []*fsmDatabaseSnapshot{}

	// Loop through all known databases and create a backup for each of
	// them. The filenames associated with follower connections are
	// authoritative, since there will be one and only follower connection
	// for each known database (we never close follower connections since
	// database deletion is not yet supported).
	for _, name := range f.connections.FilenamesOfFollowers() {
		backup, err := f.snapshotDatabase(logger, name)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot backup database %s", name)
		}
		// Append the backup bytes to the snapshot.
		backups = append(backups, backup)
	}

	logger.Tracef("done")

	return &FSMSnapshot{
		backups: backups,
	}, nil
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other commands. The FSM must discard all previous
// state.
func (f *FSM) Restore(reader io.ReadCloser) error {
	logger := f.logger.Augment("restore: ")
	logger.Tracef("start")

	for {
		done, err := f.restoreDatabase(logger, reader)
		if err != nil {
			return err
		}
		if done {
			break
		}
	}

	logger.Tracef("done")

	return nil
}

// Backup a single database.
func (f *FSM) snapshotDatabase(logger *logging.Logger, name string) (*fsmDatabaseSnapshot, error) {
	logger = logger.Augment(fmt.Sprintf("%s: ", name))
	logger.Tracef("backup")

	// Figure out if there is an ongoing transaction associated with any of
	// the database connections, if so we'll return an error.
	conns := f.connections.Leaders(name)
	conns = append(conns, f.connections.Follower(name))

	txid := ""
	for _, conn := range conns {
		if txn := f.transactions.GetByConn(conn); txn != nil {
			txn.Enter()
			state := txn.State()
			txn.Exit()
			if state != transaction.Started && state != transaction.Ended {
				return nil, fmt.Errorf("transaction %s is in progress", txn)
			}
			logger.Tracef("found txn %s: %s", txn.ID(), txn.String())
			txid = txn.ID()
		}
	}

	database, wal, err := connection.Snapshot(f.dir, name)
	if err != nil {
		return nil, err
	}
	return &fsmDatabaseSnapshot{
		filename: name,
		database: database,
		wal:      wal,
		txid:     txid,
	}, nil
}

// Restore a single database. Returns true if there are more databases
// to restore, false otherwise.
func (f *FSM) restoreDatabase(logger *logging.Logger, reader io.ReadCloser) (bool, error) {
	done := false

	// The first 8 bytes contain the size of database.
	var dataSize uint64
	if err := binary.Read(reader, binary.LittleEndian, &dataSize); err != nil {
		return false, errors.Wrap(err, "failed to read database size")
	}
	logger.Tracef("database size %d", dataSize)

	// Then there's the database data..
	data := make([]byte, dataSize)
	if _, err := io.ReadFull(reader, data); err != nil {
		return false, errors.Wrap(err, "failed to read database data")
	}

	// Next, the size of the WAL.
	var walSize uint64
	if err := binary.Read(reader, binary.LittleEndian, &walSize); err != nil {
		return false, errors.Wrap(err, "failed to read wal size")
	}
	logger.Tracef("wal size %d", walSize)

	// Read the WAL data.
	wal := make([]byte, walSize)
	if _, err := io.ReadFull(reader, wal); err != nil {
		return false, errors.Wrap(err, "failed to read wal data")
	}

	// Read the database filename.
	bufReader := bufio.NewReader(reader)
	filename, err := bufReader.ReadString(0)
	if err != nil {
		return false, errors.Wrap(err, "failed to read database name")
	}
	filename = filename[:len(filename)-1] // Strip the trailing 0
	logger = logger.Augment(fmt.Sprintf("%s :", filename))
	logger.Tracef("done reading database and WAL bytes")

	// Check that there are no leader connections for this database.
	//
	// FIXME: we should relax this, as it prevents restoring snapshots "on
	// the fly".
	conns := f.connections.Leaders(filename)
	if len(conns) > 0 {
		logger.Panicf("found %d leader connections", len(conns))
	}

	// XXX TODO: reason about this situation, is it possible?
	/*txn := f.transactions.GetByConn(f.connections.Follower(name))
	if txn != nil {
		f.logger.Printf("[WARN] dqlite: fsm: closing follower in-flight transaction %s", txn)
		f.transactions.Remove(txn.ID())
	}*/

	// Close any follower connection, since we're going to overwrite the
	// database file.
	if f.connections.HasFollower(filename) {
		logger.Tracef("close open follower")
		follower := f.connections.Follower(filename)
		f.connections.DelFollower(filename)
		if err := follower.Close(); err != nil {
			return false, err
		}
	}

	// At this point there should be not connection open against this
	// database, so it's safe to overwrite it.
	txid, err := bufReader.ReadString(0)
	if err != nil {
		if err != io.EOF {
			return false, errors.Wrap(err, "failed to read txid")
		}
		done = true // This is the last database.
	}
	logger.Tracef("txid %s", txid)

	logger.Tracef("restore")
	if err := connection.Restore(f.dir, filename, data, wal); err != nil {
		return false, err
	}

	logger.Tracef("open follower")
	uri := filepath.Join(f.dir, filename)
	conn, err := connection.OpenFollower(uri)
	if err != nil {
		return false, err
	}
	f.connections.AddFollower(filename, conn)

	if txid != "" {
		logger.Tracef("restore transaction")
		conn := f.connections.Follower(filename)
		txn := f.transactions.AddFollower(conn, txid)
		txn.Enter()
		defer txn.Exit()
		if err := txn.Begin(); err != nil {
			return false, err
		}
	}

	return done, nil

}

// FSMSnapshot is returned by an FSM in response to a Snapshot
// It must be safe to invoke FSMSnapshot methods with concurrent
// calls to Apply.
type FSMSnapshot struct {
	backups []*fsmDatabaseSnapshot
}

// Persist should dump all necessary state to the WriteCloser 'sink',
// and call sink.Close() when finished or call sink.Cancel() on error.
func (s *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	for _, backup := range s.backups {
		if err := s.persistBackup(sink, backup); err != nil {
			sink.Cancel()
			return err
		}

	}

	if err := sink.Close(); err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

// Persist a single backup file.
func (s *FSMSnapshot) persistBackup(sink raft.SnapshotSink, backup *fsmDatabaseSnapshot) error {
	// Start by writing the size of the backup
	buffer := new(bytes.Buffer)
	dataSize := uint64(len(backup.database))
	if err := binary.Write(buffer, binary.LittleEndian, dataSize); err != nil {
		return errors.Wrap(err, "failed to encode data size")
	}
	if _, err := sink.Write(buffer.Bytes()); err != nil {
		return errors.Wrap(err, "failed to write data size to sink")
	}

	// Next write the data to the sink.
	if _, err := sink.Write(backup.database); err != nil {
		return errors.Wrap(err, "failed to write backup data to sink")

	}

	buffer.Reset()
	walSize := uint64(len(backup.wal))
	if err := binary.Write(buffer, binary.LittleEndian, walSize); err != nil {
		return errors.Wrap(err, "failed to encode wal size")
	}
	if _, err := sink.Write(buffer.Bytes()); err != nil {
		return errors.Wrap(err, "failed to write wal size to sink")
	}
	if _, err := sink.Write(backup.wal); err != nil {
		return errors.Wrap(err, "failed to write backup data to sink")

	}

	// Next write the database name.
	buffer.Reset()
	buffer.WriteString(backup.filename)
	if _, err := sink.Write(buffer.Bytes()); err != nil {
		return errors.Wrap(err, "failed to write database name to sink")
	}
	if _, err := sink.Write([]byte{0}); err != nil {
		return errors.Wrap(err, "failed to write database name delimiter to sink")
	}

	// FInally write the current transaction ID, if any.
	buffer.Reset()
	buffer.WriteString(backup.txid)
	if _, err := sink.Write(buffer.Bytes()); err != nil {
		return errors.Wrap(err, "failed to write txid to sink")
	}

	return nil
}

// Release is invoked when we are finished with the snapshot.
func (s *FSMSnapshot) Release() {
}

// fsmDatabaseSnapshot holds backup information for a single database.
type fsmDatabaseSnapshot struct {
	filename string
	database []byte
	wal      []byte
	txid     string
}
