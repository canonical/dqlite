package replication

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"path/filepath"
	"sync"

	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/CanonicalLtd/dqlite/internal/log"
	"github.com/CanonicalLtd/dqlite/internal/protocol"
	"github.com/CanonicalLtd/dqlite/internal/transaction"
	"github.com/CanonicalLtd/go-sqlite3"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

// NewFSM creates a new Raft state machine for executing dqlite-specific
// command.
func NewFSM(l *log.Logger, dir string, c *connection.Registry, t *transaction.Registry) *FSM {
	return &FSM{
		logger:       l.Augment("fsm"),
		dir:          dir,
		connections:  c,
		transactions: t,
	}
}

// FSM implements the raft finite-state machine used to replicate
// SQLite data.
type FSM struct {
	logger       *log.Logger
	dir          string
	connections  *connection.Registry  // Open connections (either leaders or followers).
	transactions *transaction.Registry // Ongoing write transactions.
	index        uint64                // Last Raft index that has been successfully applied.

	mu sync.Mutex // Only used on 386 as alternative to StoreUint64
}

// Logger used by this FSM.
func (f *FSM) Logger() *log.Logger {
	return f.logger
}

// Dir is the directory where this FSM stores the replicated SQLite files.
func (f *FSM) Dir() string {
	return f.dir
}

// Connections returns the internal connections registry used by this FSM.
func (f *FSM) Connections() *connection.Registry {
	return f.connections
}

// Transactions returns the internal transactions registry used by this FSM.
func (f *FSM) Transactions() *transaction.Registry {
	return f.transactions
}

// Apply log is invoked once a log entry is committed.
// It returns a value which will be made available in the
// ApplyFuture returned by Raft.Apply method if that
// method was called on the same Raft node as the FSM.
func (f *FSM) Apply(log *raft.Log) interface{} {
	logger := f.logger.Augment(fmt.Sprintf("apply log %d", log.Index))
	logger.Tracef("start")

	cmd, err := protocol.UnmarshalCommand(log.Data)
	if err != nil {
		logger.Panicf("corrupted: %v", err)
	}

	logger = logger.Augment(cmd.Name())
	logger.Tracef("start")
	defer func() {
		if result := recover(); result != nil {
			logger.Errorf("%v", result)
			logger.Errorf("\n%s\n%s", f.connections.Dump(), f.transactions.Dump())
			panic(result)
		}
	}()

	switch params := cmd.Params.(type) {
	case *protocol.Command_Open:
		f.applyOpen(logger, params.Open)
	case *protocol.Command_Begin:
		f.applyBegin(logger, params.Begin)
	case *protocol.Command_WalFrames:
		f.applyWalFrames(logger, params.WalFrames)
	case *protocol.Command_Undo:
		f.applyUndo(logger, params.Undo)
	case *protocol.Command_End:
		f.applyEnd(logger, params.End)
	case *protocol.Command_Checkpoint:
		f.applyCheckpoint(logger, params.Checkpoint)
	default:
		logger.Panicf("unhandled params type")
	}

	f.saveIndex(log.Index)

	logger.Tracef("done")
	return nil
}

func (f *FSM) applyOpen(logger *log.Logger, params *protocol.Open) {
	logger.Tracef("add follower for %s", params.Name)
	conn, err := connection.OpenFollower(filepath.Join(f.dir, params.Name))
	if err != nil {
		logger.Panicf("failed to open follower connection: %v", err)
	}
	f.connections.AddFollower(params.Name, conn)
}

func (f *FSM) applyBegin(logger *log.Logger, params *protocol.Begin) {
	logger = logger.Augment(fmt.Sprintf("txn %s", params.Txid))

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
				logger.Panicf("dangling transaction %s", txn)
			}
		}

		conn := f.connections.Follower(params.Name)
		txn = f.transactions.AddFollower(conn, params.Txid)
	}

	if err := f.txnDo(logger, txn, txn.Begin); err != nil {
		logger.Panicf("failed to begin transaction %s: %s", txn, err)
	}
}

func (f *FSM) applyWalFrames(logger *log.Logger, params *protocol.WalFrames) {
	logger = logger.Augment(fmt.Sprintf("txn %s", params.Txid))

	txn := f.transactions.GetByID(params.Txid)
	if txn == nil {
		logger.Panicf("not found")
	}

	logger.Tracef(txn.String())
	logger.Tracef("pages=%d commit=%d", len(params.Pages), params.IsCommit)

	pages := sqlite3.NewReplicationPages(len(params.Pages), int(params.PageSize))
	defer sqlite3.DestroyReplicationPages(pages)

	for i, page := range params.Pages {
		pages[i].Fill(page.Data, uint16(page.Flags), page.Number)
	}

	framesParams := &sqlite3.ReplicationWalFramesParams{
		PageSize:  int(params.PageSize),
		Pages:     pages,
		Truncate:  uint32(params.Truncate),
		IsCommit:  int(params.IsCommit),
		SyncFlags: uint8(params.SyncFlags),
	}

	if err := f.txnDo(logger, txn, func() error { return txn.WalFrames(framesParams) }); err != nil {
		logger.Panicf("failed to write farames: %s", err)
	}
}

func (f *FSM) applyUndo(logger *log.Logger, params *protocol.Undo) {
	logger = logger.Augment(fmt.Sprintf("txn %s", params.Txid))
	txn := f.transactions.GetByID(params.Txid)
	if txn == nil {
		logger.Panicf("not found")
	}
	logger.Tracef(txn.String())

	if err := f.txnDo(logger, txn, txn.Undo); err != nil {
		logger.Panicf("failed to undo transaction %s: %s", txn, err)
	}
}

func (f *FSM) applyEnd(logger *log.Logger, params *protocol.End) {
	logger = logger.Augment(fmt.Sprintf("txn %s", params.Txid))

	txn := f.transactions.GetByID(params.Txid)
	if txn == nil {
		logger.Panicf("not found")
	}
	logger.Tracef(txn.String())

	if err := f.txnDo(logger, txn, txn.End); err != nil {
		logger.Panicf("failed to end transaction %s: %s", txn, err)
	}

	logger.Tracef("unregister")
	f.transactions.Remove(params.Txid)
}

func (f *FSM) applyCheckpoint(logger *log.Logger, params *protocol.Checkpoint) {
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
	err := sqlite3.ReplicationCheckpoint(
		conn, sqlite3.WalCheckpointTruncate, &logFrames, &checkpointedFrames)
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
	logger := f.logger.Augment("snapshot")
	logger.Tracef("start")

	databases := []*fsmDatabaseSnapshot{}

	// Loop through all known databases and create a backup for each of
	// them. The filenames associated with follower connections uniquely
	// identify all known databases, since there will be one and only
	// follower connection for each known database (we never close follower
	// connections since database deletion is not yet supported).
	for _, name := range f.connections.FilenamesOfFollowers() {
		database, err := f.snapshotDatabase(logger, name)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot snapshot database %s", name)
		}
		databases = append(databases, database)
	}

	logger.Tracef("done")

	return &FSMSnapshot{
		index:     f.Index(),
		databases: databases,
	}, nil
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state.
func (f *FSM) Restore(reader io.ReadCloser) error {
	logger := f.logger.Augment("restore")
	logger.Tracef("start")

	// The first 8 bytes contain the FSM Raft log index.
	var index uint64
	if err := binary.Read(reader, binary.LittleEndian, &index); err != nil {
		return errors.Wrap(err, "failed to read FSM index")
	}
	f.saveIndex(index)
	logger.Tracef("index %d", index)

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
func (f *FSM) snapshotDatabase(logger *log.Logger, path string) (*fsmDatabaseSnapshot, error) {
	logger = logger.Augment(path)
	logger.Tracef("backup")

	// Figure out if there is an ongoing transaction associated with any of
	// the database connections, if so we'll return an error.
	follower := f.connections.Follower(path)
	conns := f.connections.Leaders(path)
	conns = append(conns, follower)
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

	database, wal, err := connection.Snapshot(filepath.Join(f.dir, path))
	if err != nil {
		return nil, err
	}
	return &fsmDatabaseSnapshot{
		filename: path,
		database: database,
		wal:      wal,
		txid:     txid,
	}, nil
}

// Restore a single database. Returns true if there are more databases
// to restore, false otherwise.
func (f *FSM) restoreDatabase(logger *log.Logger, reader io.ReadCloser) (bool, error) {
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

	// Read the database path.
	bufReader := bufio.NewReader(reader)
	filename, err := bufReader.ReadString(0)
	if err != nil {
		return false, errors.Wrap(err, "failed to read database name")
	}
	filename = filename[:len(filename)-1] // Strip the trailing 0
	logger = logger.Augment(filename)
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

	if err := connection.Restore(filepath.Join(f.dir, filename), data, wal); err != nil {
		return false, err
	}

	logger.Tracef("open follower")
	conn, err := connection.OpenFollower(filepath.Join(f.dir, filename))
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

func (f *FSM) txnDo(logger *log.Logger, txn *transaction.Txn, fn func() error) error {
	logger.Tracef("perform on conn %d", f.connections.Serial(txn.Conn()))
	return txn.Do(fn)
}

// FSMSnapshot is returned by an FSM in response to a Snapshot
// It must be safe to invoke FSMSnapshot methods with concurrent
// calls to Apply.
type FSMSnapshot struct {
	index     uint64
	databases []*fsmDatabaseSnapshot
}

// Persist should dump all necessary state to the WriteCloser 'sink',
// and call sink.Close() when finished or call sink.Cancel() on error.
func (s *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	// First, write the FSM index.
	buffer := new(bytes.Buffer)
	if err := binary.Write(buffer, binary.LittleEndian, s.index); err != nil {
		return errors.Wrap(err, "failed to FSM index")
	}
	if _, err := sink.Write(buffer.Bytes()); err != nil {
		return errors.Wrap(err, "failed to write FSM index to sink")
	}

	// Then write the individual databases.
	for _, database := range s.databases {
		if err := s.persistDatabase(sink, database); err != nil {
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

// Persist a single daabase snapshot.
func (s *FSMSnapshot) persistDatabase(sink raft.SnapshotSink, database *fsmDatabaseSnapshot) error {
	// Start by writing the size of the backup
	buffer := new(bytes.Buffer)
	dataSize := uint64(len(database.database))
	if err := binary.Write(buffer, binary.LittleEndian, dataSize); err != nil {
		return errors.Wrap(err, "failed to encode data size")
	}
	if _, err := sink.Write(buffer.Bytes()); err != nil {
		return errors.Wrap(err, "failed to write data size to sink")
	}

	// Next write the data to the sink.
	if _, err := sink.Write(database.database); err != nil {
		return errors.Wrap(err, "failed to write backup data to sink")

	}

	buffer.Reset()
	walSize := uint64(len(database.wal))
	if err := binary.Write(buffer, binary.LittleEndian, walSize); err != nil {
		return errors.Wrap(err, "failed to encode wal size")
	}
	if _, err := sink.Write(buffer.Bytes()); err != nil {
		return errors.Wrap(err, "failed to write wal size to sink")
	}
	if _, err := sink.Write(database.wal); err != nil {
		return errors.Wrap(err, "failed to write backup data to sink")

	}

	// Next write the database name.
	buffer.Reset()
	buffer.WriteString(database.filename)
	if _, err := sink.Write(buffer.Bytes()); err != nil {
		return errors.Wrap(err, "failed to write database name to sink")
	}
	if _, err := sink.Write([]byte{0}); err != nil {
		return errors.Wrap(err, "failed to write database name delimiter to sink")
	}

	// FInally write the current transaction ID, if any.
	buffer.Reset()
	buffer.WriteString(database.txid)
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
