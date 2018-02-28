package replication

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/CanonicalLtd/dqlite/internal/protocol"
	"github.com/CanonicalLtd/dqlite/internal/trace"
	"github.com/CanonicalLtd/dqlite/internal/transaction"
	"github.com/CanonicalLtd/go-sqlite3"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

// NewFSM creates a new Raft state machine for executing dqlite-specific
// command.
//
// The 'dir' parameter set the directory where the FSM will save the SQLite
// database files.
func NewFSM(dir string) *FSM {
	fsm := &FSM{
		dir:            dir,
		connections:    connection.NewRegistry(),
		transactions:   transaction.NewRegistry(),
		tracers:        trace.NewRegistry(250),
		panicOnFailure: true,
	}
	fsm.tracers.Add("fsm")

	return fsm
}

// FSM implements the raft finite-state machine used to replicate
// SQLite data.
type FSM struct {
	dir          string
	connections  *connection.Registry  // Open connections (either leaders or followers).
	transactions *transaction.Registry // Ongoing write transactions.
	tracers      *trace.Registry       // Registry of event tracers.
	index        uint64                // Last Raft index that has been successfully applied.

	mu sync.Mutex // Only used on 386 as alternative to StoreUint64

	// Whether to make Apply panic when an error occurs, or to simply
	// return an error. This should always be is true except for unit
	// tests.
	panicOnFailure bool
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

// Tracers return the internal tracers registry used by this FSM.
func (f *FSM) Tracers() *trace.Registry {
	return f.tracers
}

// Apply log is invoked once a log entry is committed.
// It returns a value which will be made available in the
// ApplyFuture returned by Raft.Apply method if that
// method was called on the same Raft node as the FSM.
func (f *FSM) Apply(log *raft.Log) interface{} {
	err := f.apply(log)
	if err != nil {
		tracer := f.tracer()
		if f.panicOnFailure {
			tracer.Panic("%v", err)
		}
		//tracer.Error("apply failed", err)
		return err
	}

	return nil
}

func (f *FSM) apply(log *raft.Log) error {
	tracer := f.tracers.Get("fsm").With(trace.Integer("log", int64(log.Index)))

	cmd, err := protocol.UnmarshalCommand(log.Data)
	if err != nil {
		return errors.Wrap(err, "corrupted command data")
	}
	//tracer = //tracer.With(trace.String("cmd", cmd.Name()))

	switch payload := cmd.Payload.(type) {
	case *protocol.Command_Open:
		err = f.applyOpen(tracer, payload.Open)
		err = errors.Wrapf(err, "open %s", payload.Open.Name)
	case *protocol.Command_Begin:
		err = f.applyBegin(tracer, payload.Begin)
		err = errors.Wrapf(err, "begin txn %d on %s", payload.Begin.Txid, payload.Begin.Name)
	case *protocol.Command_WalFrames:
		err = f.applyWalFrames(tracer, payload.WalFrames)
		err = errors.Wrapf(err, "wal frames txn %d (%v)", payload.WalFrames.Txid, payload.WalFrames.IsCommit)
	case *protocol.Command_Undo:
		err = f.applyUndo(tracer, payload.Undo)
		err = errors.Wrapf(err, "undo txn %d", payload.Undo.Txid)
	case *protocol.Command_End:
		err = f.applyEnd(tracer, payload.End)
		err = errors.Wrapf(err, "end txn %d", payload.End.Txid)
	case *protocol.Command_Checkpoint:
		err = f.applyCheckpoint(tracer, payload.Checkpoint)
		err = errors.Wrapf(err, "checkpoint")
	default:
		err = fmt.Errorf("unknown command")
	}

	if err != nil {
		//tracer.Error("failed", err)
		return err
	}

	f.saveIndex(log.Index)

	return nil
}

func (f *FSM) applyOpen(tracer *trace.Tracer, params *protocol.Open) error {
	//tracer = //tracer.With(
	//trace.String("file", params.Name),
	//)
	//tracer.Message("start")

	conn, err := connection.OpenFollower(filepath.Join(f.dir, params.Name))
	if err != nil {
		return err
	}
	f.connections.AddFollower(params.Name, conn)

	//tracer.Message("done")

	return nil
}

func (f *FSM) applyBegin(tracer *trace.Tracer, params *protocol.Begin) error {
	//tracer = //tracer.With(
	//trace.Integer("txn", int64(params.Txid)),
	//trace.String("file", params.Name),
	//)
	//tracer.Message("start")

	txn := f.transactions.GetByID(params.Txid)
	if txn != nil {
		//tracer.Message("txn found, use it")

		// We know about this txid, so the transaction must have
		// originated on this node and be a leader transaction.
		if !txn.IsLeader() {
			tracer.Panic("unexpected follower connection for existing transaction")
		}

		// We don't need to do anything else, since the WAL write
		// transaction was already started by the methods hook.
	} else {
		//tracer.Message("txn not found, add follower")

		// This is must be a new follower transaction. Let's check
		// check that no leader transaction against this database is
		// happening on this node (since we're supposed purely
		// followers, and unreleased write locks would get on the
		// way).
		for _, conn := range f.connections.Leaders(params.Name) {
			if txn := f.transactions.GetByConn(conn); txn != nil {
				tracer.Panic("unexpected transaction %v", txn)
			}
		}

		conn := f.connections.Follower(params.Name)
		txn = f.transactions.AddFollower(conn, params.Txid)

		if err := txn.Do(txn.Begin); err != nil {
			return err
		}
	}

	//tracer.Message("done")

	return nil
}

func (f *FSM) applyWalFrames(tracer *trace.Tracer, params *protocol.WalFrames) error {
	//tracer = //tracer.With(
	//trace.Integer("txn", int64(params.Txid)),
	//trace.Integer("pages", int64(len(params.Pages))),
	//trace.Integer("commit", int64(params.IsCommit)))
	//tracer.Message("start")

	txn := f.transactions.GetByID(params.Txid)
	if txn == nil {
		tracer.Panic("no transaction with ID %d", params.Txid)
	}

	pages := sqlite3.NewReplicationPages(len(params.Pages), int(params.PageSize))

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

	if err := txn.Do(func() error { return txn.WalFrames(framesParams) }); err != nil {
		return err
	}

	//tracer.Message("done")

	return nil
}

func (f *FSM) applyUndo(tracer *trace.Tracer, params *protocol.Undo) error {
	//tracer = //tracer.With(
	// 	trace.Integer("txn", int64(params.Txid)),
	// )
	//tracer.Message("start")

	txn := f.transactions.GetByID(params.Txid)
	if txn == nil {
		tracer.Panic("no transaction with ID %d", params.Txid)
	}

	txn.Enter()
	defer txn.Exit()
	if txn.State() == transaction.Stale {
		// This transaction was initiated by a leader which could not
		// apply the End command and so rolled back the transaction in
		// methods.go. We just no-op in this case.
		return nil
	}

	if err := txn.Undo(); err != nil {
		return err
	}

	//tracer.Message("done")

	return nil
}

func (f *FSM) applyEnd(tracer *trace.Tracer, params *protocol.End) error {
	//tracer = //tracer.With(
	// 	trace.Integer("txn", int64(params.Txid)),
	// )
	//tracer.Message("start")

	txn := f.transactions.GetByID(params.Txid)
	if txn == nil {
		tracer.Panic("no transaction with ID %d", params.Txid)
	}

	txn.Enter()
	defer txn.Exit()
	if txn.State() == transaction.Stale {
		// This transaction was initiated by a leader which could not
		// apply the End command and so rolled back the transaction in
		// methods.go. We just no-op in this case.
	} else if err := txn.End(); err != nil {
		return err
	}

	//tracer.Message("unregister txn")
	f.transactions.Remove(params.Txid)

	//tracer.Message("done")

	return nil
}

func (f *FSM) applyCheckpoint(tracer *trace.Tracer, params *protocol.Checkpoint) error {
	//tracer = //tracer.With(
	// 	trace.String("file", params.Name),
	// )
	//tracer.Message("start")

	conn := f.connections.Follower(params.Name)

	// See if we can use the leader connection that triggered the
	// checkpoint.
	for _, leaderConn := range f.connections.Leaders(params.Name) {
		// XXX TODO: choose correct leader connection, without
		//           assuming that there is at most one
		conn = leaderConn
		//logger.Tracef("using leader connection %d", f.connections.Serial(conn))
		break
	}

	if txn := f.transactions.GetByConn(conn); txn != nil {
		// Something went really wrong, a checkpoint should never be issued
		// while a follower transaction is in flight.
		tracer.Panic("can't run with transaction %s", txn)
	}

	// Run the checkpoint.
	logFrames := 0
	checkpointedFrames := 0
	err := sqlite3.ReplicationCheckpoint(
		conn, sqlite3.WalCheckpointTruncate, &logFrames, &checkpointedFrames)
	if err != nil {
		return err
	}
	if logFrames != 0 {
		tracer.Panic("%d frames are still in the WAL", logFrames)
	}
	if checkpointedFrames != 0 {
		tracer.Panic("only %d frames were checkpointed", checkpointedFrames)
	}

	//tracer.Message("done")

	return nil
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
//   but for now we do concurrent-write-safe backup as good measure.
//
// - For each database we track, look for ongoing transactions and include
//   their ID in the FSM snapshot, so their state can be re-created upon
//   snapshot Restore.
//
// This is a bit heavy-weight but should be safe. Optimizations can be added as
// needed.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	databases := []*fsmDatabaseSnapshot{}

	// Loop through all known databases and create a backup for each of
	// them. The filenames associated with follower connections uniquely
	// identify all known databases, since there will be one and only
	// follower connection for each known database (we never close follower
	// connections since database deletion is not yet supported).
	for _, filename := range f.connections.FilenamesOfFollowers() {
		database, err := f.snapshotDatabase(filename)
		if err != nil {
			err = errors.Wrapf(err, "%s", filename)
			f.tracer().Error("database snapshot failed", err)
			return nil, err
		}
		databases = append(databases, database)
	}

	return &FSMSnapshot{
		index:     f.Index(),
		databases: databases,
	}, nil
}

// Backup a single database.
func (f *FSM) snapshotDatabase(filename string) (*fsmDatabaseSnapshot, error) {
	// tracer := f.tracer().With(trace.String("snapshot", filename))
	//tracer.Message("start")

	// Figure out if there is an ongoing transaction associated with any of
	// the database connections, if so we'll return an error.
	follower := f.connections.Follower(filename)
	conns := f.connections.Leaders(filename)
	conns = append(conns, follower)
	txid := ""
	for _, conn := range conns {
		if txn := f.transactions.GetByConn(conn); txn != nil {
			txn.Enter()
			state := txn.State()
			txn.Exit()
			if state != transaction.Started && state != transaction.Ended {
				//tracer.Message("transaction %s is in progress", txn)
				return nil, fmt.Errorf("transaction %s is in progress", txn)
			}
			//tracer.Message("idle transaction %s", txn)
			txid = strconv.FormatUint(txn.ID(), 10)
		}
	}

	database, wal, err := connection.Snapshot(filepath.Join(f.dir, filename))
	if err != nil {
		return nil, err
	}

	//tracer.Message("done")

	return &fsmDatabaseSnapshot{
		filename: filename,
		database: database,
		wal:      wal,
		txid:     txid,
	}, nil
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state.
func (f *FSM) Restore(reader io.ReadCloser) error {
	// The first 8 bytes contain the FSM Raft log index.
	var index uint64
	if err := binary.Read(reader, binary.LittleEndian, &index); err != nil {
		return errors.Wrap(err, "failed to read FSM index")
	}

	tracer := f.tracer().With(trace.Integer("restore", int64(index)))
	//tracer.Message("start")

	f.saveIndex(index)

	for {
		done, err := f.restoreDatabase(tracer, reader)
		if err != nil {
			return err
		}
		if done {
			break
		}
	}

	//tracer.Message("done")

	return nil
}

// Restore a single database. Returns true if there are more databases
// to restore, false otherwise.
func (f *FSM) restoreDatabase(tracer *trace.Tracer, reader io.ReadCloser) (bool, error) {
	done := false

	// The first 8 bytes contain the size of database.
	var dataSize uint64
	if err := binary.Read(reader, binary.LittleEndian, &dataSize); err != nil {
		return false, errors.Wrap(err, "failed to read database size")
	}
	//tracer.Message("database size: %d", dataSize)

	// Then there's the database data.
	data := make([]byte, dataSize)
	if _, err := io.ReadFull(reader, data); err != nil {
		return false, errors.Wrap(err, "failed to read database data")
	}

	// Next, the size of the WAL.
	var walSize uint64
	if err := binary.Read(reader, binary.LittleEndian, &walSize); err != nil {
		return false, errors.Wrap(err, "failed to read wal size")
	}
	//tracer.Message("wal size: %d", walSize)

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
	//tracer.Message("filename: %s", filename)

	// Check that there are no leader connections for this database.
	//
	// FIXME: we should relax this, as it prevents restoring snapshots "on
	// the fly".
	conns := f.connections.Leaders(filename)
	if len(conns) > 0 {
		tracer.Panic("found %d leader connections", len(conns))
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
		//tracer.Message("close follower: %s", filename)
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
	//tracer.Message("transaction ID: %s", txid)

	if err := connection.Restore(filepath.Join(f.dir, filename), data, wal); err != nil {
		return false, err
	}

	//tracer.Message("open follower: %s", filename)
	conn, err := connection.OpenFollower(filepath.Join(f.dir, filename))
	if err != nil {
		return false, err
	}
	f.connections.AddFollower(filename, conn)

	if txid != "" {
		txid, err := strconv.ParseUint(txid, 10, 64)
		if err != nil {
			return false, err
		}
		//tracer.Message("add transaction: %s", txid)
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

func (f *FSM) tracer() *trace.Tracer {
	return f.tracers.Get("fsm")
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
