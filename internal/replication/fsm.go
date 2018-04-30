// Copyright 2017 Canonical Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package replication

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"path/filepath"

	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/CanonicalLtd/dqlite/internal/protocol"
	"github.com/CanonicalLtd/dqlite/internal/registry"
	"github.com/CanonicalLtd/dqlite/internal/trace"
	"github.com/CanonicalLtd/dqlite/internal/transaction"
	"github.com/CanonicalLtd/go-sqlite3"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

// FSM implements the raft finite-state machine used to replicate
// SQLite data.
type FSM struct {
	registry *registry.Registry

	// Whether to make Apply panic when an error occurs, or to simply
	// return an error. This should always be is true except for unit
	// tests.
	panicOnFailure bool

	noopBeginTxn uint64 // For upgrades
}

// NewFSM creates a new Raft state machine for executing dqlite-specific
// command.
func NewFSM(registry *registry.Registry) *FSM {
	return &FSM{
		registry:       registry,
		panicOnFailure: true,
	}
}

// Apply log is invoked once a log entry is committed.  It returns a value
// which will be made available in the ApplyFuture returned by Raft.Apply
// method if that method was called on the same Raft node as the FSM.
func (f *FSM) Apply(log *raft.Log) interface{} {
	// Lock the registry for the entire duration of the command
	// handlers. This is fine since no other write change can happen
	// anyways while we're running. This might slowdown a bit opening new
	// leader connections, but since application should be designed to open
	// their leaders once for all, it shouldn't be a problem in
	// practice. Read transactions are not be affected by the locking.
	f.registry.Lock()
	defer f.registry.Unlock()

	tracer := f.registry.TracerFSM()

	// If we're being invoked in the context of a Methods replication hook
	// applying a log command, block execution of any log commands coming
	// on the wire from other leaders until the hook as completed.
	if f.registry.HookSyncPresent() && !f.registry.HookSyncMatches(log.Data) {
		tracer.Message("wait for methods hook to complete")
		// This will temporarily release and re-acquire the registry lock.
		f.registry.HookSyncWait()
	}

	err := f.apply(tracer, log)
	if err != nil {
		if f.panicOnFailure {
			tracer.Panic("%v", err)
		}
		tracer.Error("apply failed", err)
		return err
	}

	return nil
}

func (f *FSM) apply(tracer *trace.Tracer, log *raft.Log) error {
	tracer = tracer.With(
		trace.Integer("term", int64(log.Term)),
		trace.Integer("index", int64(log.Index)),
	)

	cmd, err := protocol.UnmarshalCommand(log.Data)
	if err != nil {
		return errors.Wrap(err, "corrupted command data")
	}
	tracer = tracer.With(trace.String("cmd", cmd.Name()))

	switch payload := cmd.Payload.(type) {
	case *protocol.Command_Open:
		err = f.applyOpen(tracer, payload.Open)
		err = errors.Wrapf(err, "open %s", payload.Open.Name)
	case *protocol.Command_Begin:
		err = f.applyBegin(tracer, payload.Begin)
		err = errors.Wrapf(err, "begin txn %d on %s", payload.Begin.Txid, payload.Begin.Name)
	case *protocol.Command_Frames:
		err = f.applyFrames(tracer, payload.Frames)
		err = errors.Wrapf(err, "wal frames txn %d (%v)", payload.Frames.Txid, payload.Frames.IsCommit)
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
		tracer.Error("failed", err)
		return err
	}

	f.registry.IndexUpdate(log.Index)

	return nil
}

func (f *FSM) applyOpen(tracer *trace.Tracer, params *protocol.Open) error {
	tracer = tracer.With(
		trace.String("name", params.Name),
	)
	tracer.Message("start")

	conn, err := connection.OpenFollower(filepath.Join(f.registry.Dir(), params.Name))
	if err != nil {
		return err
	}
	f.registry.ConnFollowerAdd(params.Name, conn)

	tracer.Message("done")

	return nil
}

func (f *FSM) applyBegin(tracer *trace.Tracer, params *protocol.Begin) error {
	tracer = tracer.With(
		trace.Integer("txn", int64(params.Txid)),
	)

	// This FSM command is not needed anymore. We make it a no-op, for
	// backward compatibility with deployments that do have it stored in
	// their raft logs.
	tracer.Message("no-op")
	f.noopBeginTxn = params.Txid

	return nil
}

func (f *FSM) applyFrames(tracer *trace.Tracer, params *protocol.Frames) error {
	tracer = tracer.With(
		trace.Integer("txn", int64(params.Txid)),
		trace.Integer("pages", int64(len(params.Pages))),
		trace.Integer("commit", int64(params.IsCommit)))
	tracer.Message("start")

	if params.Filename == "" {
		// Backward compatibility with existing LXD deployments.
		params.Filename = "db.bin"
	}

	txn := f.registry.TxnByID(params.Txid)
	begin := true

	if txn != nil {
		// We know about this transaction.
		tracer.Message("txn found %s", txn)

		if txn.IsLeader() {
			// We're executing a Frames command triggered by the
			// Methods.Frames hook on this servers.
			if txn.IsZombie() {
				// The only way that this can be a zombie is if
				// this Frames command is being executed by
				// this FSM after this leader failed with
				// ErrLeadershipLost, and 1) this server was
				// re-elected right away and has successfully
				// retried to apply this command or 2) another
				// server was elected and a quorum was still
				// reached for this command log despite the
				// previous leader not getting notified about
				// it.
				if params.IsCommit == 0 {
					// This is not a commit frames
					// command. Regardless of whether 1) or
					// 2) happened, it's safe to create a
					// surrogate follower transaction and
					// transition it to Writing.
					//
					// If 1) happens, then the next
					// Methods.Begin hook on this server
					// will find a leftover Writing
					// follower and will roll it back with
					// an Undo command. If 2) happens, same.
					tracer.Message("create surrogate follower", txn)
					txn = f.registry.TxnFollowerSurrogate(txn)
				} else {
					// This is a commit frames
					// command. Regardless of whether 1) or
					// 2) happened, we need to resurrect
					// the zombie into a follower and
					// possibly re-apply any non-commit
					// frames that were applied so far in
					// the transaction.
					tracer.Message("recover commit")
					conn := f.registry.ConnFollower(params.Filename)
					var err error
					txn, err = txn.Resurrect(conn)
					if err != nil {
						return err
					}
					f.registry.TxnFollowerResurrected(txn)
					begin = txn.State() == transaction.Pending
				}
			} else {
				// We're executing this FSM command in during
				// the execution of the Methods.Frames hook.
			}

		} else {
			// We're executing the Frames command as followers. The
			// transaction must be in the Writing state.
			if txn.State() != transaction.Writing {
				tracer.Panic("unexpected transaction %s", txn)
			}
			begin = false
		}
	} else {
		// We don't know about this transaction.
		//
		// This is must be a new follower transaction. Let's make sure
		// that no other transaction against this database is happening
		// on this server.
		if txn := f.registry.TxnByFilename(params.Filename); txn != nil {
			if txn.IsZombie() {
				// This transactions was left around by a
				// leader that lost leadership during a Frames
				// hook that was the first to be sent and did
				// not reach a quorum, so no other server knows
				// about it, and now we're starting a new
				// trasaction initiated by a new leader. We can
				// just purge it from the registry, since its
				// state was already rolled back by SQLite
				// after the xFrames hook failure.
				tracer.Message("found zombie transaction %s", txn)

				// Perform some sanity checks.
				if txn.ID() > params.Txid {
					tracer.Panic("zombie transaction too recent %s", txn)
				}
				if txn.State() != transaction.Pending {
					tracer.Panic("unexpected transaction state %s", txn)
				}

				tracer.Message("removing stale zombie transaction %s", txn)
				f.registry.TxnDel(txn.ID())
			} else {
				tracer.Panic("unexpected transaction %s", txn)
			}
		}

		conn := f.registry.ConnFollower(params.Filename)
		txn = f.registry.TxnFollowerAdd(conn, params.Txid)
	}

	framesParams := &sqlite3.ReplicationFramesParams{
		PageSize:  int(params.PageSize),
		Truncate:  uint32(params.Truncate),
		IsCommit:  int(params.IsCommit),
		SyncFlags: uint8(params.SyncFlags),
	}
	pages := sqlite3.NewReplicationPages(len(params.Pages), int(params.PageSize))

	for i, page := range params.Pages {
		pages[i].Fill(page.Data, uint16(page.Flags), page.Number)
	}

	framesParams.Pages = pages
	if err := txn.Frames(begin, framesParams); err != nil {
		return err
	}

	// If the commit flag is on, this is the final write of a transaction,
	if framesParams.IsCommit > 0 {
		// Save the ID of this transaction in the buffer of recently committed
		// transactions.
		f.registry.TxnCommittedAdd(txn)

		// If it's a follower, we also unregister it.
		if !txn.IsLeader() {
			tracer.Message("unregister txn")
			f.registry.TxnDel(params.Txid)
		}
	}

	tracer.Message("done")

	f.noopBeginTxn = 0 // Backward compat.

	return nil
}

func (f *FSM) applyUndo(tracer *trace.Tracer, params *protocol.Undo) error {
	tracer = tracer.With(
		trace.Integer("txn", int64(params.Txid)),
	)
	tracer.Message("start")

	txn := f.registry.TxnByID(params.Txid)

	if txn != nil {
		// We know about this transaction.
		tracer.Message("txn found: %s", txn)
	} else {
		if f.noopBeginTxn != params.Txid {
			tracer.Panic("txn not found")
		}
		f.noopBeginTxn = 0
		return nil
	}

	if err := txn.Undo(); err != nil {
		return err
	}

	// Let's decide whether to remove the transaction from the registry or
	// not. The following scenarios are possible:
	//
	// 1. This is a non-zombie leader transaction. We can assume that this
	//    command is being applied in the context of a Methods.Undo() hook
	//    execution, which will wait for the command to succeed and then
	//    remove the transaction by itself in the End hook, so no need to
	//    remove it here.
	//
	// 2. This is a follower transaction. We're done here, since undone is
	//    a final state, so let's remove the transaction.
	//
	// 3. This is a zombie leader transaction. This can happen if the
	//    leader lost leadership when applying the a non-commit frames, but
	//    the command was still committed (either by us is we were
	//    re-elected, or by another server if the command still reached a
	//    quorum). In that case the we're handling an Undo command to
	//    rollback a dangling transaction, and we have to remove the zombie
	//    ourselves, because nobody else would do it otherwise.
	if !txn.IsLeader() || txn.IsZombie() {
		tracer.Message("unregister txn")
		f.registry.TxnDel(params.Txid)
	}

	tracer.Message("done")

	return nil
}

func (f *FSM) applyEnd(tracer *trace.Tracer, params *protocol.End) error {
	tracer = tracer.With(
		trace.Integer("txn", int64(params.Txid)),
	)

	// This FSM command is not needed anymore. We make it a no-op, for
	// backward compatibility with deployments that do have it stored in
	// their raft logs.
	tracer.Message("no-op")

	return nil
}

func (f *FSM) applyCheckpoint(tracer *trace.Tracer, params *protocol.Checkpoint) error {
	tracer = tracer.With(
		trace.String("file", params.Name),
	)
	tracer.Message("start")

	conn := f.registry.ConnFollower(params.Name)

	if txn := f.registry.TxnByConn(conn); txn != nil {
		// Something went really wrong, a checkpoint should never be issued
		// while a follower transaction is in flight.
		tracer.Panic("can't run checkpoint concurrently with transaction %s", txn)
	}

	// Run the checkpoint.
	logFrames, checkpointedFrames, err := conn.WalCheckpoint("main", sqlite3.WalCheckpointTruncate)
	if err != nil {
		return err
	}
	if logFrames != 0 {
		tracer.Panic("%d frames are still in the WAL", logFrames)
	}
	if checkpointedFrames != 0 {
		tracer.Panic("only %d frames were checkpointed", checkpointedFrames)
	}

	tracer.Message("done")

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
	f.registry.Lock()
	defer f.registry.Unlock()

	tracer := f.registry.TracerFSM()

	databases := []*fsmDatabaseSnapshot{}

	// Loop through all known databases and create a backup for each of
	// them. The filenames associated with follower connections uniquely
	// identify all known databases, since there will be one and only
	// follower connection for each known database (we never close follower
	// connections since database deletion is not supported).
	for _, filename := range f.registry.ConnFollowerFilenames() {
		database, err := f.snapshotDatabase(tracer, filename)
		if err != nil {
			err = errors.Wrapf(err, "%s", filename)
			tracer.Error("database snapshot failed", err)
			return nil, err
		}
		databases = append(databases, database)
	}

	return &FSMSnapshot{
		index:     f.registry.Index(),
		databases: databases,
	}, nil
}

// Backup a single database.
func (f *FSM) snapshotDatabase(tracer *trace.Tracer, filename string) (*fsmDatabaseSnapshot, error) {
	tracer = tracer.With(trace.String("snapshot", filename))
	tracer.Message("start")

	// Figure out if there is an ongoing transaction associated with any of
	// the database connections, if so we'll return an error.
	conns := f.registry.ConnLeaders(filename)
	conns = append(conns, f.registry.ConnFollower(filename))
	txid := ""
	for _, conn := range conns {
		if txn := f.registry.TxnByConn(conn); txn != nil {
			// XXX TODO: If we let started transaction in the
			// snapshot, the TestIntegration_Snapshot crashes with:
			//
			// panic: unexpected follower transaction 7 started as follower
			//
			// figure out why.
			//if txn.State() == transaction.Writing {
			tracer.Message("transaction %s is in progress", txn)
			return nil, fmt.Errorf("transaction %s is in progress", txn)
			//}
			// We'll save the transaction ID in the snapshot.
			//tracer.Message("idle transaction %s", txn)
			//txid = strconv.FormatUint(txn.ID(), 10)
		}
	}

	database, wal, err := connection.Snapshot(filepath.Join(f.registry.Dir(), filename))
	if err != nil {
		return nil, err
	}

	tracer.Message("done")

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
	f.registry.Lock()
	defer f.registry.Unlock()

	tracer := f.registry.TracerFSM()

	// The first 8 bytes contain the FSM Raft log index.
	var index uint64
	if err := binary.Read(reader, binary.LittleEndian, &index); err != nil {
		return errors.Wrap(err, "failed to read FSM index")
	}

	tracer = tracer.With(trace.Integer("restore", int64(index)))
	tracer.Message("start")

	f.registry.IndexUpdate(index)

	for {
		done, err := f.restoreDatabase(tracer, reader)
		if err != nil {
			return err
		}
		if done {
			break
		}
	}

	tracer.Message("done")

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
	tracer.Message("database size: %d", dataSize)

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
	tracer.Message("wal size: %d", walSize)

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
	tracer.Message("filename: %s", filename)

	// XXX TODO: reason about this situation, is it harmful?
	// Check that there are no leader connections for this database.
	//
	// FIXME: we should relax this, as it prevents restoring snapshots "on
	// the fly".
	// conns := f.registry.ConnLeaders(filename)
	// if len(conns) > 0 {
	// 	tracer.Panic("found %d leader connections", len(conns))
	// }

	// XXX TODO: reason about this situation, is it possible?
	//txn := f.transactions.GetByConn(f.connections.Follower(name))
	//if txn != nil {
	//	f.logger.Printf("[WARN] dqlite: fsm: closing follower in-flight transaction %s", txn)
	//	f.transactions.Remove(txn.ID())
	//}

	// Close any follower connection, since we're going to overwrite the
	// database file.
	if f.registry.ConnFollowerExists(filename) {
		tracer.Message("close follower: %s", filename)
		follower := f.registry.ConnFollower(filename)
		f.registry.ConnFollowerDel(filename)
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
	tracer.Message("transaction ID: %s", txid)

	path := filepath.Join(f.registry.Dir(), filename)
	if err := connection.Restore(path, data, wal); err != nil {
		return false, err
	}

	tracer.Message("open follower: %s", filename)
	conn, err := connection.OpenFollower(path)
	if err != nil {
		return false, err
	}
	f.registry.ConnFollowerAdd(filename, conn)

	if txid != "" {
		/*
			txid, err := strconv.ParseUint(txid, 10, 64)
			if err != nil {
				return false, err
			}
			tracer.Message("add transaction: %d", txid)
			conn := f.registry.ConnFollower(filename)
			txn := f.registry.TxnFollowerAdd(conn, txid)
			if err := txn.Begin(); err != nil {
				return false, err
			}*/
	}

	return done, nil
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
