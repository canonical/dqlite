package replication

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/dqlite/dqlite/command"
	"github.com/dqlite/dqlite/connection"
	"github.com/dqlite/dqlite/transaction"
	"github.com/dqlite/go-sqlite3x"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

// NewFSM creates a new state machine.
func NewFSM(logger *log.Logger, connections *connection.Registry, transactions *transaction.Registry) *FSM {
	return &FSM{
		logger:       logger,
		connections:  connections,
		transactions: transactions,
		indexes:      make(chan uint64, 10), // Should be enough for tests
	}
}

// FSM implements the raft finite-state machine used to replicate
// SQLite data.
type FSM struct {
	logger       *log.Logger
	connections  *connection.Registry
	transactions *transaction.Registry
	errors       chan error
	indexes      chan uint64
}

// Apply log is invoked once a log entry is committed.
// It returns a value which will be made available in the
// ApplyFuture returned by Raft.Apply method if that
// method was called on the same Raft node as the FSM.
func (f *FSM) Apply(log *raft.Log) interface{} {
	f.logger.Printf("[DEBUG] dqlite: fsm: applying log with index %d", log.Index)

	cmd, err := command.Unmarshal(log.Data)
	if err != nil {
		panic(fmt.Sprintf("fsm apply error: failed to unmarshal command: %s", err))
	}

	switch cmd.Code {
	case command.Begin:
		err = f.applyBegin(cmd)
	case command.WalFrames:
		err = f.applyWalFrames(cmd)
	case command.Undo:
		err = f.applyUndo(cmd)
	case command.End:
		err = f.applyEnd(cmd)
	case command.Checkpoint:
		err = f.applyCheckpoint(cmd)
	default:
		err = fmt.Errorf("invalid code: %d", cmd.Code)
	}

	if err != nil {
		panic(fmt.Sprintf("fsm apply error for command %s: %v", cmd.Code, err))
	}

	// Non-blocking notification of the last applied index. Used
	// by tests for waiting for followers to actually finish
	// committing logs.
	select {
	case f.indexes <- log.Index:
	default:
	}

	f.logger.Printf("[DEBUG] dqlite: fsm: applied log with index %d", log.Index)
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

func (f *FSM) applyBegin(cmd *command.Command) error {
	params, err := cmd.UnmarshalBegin()
	if err != nil {
		return err
	}
	f.logCommand(cmd, params)

	txn := f.transactions.GetByID(params.Txid)
	if txn != nil {
		// We know about this txid, so the transaction must
		// have originated on this node and be a leader
		// transaction.
		if !txn.IsLeader() {
			return fmt.Errorf(
				"existing transaction %s has non-leader connection", txn)
		}
	} else {
		// This is a new follower transaction.

		// Check for stale leader transactions. This might happen when
		// a leader node started a transaction but meanwhile it got
		// deposed before ending it and a follower transaction is now
		// being started on the same node. In this case the stale leader
		// WAL write lock must be released in order let the follower
		// transaction begin.
		for _, conn := range f.connections.Leaders(params.Name) {
			if txn := f.transactions.GetByConn(conn); txn != nil {
				return fmt.Errorf("found dangling leader connection %s", txn)
			}
		}

		conn := f.connections.Follower(params.Name)
		txn = f.transactions.AddFollower(conn, params.Txid)
	}
	txn.Enter()
	defer txn.Exit()

	f.logTxn(cmd, txn)

	return txn.Begin()
}

func (f *FSM) applyWalFrames(cmd *command.Command) error {
	params, err := cmd.UnmarshalWalFrames()
	if err != nil {
		return err
	}
	f.logCommand(cmd, params)

	txn := f.transactions.GetByID(params.Txid)
	if txn == nil {
		return fmt.Errorf("transaction for %s doesn't exist", params)
	}
	txn.Enter()
	defer txn.Exit()

	f.logTxn(cmd, txn)

	pages := sqlite3x.NewReplicationPages(len(params.Pages), params.PageSize)
	defer sqlite3x.DestroyReplicationPages(pages)

	for i, page := range params.Pages {
		data := (*[1 << 30]byte)(pages[i].Data())[:params.PageSize:params.PageSize]
		bytes, err := base64.StdEncoding.DecodeString(page.Data)
		if err != nil {
			return fmt.Errorf("failed to decode page %d data: %s for transaction %s", i, err, txn)
		}
		for i := range bytes {
			data[i] = bytes[i]
		}
		pages[i].Fill(page.Flags, page.Number)
	}

	framesParams := &sqlite3x.ReplicationWalFramesParams{
		PageSize:  params.PageSize,
		Pages:     pages,
		Truncate:  params.Truncate,
		IsCommit:  params.IsCommit,
		SyncFlags: params.SyncFlags,
	}

	return txn.WalFrames(framesParams)
}

func (f *FSM) applyUndo(cmd *command.Command) error {
	params, err := cmd.UnmarshalUndo()
	if err != nil {
		return err
	}
	f.logCommand(cmd, params)

	txn := f.transactions.GetByID(params.Txid)
	if txn == nil {
		return fmt.Errorf("transaction for %s doesn't exist", params)
	}
	txn.Enter()
	defer txn.Exit()

	f.logTxn(cmd, txn)

	return txn.Undo()
}

func (f *FSM) applyEnd(cmd *command.Command) error {
	params, err := cmd.UnmarshalEnd()
	if err != nil {
		return err
	}
	f.logCommand(cmd, params)

	txn := f.transactions.GetByID(params.Txid)
	if txn == nil {
		return fmt.Errorf("transaction for %s doesn't exist", params)
	}
	txn.Enter()
	defer txn.Exit()

	f.logTxn(cmd, txn)

	err = txn.End()

	f.logger.Printf("[DEBUG] dqlite: fsm: remove transaction %s", txn)
	f.transactions.Remove(params.Txid)

	return err
}

func (f *FSM) applyCheckpoint(cmd *command.Command) error {
	params, err := cmd.UnmarshalCheckpoint()
	if err != nil {
		return err
	}

	conn := f.connections.Follower(params.Name)

	f.logger.Printf("[DEBUG] dqlite: fsm: applying checkpoint for '%s'", params.Name)

	// See if we can use the leader connection that triggered the
	// checkpoint.
	for _, leaderConn := range f.connections.Leaders(params.Name) {
		// XXX TODO: choose correct leader connection, without
		//           assuming that there is at most one
		conn = leaderConn
		f.logger.Printf("[DEBUG] dqlite: fsm: using leader connection %p for checkpoint", conn)
		break
	}

	if txn := f.transactions.GetByConn(conn); txn != nil {
		// Something went really wrong, a checkpoint should never be issued
		// while a follower transaction is in flight.
		return fmt.Errorf(
			"checkpoint for database '%s' can't run with transaction %s",
			params.Name, txn)
	}

	// Run the checkpoint.
	logFrames := 0
	checkpointedFrames := 0
	if err := sqlite3x.ReplicationCheckpoint(conn, sqlite3x.WalCheckpointTruncate, &logFrames, &checkpointedFrames); err != nil {
		return errors.Wrap(err, fmt.Sprintf("checkpoint for '%s' failed", params.Name))
	}
	if logFrames != 0 {
		return fmt.Errorf("%d frames still in the WAL after checkpoint for '%s'", logFrames, params.Name)
	}
	if checkpointedFrames != 0 {
		return fmt.Errorf("%d frames were checkpointed for '%s'", checkpointedFrames, params.Name)
	}

	return nil
}

func (f *FSM) logCommand(cmd *command.Command, params command.Params) {
	f.logger.Printf("[DEBUG] dqlite: fsm: applying %s with params %s", cmd.Code, params)
}

func (f *FSM) logTxn(cmd *command.Command, txn *transaction.Txn) {
	f.logger.Printf("[DEBUG] dqlite: fsm: applying %s for transaction %s", cmd.Code, txn)
}

// Snapshot is used to support log compaction. This call should
// return an FSMSnapshot which can be used to save a point-in-time
// snapshot of the FSM. Apply and Snapshot are not called in multiple
// threads, but Apply will be called concurrently with Persist. This means
// the FSM should be implemented in a fashion that allows for concurrent
// updates while a snapshot is happening.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.logger.Printf("[DEBUG] dqlite: fsm: start snapshot")

	backups := []*FSMDatabaseBackup{}

	for _, name := range f.connections.AllNames() {
		backup, err := f.backupDatabase(name)
		if err != nil {
			return nil, err
		}
		backups = append(backups, backup)
	}

	f.logger.Printf("[DEBUG] dqlite: fsm: snapshot completed")

	return &FSMSnapshot{
		backups: backups,
	}, nil
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state.
func (f *FSM) Restore(reader io.ReadCloser) error {
	f.logger.Printf("[DEBUG] dqlite: restore snapshot")

	for {
		done, err := f.restoreDatabase(reader)
		if err != nil {
			return err
		}
		if done {
			break
		}
	}

	return nil
}

// Backup a single database.
func (f *FSM) backupDatabase(name string) (*FSMDatabaseBackup, error) {
	f.logger.Printf("[DEBUG] dqlite: fsm: database backup for %s", name)

	// Figure out if there is an ongoing transaction any of the
	// database connections, if so we'll return an error.
	conns := f.connections.Leaders(name)
	conns = append(conns, f.connections.Follower(name))

	txid := ""
	for _, conn := range conns {
		if txn := f.transactions.GetByConn(conn); txn != nil {
			txn.Enter()
			state := txn.State()
			txn.Exit()
			if state != transaction.Started && state != transaction.Ended {
				return nil, fmt.Errorf("can't snapshot right now, transaction %s is in progress", txn)
			}
			f.logger.Printf("[DEBUG] dqlite: fsm: snapshot includes pending transaction %s", txn)
			txid = txn.ID()
		}
	}

	database, wal, err := f.connections.Backup(name)
	if err != nil {
		return nil, err
	}
	return &FSMDatabaseBackup{
		database: name,
		data:     database,
		wal:      wal,
		txid:     txid,
	}, nil
}

// Restore a single database. Returns true if there are more databases
// to restore, false otherwise.
func (f *FSM) restoreDatabase(reader io.ReadCloser) (bool, error) {

	done := false
	// Get size of database.
	var dataSize uint64
	if err := binary.Read(reader, binary.LittleEndian, &dataSize); err != nil {
		return false, errors.Wrap(err, "failed to read database size")
	}
	f.logger.Printf("[DEBUG] dqlite: snapshot database size %d", dataSize)

	// Now read in the database data.
	data := make([]byte, dataSize)
	if _, err := io.ReadFull(reader, data); err != nil {
		return false, errors.Wrap(err, "failed to read database data")
	}

	// Get size of wal
	var walSize uint64
	if err := binary.Read(reader, binary.LittleEndian, &walSize); err != nil {
		return false, errors.Wrap(err, "failed to read wal size")
	}
	f.logger.Printf("[DEBUG] dqlite: snapshot wal size %d", walSize)

	// Now read in the wal data.
	wal := make([]byte, walSize)
	if _, err := io.ReadFull(reader, wal); err != nil {
		return false, errors.Wrap(err, "failed to read wal data")
	}

	bufReader := bufio.NewReader(reader)
	name, err := bufReader.ReadString(0)
	if err != nil {
		return false, errors.Wrap(err, "failed to read database name")
	}
	name = name[:len(name)-1] // Strip the trailing 0

	conns := f.connections.Leaders(name)
	if len(conns) > 0 {
		panic(fmt.Sprintf("restore failure: database '%s' has %d leader connections", name, len(conns)))
	}

	txn := f.transactions.GetByConn(f.connections.Follower(name))
	if txn != nil {
		f.logger.Printf("[WARN] dqlite: fsm: closing follower in-flight transaction %s", txn)
		f.transactions.Remove(txn.ID())
	}

	if err := f.connections.CloseFollower(name); err != nil {
		return false, err
	}

	txid, err := bufReader.ReadString(0)
	f.logger.Printf("[DEBUG] dqlite: snapshot txid %s", txid)
	if err != nil {
		if err != io.EOF {
			return false, errors.Wrap(err, "failed to read database database")
		}
		done = true
	}

	dsn := f.connections.DSN(name)
	path := filepath.Join(f.connections.Dir(), dsn.Filename)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return false, err
	}
	if err := os.Remove(path + "-wal"); err != nil && !os.IsNotExist(err) {
		return false, err
	}
	if err := os.Remove(path + "-shm"); err != nil && !os.IsNotExist(err) {
		return false, err
	}

	if err := ioutil.WriteFile(path, data, 0600); err != nil {
		return false, errors.Wrap(err, "failed to restore database")
	}

	if err := ioutil.WriteFile(path+"-wal", wal, 0600); err != nil {
		return false, errors.Wrap(err, "failed to restore wal")
	}

	if err := f.connections.OpenFollower(name); err != nil {
		return false, err
	}

	if txid != "" {
		conn := f.connections.Follower(name)
		txn := f.transactions.AddFollower(conn, txid)
		txn.Enter()
		defer txn.Exit()
		if err := txn.Begin(); err != nil {
			return false, err
		}
	}

	return done, nil

}

// FSMDatabaseBackup holds backup information for a single database.
type FSMDatabaseBackup struct {
	database string
	data     []byte
	wal      []byte
	txid     string
}

// FSMSnapshot is returned by an FSM in response to a Snapshot
// It must be safe to invoke FSMSnapshot methods with concurrent
// calls to Apply.
type FSMSnapshot struct {
	backups []*FSMDatabaseBackup
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
func (s *FSMSnapshot) persistBackup(sink raft.SnapshotSink, backup *FSMDatabaseBackup) error {
	// Start by writing the size of the backup
	buffer := new(bytes.Buffer)
	dataSize := uint64(len(backup.data))
	if err := binary.Write(buffer, binary.LittleEndian, dataSize); err != nil {
		return errors.Wrap(err, "failed to encode data size")
	}
	if _, err := sink.Write(buffer.Bytes()); err != nil {
		return errors.Wrap(err, "failed to write data size to sink")
	}

	// Next write the data to the sink.
	if _, err := sink.Write(backup.data); err != nil {
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
	buffer.WriteString(backup.database)
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
