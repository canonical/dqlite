package transaction

import (
	"fmt"
	"sync"

	"github.com/CanonicalLtd/go-sqlite3"
	"github.com/ryanfaerman/fsm"
)

// Txn captures information about an active WAL write transaction
// that has been started on a SQLite connection configured to be in
// either leader or replication mode.
type Txn struct {
	conn     *sqlite3.SQLiteConn // Underlying SQLite connection.
	id       uint64              // Transaction ID.
	isLeader bool                // Whether we're attached to a connection in leader mode.
	dryRun   bool                // Dry run mode, don't invoke actual SQLite hooks, for testing.
	mu       sync.Mutex          // Serialize access to internal state.
	entered  bool                // Whether the mutex is currently locked.
	state    *txnState           // Current state of our internal fsm.
	machine  *fsm.Machine        // Internal fsm.
}

// Possible transaction states. Most states are associated with SQLite
// replication hooks that are invoked upon transitioning from one lifecycle
// state to the next.
const (
	Pending = fsm.State("pending") // Initial state right after creation.
	Started = fsm.State("started") // After the begin hook has been executed.
	Writing = fsm.State("writing") // After the wal frames hook has been executed.
	Undoing = fsm.State("undoing") // After the undo hook has been executed.
	Ended   = fsm.State("ended")   // After the end hook has been executed.
	Stale   = fsm.State("stale")   // The transaction is stale (leader only).
	Doomed  = fsm.State("doomed")  // The transaction has errored.
)

func newTxn(conn *sqlite3.SQLiteConn, id uint64, isLeader bool, dryRun bool) *Txn {
	state := &txnState{state: Pending}

	// Initialize our internal FSM. Rules for state transitions are
	// slightly different for leader and follower transactions, since only
	// the former can be stale.
	machine := &fsm.Machine{
		Subject: state,
		Rules:   newTxnStateRules(isLeader),
	}

	txn := &Txn{
		conn:     conn,
		id:       id,
		isLeader: isLeader,
		dryRun:   dryRun,
		state:    state,
		machine:  machine,
	}

	return txn
}

func (t *Txn) String() string {
	s := fmt.Sprintf("%d %s as ", t.id, t.state.CurrentState())
	if t.isLeader {
		s += "leader"
	} else {
		s += "follower"
	}
	return s
}

// Enter starts a critical section accessing or modifying this
// transaction instance.
func (t *Txn) Enter() {
	t.mu.Lock()
	t.entered = true
}

// Exit ends a critical section accessing or modifying this
// transaction instance.
func (t *Txn) Exit() {
	t.entered = false
	t.mu.Unlock()
}

// Do is a convenience around Enter/Exit executing the given function within
// lock boundaries.
func (t *Txn) Do(f func() error) error {
	t.Enter()
	defer t.Exit()
	return f()
}

// Conn returns the sqlite connection that started this write
// transaction.
func (t *Txn) Conn() *sqlite3.SQLiteConn {
	return t.conn
}

// ID returns the ID associated with this transaction.
func (t *Txn) ID() uint64 {
	return t.id
}

// State returns the current state of the transition.
func (t *Txn) State() fsm.State {
	t.checkEntered()
	return t.state.CurrentState()
}

// IsLeader returns true if the underlying connection is in leader
// replication mode.
func (t *Txn) IsLeader() bool {
	// This flag is set by the registry at creation time. See Registry.add().
	return t.isLeader
}

// Begin the WAL write transaction.
func (t *Txn) Begin() error {
	t.checkEntered()
	return t.transition(Started)
}

// WalFrames writes frames to the WAL.
func (t *Txn) WalFrames(frames *sqlite3.ReplicationWalFramesParams) error {
	t.checkEntered()
	return t.transition(Writing, frames)
}

// Undo reverts all changes to the WAL since the start of the
// transaction.
func (t *Txn) Undo() error {
	t.checkEntered()
	return t.transition(Undoing)
}

// End completes the transaction.
func (t *Txn) End() error {
	t.checkEntered()
	return t.transition(Ended)
}

// Stale marks this transaction as stale. It must be called only for
// leader transactions.
func (t *Txn) Stale() error {
	t.checkEntered()
	if t.State() == Started || t.State() == Writing {
		if err := t.transition(Undoing); err != nil {
			return err
		}
	}
	if t.State() == Undoing {
		if err := t.transition(Ended); err != nil {
			return err
		}
	}
	return t.transition(Stale)
}

// IsStale returns true if the underlying connection is the Stale state.
func (t *Txn) IsStale() bool {
	// This flag is set by the registry at creation time. See Registry.add().
	t.Enter()
	defer t.Exit()
	return t.State() == Stale
}

// DryRun makes this transaction only transition between states, without
// actually invoking the relevant SQLite APIs. This should only be
// used by tests.
func (t *Txn) DryRun(v bool) {
	t.dryRun = v
}

// Try to transition to the given state. If the transition is invalid,
// panic out.
func (t *Txn) transition(state fsm.State, args ...interface{}) error {
	if err := t.machine.Transition(state); err != nil {
		panic(fmt.Sprintf(
			"invalid %s -> %s transition", t.state.CurrentState(), state))
	}

	if t.dryRun {
		// In dry run mode, don't actually invoke the sqlite APIs.
		return nil
	}

	var err error

	switch state {
	case Started:
		err = sqlite3.ReplicationBegin(t.conn)
	case Writing:
		frames := args[0].(*sqlite3.ReplicationWalFramesParams)
		err = sqlite3.ReplicationWalFrames(t.conn, frames)
	case Undoing:
		err = sqlite3.ReplicationUndo(t.conn)
	case Ended:
		err = sqlite3.ReplicationEnd(t.conn)
	case Stale:
	}

	if err != nil {
		if err := t.machine.Transition(Doomed); err != nil {
			panic(fmt.Sprintf("cannot doom from %s", t.state.CurrentState()))
		}
	}

	return err
}

// Assert that we have actually entered a cricial section via mutex
// locking.
func (t *Txn) checkEntered() {
	if !t.entered {
		panic(fmt.Sprintf("accessing or modifying txn state without mutex: %d", t.id))
	}
}

type txnState struct {
	state fsm.State
}

// CurrentState returns the current state, implementing fsm.Stater.
func (s *txnState) CurrentState() fsm.State {
	return s.state
}

// SetState switches the current state, implementing fsm.Stater.
func (s *txnState) SetState(state fsm.State) {
	s.state = state
}

// Capture valid state transitions within a transaction.
func newTxnStateRules(forLeader bool) *fsm.Ruleset {
	rules := &fsm.Ruleset{}

	// Add all rules common to both leader and follower transactions.
	for o, states := range transitions {
		for _, e := range states {
			rules.AddTransition(fsm.T{O: o, E: e})
		}
	}
	for _, o := range []fsm.State{Started, Writing, Undoing, Ended} {
		rules.AddTransition(fsm.T{O: o, E: Doomed})
	}

	// Add rules valid only for leader transactions.
	if forLeader {
		for _, state := range []fsm.State{Pending, Ended} {
			rules.AddTransition(fsm.T{
				O: state,
				E: Stale,
			})
		}
	}

	return rules
}

// Map of all valid state transitions.
var transitions = map[fsm.State][]fsm.State{
	Pending: {Started},
	Started: {Started, Writing, Undoing, Ended},
	Writing: {Writing, Undoing, Ended},
	Undoing: {Undoing, Ended},
}
