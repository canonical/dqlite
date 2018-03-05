package transaction

import (
	"fmt"

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
	isZombie bool                // Whether this is a zombie transaction, see Zombie().
	dryRun   bool                // Dry run mode, don't invoke actual SQLite hooks, for testing.
	state    *txnState           // Current state of our internal fsm.
	machine  *fsm.Machine        // Internal fsm.
}

// Possible transaction states. Most states are associated with SQLite
// replication hooks that are invoked upon transitioning from one lifecycle
// state to the next.
const (
	Pending = fsm.State("pending") // Initial state right after creation.
	Writing = fsm.State("writing") // After the frames hook has been executed.
	Written = fsm.State("written") // After a final frames hook has been executed.
	Undone  = fsm.State("undone")  // After the undo hook has been executed.
	Doomed  = fsm.State("doomed")  // The transaction has errored.
)

// New creates a new Txn instance.
func New(conn *sqlite3.SQLiteConn, id uint64, isLeader bool, dryRun bool) *Txn {
	state := &txnState{state: Pending}

	// Initialize our internal FSM. Rules for state transitions are
	// slightly different for leader and follower transactions, since only
	// the former can be stale.
	machine := &fsm.Machine{
		Subject: state,
		Rules:   newTxnStateRules(),
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
	if t.IsLeader() {
		s += "leader"
		if t.IsZombie() {
			s += " (zombie)"
		}
	} else {
		s += "follower"
	}
	return s
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
	return t.state.CurrentState()
}

// IsLeader returns true if the underlying connection is in leader
// replication mode.
func (t *Txn) IsLeader() bool {
	// This flag is set by the registry at creation time. See Registry.add().
	return t.isLeader
}

// Frames writes frames to the WAL.
func (t *Txn) Frames(begin bool, frames *sqlite3.ReplicationFramesParams) error {
	state := Writing
	if frames.IsCommit > 0 {
		state = Written
	}
	return t.transition(state, begin, frames)
}

// Undo reverts all changes to the WAL since the start of the
// transaction.
func (t *Txn) Undo() error {
	return t.transition(Undone)
}

// Zombie marks this transaction as zombie. It must be called only for leader
// transactions.
//
// A zombie transaction is one whose leader has lost leadership while applying
// the associated FSM command. The transaction is left in state passed as
// argument.
func (t *Txn) Zombie(state fsm.State) {
	if !t.isLeader {
		panic("non-leader transactions can't be marked as zombie")
	}
	if t.isZombie {
		panic("transaction is already marked as zombie")
	}
	t.machine.Subject.SetState(state)
	t.isZombie = true
}

// Resurrect a zombie transaction, to be re-used after a leader that lost
// leadership was re-elected right away.
func (t *Txn) Resurrect(state fsm.State) {
	if !t.isLeader {
		panic("non-leader transactions can't be marked as zombie")
	}
	if !t.isZombie {
		panic("transaction is not marked as zombie")
	}
	t.machine.Subject.SetState(state)
	t.isZombie = false
}

// IsZombie returns true if this is a zombie transaction.
func (t *Txn) IsZombie() bool {
	if !t.isLeader {
		panic("non-leader transactions can't be zombie")
	}
	return t.isZombie
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
		// In dry run mode, don't actually invoke any SQLite API.
		return nil
	}

	if t.isLeader {
		// In leader mode, don't actually invoke SQLite replication
		// API, since that will be done by SQLite internally.
		return nil
	}

	var err error
	switch state {
	case Writing:
	case Written:
		begin := args[0].(bool)
		frames := args[1].(*sqlite3.ReplicationFramesParams)
		err = sqlite3.ReplicationFrames(t.conn, begin, frames)
	case Undone:
		err = sqlite3.ReplicationUndo(t.conn)
	}

	if err != nil {
		if err := t.machine.Transition(Doomed); err != nil {
			panic(fmt.Sprintf("cannot doom from %s", t.state.CurrentState()))
		}
	}

	return err
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
func newTxnStateRules() *fsm.Ruleset {
	rules := &fsm.Ruleset{}

	for o, states := range transitions {
		for _, e := range states {
			rules.AddTransition(fsm.T{O: o, E: e})
		}
	}

	return rules
}

// Map of all valid state transitions.
var transitions = map[fsm.State][]fsm.State{
	Pending: {Writing, Written, Undone},
	Writing: {Written, Undone, Doomed},
	Written: {Doomed},
	Undone:  {Doomed},
}
