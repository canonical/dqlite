package transaction

import (
	"github.com/ryanfaerman/fsm"
)

// Create a new FSM initialized with a fresh state object set to Pending.
func newMachine() fsm.Machine {
	return fsm.New(
		fsm.WithRules(newRules()),
		fsm.WithSubject(newState()),
	)
}

// Capture valid state transitions within a transaction.
func newRules() fsm.Ruleset {
	rules := fsm.Ruleset{}

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
	Writing: {Writing, Written, Undone, Doomed},
	Written: {Doomed},
	Undone:  {Doomed},
}

// Track the state of transaction. Implements the fsm.Stater interface.
type state struct {
	state fsm.State
}

// Return a new transaction state object, set to Pending.
func newState() *state {
	return &state{
		state: Pending,
	}
}

// CurrentState returns the current state, implementing fsm.Stater.
func (s *state) CurrentState() fsm.State {
	return s.state
}

// SetState switches the current state, implementing fsm.Stater.
func (s *state) SetState(state fsm.State) {
	s.state = state
}
