package dqlite

import (
	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/CanonicalLtd/dqlite/internal/log"
	"github.com/CanonicalLtd/dqlite/internal/replication"
	"github.com/CanonicalLtd/dqlite/internal/transaction"
	"github.com/hashicorp/raft"
)

// NewFSM creates a new dqlite FSM, suitable to be passed to raft.NewRaft.
//
// It will handle replication of the SQLite write-ahead log.
//
// This is mostly an internal implementation detail of dqlite, but it needs to
// be exposed since the raft.Raft parameter that NewDriver accepts doesn't
// allow access to the FSM that it was passed.
func NewFSM(dir string) raft.FSM {
	logger := log.New(log.Standard(), log.Info)
	connections := connection.NewRegistry()
	transactions := transaction.NewRegistry()
	return replication.NewFSM(logger, dir, connections, transactions)
}
