package replication

import (
	"github.com/CanonicalLtd/dqlite/internal/registry"
	"github.com/hashicorp/raft"
)

// Expose the internal registry.
func (m *Methods) Registry() *registry.Registry {
	return m.registry
}

// Expose the internal raft instance so tests can change the leadership state
// of the node.
func (m *Methods) Raft() *raft.Raft {
	return m.raft
}

// Don't check for leadership when entering a hook.
func (m *Methods) NoLeaderCheck() {
	m.noLeaderCheck = true
}
