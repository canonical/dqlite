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

// Don't check for leadership this amount of times when entering a hook.
func (m *Methods) NoLeaderCheck(n int) {
	m.noLeaderCheck = n
}
