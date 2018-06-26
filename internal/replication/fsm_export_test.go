package replication

import (
	"github.com/CanonicalLtd/dqlite/internal/registry"
)

// Toggles whether the FSM should panic in case of errors, or just return them.
func (f *FSM) PanicOnFailure(flag bool) {
	f.panicOnFailure = flag
}

// Return the FSM's registry.
func (f *FSM) Registry() *registry.Registry {
	return f.registry
}

// Replace the FSM's registry.
func (f *FSM) RegistryReplace(registry *registry.Registry) {
	f.registry = registry
}
