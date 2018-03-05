package dqlite

import (
	"github.com/CanonicalLtd/dqlite/internal/registry"
)

// Registry tracks internal data shared by the dqlite Driver and FSM.
type Registry registry.Registry

// NewRegistry creates a new Registry, which is expected to be passed to both
// NewFSM and NewDriver.
//
// The dir parameter is the directory where dqlite will store the underlying
// SQLite database files.
func NewRegistry(dir string) *Registry {
	return (*Registry)(registry.New(dir))
}
