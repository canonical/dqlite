package dqlite

import (
	"fmt"

	"github.com/CanonicalLtd/dqlite/internal/bindings"
	"github.com/CanonicalLtd/dqlite/internal/registry"
)

// Registry tracks internal data shared by the dqlite Driver and FSM.
type Registry struct {
	name     string
	vfs      *bindings.Vfs
	registry *registry.Registry
}

// NewRegistry creates a new Registry, which is expected to be passed to both
// NewFSM and NewDriver.
//
// The ID parameter is a string identifying the local node.
func NewRegistry(id string) *Registry {
	name := fmt.Sprintf("dqlite-%s", id)
	vfs, err := bindings.RegisterVfs(name)
	if err != nil {
		panic("failed to register VFS")
	}

	return &Registry{
		name:     name,
		vfs:      vfs,
		registry: registry.New(vfs),
	}
}

// Close the registry.
func (r *Registry) Close() {
	bindings.UnregisterVfs(r.vfs)
}
