package client

import (
	"context"
)

// ServerStore is used by a dqlite client to get an initial list of candidate
// dqlite server addresses that it can dial in order to find a leader dqlite
// server to use.
//
// Once connected, the client periodically updates the addresses in the store
// by querying the leader server about changes in the cluster (such as servers
// being added or removed).
type ServerStore interface {
	// Get return the list of known gRPC SQL server addresses.
	//
	// Each address name must follow the sysntax defined by the gRPC dial
	// protocol.
	//
	// See https://github.com/grpc/grpc/blob/master/doc/naming.md.
	Get(context.Context) ([]string, error)

	// Set updates the list of known gRPC SQL server addresses.
	Set(context.Context, []string) error
}

// InmemServerStore keeps the list of target gRPC SQL servers in memory.
type InmemServerStore struct {
	addresses []string
}

// NewInmemServerStore creates ServerStore which stores its data in-memory.
func NewInmemServerStore() *InmemServerStore {
	return &InmemServerStore{
		addresses: make([]string, 0),
	}
}

// Get the current targets.
func (i *InmemServerStore) Get(ctx context.Context) ([]string, error) {
	return i.addresses, nil
}

// Set the targets.
func (i *InmemServerStore) Set(ctx context.Context, addresses []string) error {
	i.addresses = addresses
	return nil
}
