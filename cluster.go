package dqlite

import (
	"time"

	"github.com/CanonicalLtd/dqlite/internal/bindings"
	"github.com/CanonicalLtd/dqlite/internal/registry"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

type cluster struct {
	replication string                     // Registration name for WAL replication
	raft        *raft.Raft                 // Raft instance
	registry    *registry.Registry         // Connection registry
	provider    raft.ServerAddressProvider // Custom address provider
}

func (c *cluster) Replication() string {
	return c.replication
}

func (c *cluster) Leader() string {
	return string(c.raft.Leader())
}

func (c *cluster) Servers() ([]string, error) {
	if c.raft.State() != raft.Leader {
		return nil, raft.ErrNotLeader
	}

	future := c.raft.GetConfiguration()

	if err := future.Error(); err != nil {
		return nil, errors.Wrap(err, "failed to get raft configuration")
	}

	configuration := future.Configuration()

	servers := configuration.Servers
	addresses := make([]string, len(servers))

	for i, server := range servers {
		if c.provider != nil {
			address, err := c.provider.ServerAddr(server.ID)
			if err != nil {
				return nil, errors.Wrap(err, "failed to fetch raft server address")
			}
			if address != "" {
				addresses[i] = string(address)
				continue
			}
		}
		addresses[i] = string(server.Address)
	}

	return addresses, nil
}

func (c *cluster) Register(conn *bindings.Conn) {
	filename := conn.Filename()
	c.registry.ConnLeaderAdd(filename, conn)
}

func (c *cluster) Unregister(conn *bindings.Conn) {
	c.registry.ConnLeaderDel(conn)
}

func (c *cluster) Barrier() error {
	if c.raft.State() != raft.Leader {
		return bindings.Error{
			Code:         bindings.ErrIoErr,
			ExtendedCode: bindings.ErrIoErrNotLeader,
		}
	}

	c.registry.Lock()
	index := c.registry.Index()
	c.registry.Unlock()
	if index == c.raft.LastIndex() {
		return nil
	}

	timeout := time.Minute // TODO: make this configurable
	if err := c.raft.Barrier(timeout).Error(); err != nil {
		if err == raft.ErrLeadershipLost {
			return bindings.Error{
				Code:         bindings.ErrIoErr,
				ExtendedCode: bindings.ErrIoErrNotLeader,
			}
		}

		// TODO: add an out-of-sync error to SQLite?
		return errors.Wrap(err, "FSM out of sync")
	}

	return nil
}

func (c *cluster) Recover(token uint64) error {
	return nil
}
