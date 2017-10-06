package dqlite

import (
	"fmt"

	"github.com/hashicorp/raft"
)

// RaftFactory is the interface of a function that creates a raft instance
// attached to the given fsm.
type RaftFactory func(raft.FSM) (*raft.Raft, error)

// RaftLoneNode is a convenience for checking if a raft node is a "lone" one,
// meaning that it has no other peers yet.
func RaftLoneNode(configuration raft.Configuration, localAddr string) (bool, error) {
	servers := configuration.Servers
	switch len(servers) {
	case 0:
		return true, nil
	case 1:
		if servers[0].Address != raft.ServerAddress(localAddr) {
			return false, fmt.Errorf("configuration has unexpected address")
		}
		return true, nil
	default:
		return false, nil
	}
}
