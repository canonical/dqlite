package dqlite

import (
	"fmt"

	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

// RaftFactory is the interface of a function that creates a raft instance
// attached to the given fsm.
type RaftFactory func(raft.FSM) (*raft.Raft, error)

// RaftLoneNode is a convenience for checking if a raft node is a "lone" one,
// meaning that it has no other peers yet.
func RaftLoneNode(peerStore raft.PeerStore, localAddr string) (bool, error) {
	peers, err := peerStore.Peers()
	if err != nil {
		return false, errors.Wrap(err, "failed to get current raft peers")
	}
	switch len(peers) {
	case 0:
		return true, nil
	case 1:
		if peers[0] != localAddr {
			return false, fmt.Errorf("peer store has unexpected address")
		}
		return true, nil
	default:
		return false, nil
	}
}
