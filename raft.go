package dqlite

import (
	//	"fmt"
	"time"

	"github.com/CanonicalLtd/dqlite/replication"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/pkg/errors"
	"path/filepath"
)

const (
	raftRetainSnapshotCount = 2
)

// Wrapper around NewRaft using our Config object and making
// opinionated choices for dqlite use.
func newRaft(config *Config, fsm *replication.FSM, peerStore raft.PeerStore, notifyCh chan bool) (*raft.Raft, error) {
	conf := &raft.Config{
		HeartbeatTimeout:           config.HeartbeatTimeout,
		ElectionTimeout:            config.ElectionTimeout,
		CommitTimeout:              config.CommitTimeout,
		MaxAppendEntries:           64,
		ShutdownOnRemove:           true,
		DisableBootstrapAfterElect: true,
		TrailingLogs:               256,
		SnapshotInterval:           500 * time.Millisecond,
		SnapshotThreshold:          64,
		EnableSingleNode:           config.EnableSingleNode,
		LeaderLeaseTimeout:         config.LeaderLeaseTimeout,
		NotifyCh:                   notifyCh,
		Logger:                     config.Logger,
	}
	store, err := raftboltdb.NewBoltStore(filepath.Join(config.Dir, "raft.db"))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create raft store")
	}
	snaps, err := raft.NewFileSnapshotStoreWithLogger(
		config.Dir, raftRetainSnapshotCount, config.Logger)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create snapshot store: %s")
	}
	raft, err := raft.NewRaft(
		conf, fsm, store, store, snaps, peerStore, config.Transport)
	if err != nil {
		return nil, errors.Wrap(err, "failed to start raft")
	}

	return raft, nil
}

// Check whether a node is currently a "lone" node, meaning that it didn't join
// any other nodes yet (i.e. peers store has no peers or that contains only our
// address).
func isLoneNode(peerStore raft.PeerStore, localAddr string) (bool, error) {
	peers, err := peerStore.Peers()
	if err != nil {
		return false, errors.Wrap(err, "failed to get current raft peers")
	}
	peersCount := len(peers)
	noPeers := peersCount == 0
	peersIsJustUs := peersCount == 1 && peers[0] == localAddr
	return noPeers || peersIsJustUs, nil
}
