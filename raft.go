package dqlite

import (
	"log"
	"time"

	"github.com/dqlite/dqlite/replication"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/pkg/errors"
	"path/filepath"
)

const (
	raftRetainSnapshotCount = 2
)

// Wrapper around NewRaft using our Config object and making
// opinionated choices for DQLite use. It also performs the
// initial cluster join if needed and wait for leadership.
func newRaft(config *Config, join string, fsm *replication.FSM, logger *log.Logger) (*raft.Raft, error) {
	peerStore := raft.NewJSONPeers(config.Dir, config.Transport)
	peers, err := peerStore.Peers()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get current raft peers")
	}
	peersCount := len(peers)

	// Enable single-node mode if there's no join address and there is
	// either no peer or just ourselves.
	noPeers := peersCount == 0
	peersIsJustUs := peersCount == 1 && peers[0] == config.Transport.LocalAddr()
	enableSingleNode := join == "" && (noPeers || peersIsJustUs)

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
		LeaderLeaseTimeout:         config.LeaderLeaseTimeout,
		EnableSingleNode:           enableSingleNode,
		Logger:                     logger,
	}
	store, err := raftboltdb.NewBoltStore(filepath.Join(config.Dir, "raft.db"))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create raft store")
	}
	snaps, err := raft.NewFileSnapshotStoreWithLogger(
		config.Dir, raftRetainSnapshotCount, logger)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create snapshot store: %s")
	}
	raft, err := raft.NewRaft(
		conf, fsm, store, store, snaps, peerStore, config.Transport)
	if err != nil {
		return nil, errors.Wrap(err, "failed to start raft")
	}

	// If a join address is given and there are no peers or peers
	// include just us, request to join the cluster via the given
	// address.
	if join != "" && (noPeers || peersIsJustUs) {
		timeout := config.SetupTimeout
		if err := config.MembershipChanger.Join(join, timeout); err != nil {
			raft.Shutdown().Error()
			return nil, errors.Wrap(err, "failed to join the cluster")
		}
	}

	return raft, nil
}
