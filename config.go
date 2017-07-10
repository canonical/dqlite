package dqlite

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/dqlite/raft-membership"
	"github.com/hashicorp/raft"

	"github.com/pkg/errors"
)

// Config holds configuration parameters for a DQLite node.
type Config struct {
	// Directory where databases will be stored.
	Dir string

	// Logger to use.
	Logger *log.Logger

	// The raft Transport to use.
	Transport raft.Transport

	// Request queue and processor for raft membership changes.
	MembershipRequests chan *raftmembership.ChangeRequest
	MembershipChanger  raftmembership.Changer

	// SetupTimeout is how long to wait before timing out
	// raft-related setup when creating a new Driver.
	SetupTimeout time.Duration

	// HeartbeatTimeout is a raft-specific setting and specifies
	// the time in follower state without a leader before we
	// attempt an election.
	HeartbeatTimeout time.Duration

	// ElectionTimeout is a raft-specific setting and specifies
	// the time in candidate state without a leader before we
	// attempt an election.
	ElectionTimeout time.Duration

	// CommitTimeout is a raft-specific setting and controls the
	// time without an Apply() operation before we heartbeat to
	// ensure a timely commit. Due to random staggering, may be
	// delayed as much as 2x this value.
	CommitTimeout time.Duration

	// LeaderLeaseTimeout is a raft-specific setting and it's used
	// to control how long the "lease" lasts for being the leader
	// without being able to contact a quorum of nodes. If we
	// reach this interval without contact, we will step down as
	// leader.
	LeaderLeaseTimeout time.Duration
}

// Ensure that the configured directory exists and is accessible.
func (c *Config) ensureDir() error {
	if c.Dir == "" {
		return fmt.Errorf("no data dir provided in config")
	}
	info, err := os.Stat(c.Dir)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(c.Dir, 0700); err != nil {
				return errors.Wrap(err, "failed to create data dir")
			}
			return nil
		}
		return errors.Wrap(err, "failed to access data dir")
	}
	if !info.IsDir() {
		return fmt.Errorf("data dir '%s' is not a directory", c.Dir)
	}
	return nil
}
