package dqlite

import (
	"log"
	"net"
	"time"

	"github.com/CanonicalLtd/raft-http"
	"github.com/hashicorp/raft"
)

const (
	maxPool = 2 // Max number of connections that the network transport will pool
)

// NewHTTPConfig creates a new driver configuration setup to establish
// connections over HTTP.
func NewHTTPConfig(dir string, handler *rafthttp.Handler, endpoint string,
	addr net.Addr, logger *log.Logger) *Config {

	layer := rafthttp.NewLayer(endpoint, addr, handler, rafthttp.NewDialTCP())
	transport := raft.NewNetworkTransportWithLogger(
		layer, maxPool, 45*time.Second, logger)

	return &Config{
		Dir:                dir,
		Logger:             logger,
		Transport:          transport,
		MembershipRequests: layer.MembershipChangeRequests(),
		MembershipChanger:  layer,
		HeartbeatTimeout:   1000 * time.Millisecond,
		ElectionTimeout:    1000 * time.Millisecond,
		CommitTimeout:      50 * time.Millisecond,
		LeaderLeaseTimeout: 500 * time.Millisecond,
	}
}
