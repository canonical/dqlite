package client

import (
	"context"
	"sync"

	"github.com/Rican7/retry"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Connector is in charge of creating a gRPC SQL client connected to the
// current leader of a gRPC SQL cluster and sending heartbeats to prevent
// connections created by that client from being killed by the server.
type Connector struct {
	id     uint64        // Client ID to use when registering against the server.
	store  ServerStore   // Used to get and update current cluster servers.
	config Config        // Connection parameters.
	logger *zap.Logger   // Logger.
	stopCh chan struct{} // Stop the connector on close.
	loopCh chan struct{} // Used to force a new connect loop iteraction.
	cond   *sync.Cond    // Block connect request until the conn field below is not nil.
	client *Client       // Healty client connected to a dqlite leader server.
}

// NewConnector creates a new connector that can be used by a gRPC SQL driver
// to create new clients connected to a leader gRPC SQL server.
func NewConnector(id uint64, store ServerStore, config Config, logger *zap.Logger) *Connector {
	connector := &Connector{
		id:     id,
		store:  store,
		config: config,
		logger: logger.With(zap.Namespace("connector")),
		stopCh: make(chan struct{}),
		loopCh: make(chan struct{}),
		cond:   sync.NewCond(&sync.Mutex{}),
	}
	go connector.connectLoop()
	return connector
}

// Stop the connector. This will eventually terminate any ongoing connection
// attempt or heartbeat process.
func (c *Connector) Stop() {
	select {
	case <-c.stopCh:
		return // Already stopped.
	default:
	}
	close(c.stopCh)
}

// Connect returns a dqlite client connected to a leader dqlite server.
//
// If no connection can be stablished within Config.ConnectTimeout or if the
// connector is stopped, an error is returned.
func (c *Connector) Connect(ctx context.Context) (*Client, error) {
	// Wait for a connection to the leader server to be available.
	client, err := c.getConn(ctx)
	if err != nil {
		return nil, err
	}

	return client, nil
}

// Connect loop is in charge of establishing a connection to the leader server,
// keep it alive by sending heartbeats, and switch to the next leader if the
// current leader fails.
func (c *Connector) connectLoop() {
	for {
		client := c.connect()
		if client == nil {
			// This means the connector was stopped.
			return
		}

		logger := c.logger.With(zap.String("address", client.Address()))
		logger.Info("starting heartbeat")

		// Make this channel buffered in case we abort this iteration
		// for a reason other than a heartbeat failure, and the
		// goroutine would be left running until the heartbeat
		// eventually errors out because we closed the network
		// connection. At that point we don't want the goroutine to be
		// stuck at sending to the channel.
		ch := make(chan error, 1)
		go func() {
			ch <- client.Heartbeat("id")
		}()

		// Heartbeat until a failure happens, or we are stopped, or the
		// loopCh triggers.
		select {
		case <-ch:
		case <-c.loopCh:
		case <-c.stopCh:
			c.setConn(nil)
			client.Close()
			return
		}

		// We lost connectivity to the server or we we were interrupted
		// by connect() because it detected that the server lost
		// leadership, let's connect again.
		c.setConn(nil)
		client.Close()
	}
}

// Connect finds the leader server and returns a connection to it.
//
// If the connector is stopped before a leader is found, nil is returned.
func (c *Connector) connect() *Client {
	var client *Client

	// The retry strategy should be configured to retry indefinitely, until
	// the connector is stopped.
	err := retry.Retry(func(attempt uint) error {
		// Regardless of whether we succeed or not, after each attempt
		// awake all waiting goroutines spawned by getServer(), to give
		// them a chance to terminate if their context is expired.
		defer func() {
			c.setConn(client)
		}()

		logger := c.logger.With(zap.Uint("attempt", attempt))

		select {
		case <-c.stopCh:
			// Stop retrying
			return nil
		default:
		}

		var err error
		client, err = c.connectAttemptAll(logger)
		if err != nil {
			logger.Info("connection failed", zap.String("err", err.Error()))
			return err
		}

		return nil
	}, c.config.RetryStrategies...)

	if err != nil {
		// The retry strategy should never give up until success or
		// connector shutdown.
		panic("connect retry aborted unexpectedly")
	}

	return client
}

// Make a single attempt to establish a connection to the leader server trying
// all addresses available in the store.
func (c *Connector) connectAttemptAll(logger *zap.Logger) (*Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.config.AttemptTimeout)
	defer cancel()

	addresses, err := c.store.Get(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get server addresses")
	}

	logger.Info("connecting to leader server", zap.Strings("addresses", addresses))

	// Make an attempt for each address until we find the leader.
	for _, address := range addresses {
		logger := logger.With(zap.String("address", address))

		ctx, cancel := context.WithTimeout(context.Background(), c.config.AttemptTimeout)
		defer cancel()

		conn, leader, err := c.connectAttemptOne(ctx, address)
		if err != nil {
			// This server is unavailable, try with the next target.
			logger.Info("server connection failed", zap.String("err", err.Error()))
			continue
		}
		if conn != nil {
			// We found the leader
			logger.Info("connected")
			return conn, nil
		}
		if leader == "" {
			// This server does not know who the current leader is,
			// try with the next target.
			continue
		}

		// If we get here, it means this server reported that another
		// server is the leader, let's close the connection to this
		// server and try with the suggested one.
		logger = logger.With(zap.String("leader", leader))
		conn, leader, err = c.connectAttemptOne(ctx, leader)
		if err != nil {
			// The leader reported by the previous server is
			// unavailable, try with the next target.
			logger.Info("leader server connection failed", zap.String("err", err.Error()))
			continue
		}
		if conn == nil {
			// The leader reported by the target server does not consider itself
			// the leader, try with the next target.
			logger.Info("reported leader server is not the leader")
			continue
		}
		logger.Info("connected")
		return conn, nil
	}

	return nil, errNoAvailableLeader
}

// Connect to the given dqlite server and check if it's the leader.
//
// Return values:
//
// - Any failure is hit:                     -> nil, "", err
// - Target not leader and no leader known:  -> nil, "", nil
// - Target not leader and leader known:     -> nil, leader, nil
// - Target is the leader:                   -> server, "", nil
//
func (c *Connector) connectAttemptOne(ctx context.Context, address string) (*Client, string, error) {
	client := newClient(address, c.store, c.logger)

	// Establish the connection.
	if err := client.connect(ctx); err != nil {
		return nil, "", errors.Wrap(err, "failed to connect to server")
	}

	// Send the initial Leader request.
	response, err := client.Leader(ctx)
	if err != nil {
		client.Close()
		return nil, "", errors.Wrap(err, "failed to send helo request")
	}

	leader := response.Address

	switch leader {
	case "":
		// Currently this server does not know about any leader.
		client.Close()
		return nil, "", nil
	case address:
		// This server is the leader
		return client, "", nil
	default:
		// This server claims to know who the current leader is.
		client.Close()
		return nil, leader, nil
	}
}

// Set the current leader server (or nil) and awake anything waiting on
// getServer.
func (c *Connector) setConn(client *Client) {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()
	c.client = client
	c.cond.Broadcast()
}

// Get a connection to the leader server. If the context is done before such a
// connection is available, return an errNoAvailableLeader.
func (c *Connector) getConn(ctx context.Context) (*Client, error) {
	// Make this channel buffered, so the goroutine below will not block on
	// sending to it if this method returns early, because the context was
	// done or the stopCh was cllosed.
	ch := make(chan *Client, 1)
	go func() {
		// Loop until we find a connected server which is still the
		// leader.
		for {
			client, err := c.waitConn(ctx)
			if err != nil {
				if err == errStaleLeader {
					continue
				}
				return
			}
			ch <- client
			return
		}
	}()

	select {
	case <-c.stopCh:
		return nil, errStop
	case <-ctx.Done():
		return nil, errNoAvailableLeader
	case server := <-ch:
		return server, nil
	}

}

// Wait for the connect loop to connect to a leader server.
func (c *Connector) waitConn(ctx context.Context) (*Client, error) {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()

	// As long as ctx has a deadline, this loop is guaranteed to finish
	// because the connect loop will call c.cond.Broadcast() after each
	// connection attempt (either successful or not), and a connection
	// attempts can't last more than c.config.AttemptTimeout times the
	// number of available targets in c.store.
	for c.client == nil {
		select {
		case <-ctx.Done():
			// Abort
			return nil, ctx.Err()
		default:
		}
		c.cond.Wait()
	}

	// Check that this server is actually still the leader. Since lost
	// leadership is detected via heartbeats (which by default happen every
	// 4 seconds), this connect attempt might be performed when the server
	// has actually lost leadership but the connect loop didn't notice it
	// yet. For example when the sql package tries to open a new driver
	// connection after the dqlite driver has returned ErrBadConn due to
	// lost leadership, we don't want to return a stale leader.

	// TODO: make sure follow-up multiple Helo's requests are fine for the
	// server, possibly rename Helo to something else, or introduce a
	// dedicated request (e.g. "Leader" and "Register").
	/*
		welcome, err := c.conn.Helo(ctx, c.id)
		leader := welcome.leader
		if err != errNotClustered && (err != nil || leader != c.conn.Address()) {
			// Force the connect loop to start a new iteration if
			// it's heartbeating.
			c.logger.Info("detected stale leader", zap.String("target", c.conn.Address()))
			c.conn = nil
			select {
			case c.loopCh <- struct{}{}:
			default:
			}
			return nil, errStaleLeader
		}
	*/

	return c.client, nil
}
