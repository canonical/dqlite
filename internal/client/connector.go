package client

import (
	"context"
	"encoding/binary"
	"io"
	"net"

	"github.com/CanonicalLtd/dqlite/internal/bindings"
	"github.com/Rican7/retry"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Connector is in charge of creating a gRPC SQL client connected to the
// current leader of a gRPC SQL cluster and sending heartbeats to prevent
// connections created by that client from being killed by the server.
type Connector struct {
	id       uint64      // Client ID to use when registering against the server.
	store    ServerStore // Used to get and update current cluster servers.
	config   Config      // Connection parameters.
	logger   *zap.Logger // Logger.
	protocol []byte      // Protocol version
}

// NewConnector creates a new connector that can be used by a gRPC SQL driver
// to create new clients connected to a leader gRPC SQL server.
func NewConnector(id uint64, store ServerStore, config Config, logger *zap.Logger) *Connector {
	connector := &Connector{
		id:       id,
		store:    store,
		config:   config,
		logger:   logger.With(zap.Namespace("connector")),
		protocol: make([]byte, 8),
	}

	binary.LittleEndian.PutUint64(
		connector.protocol,
		bindings.ProtocolVersion,
	)

	return connector
}

// Connect finds the leader server and returns a connection to it.
//
// If the connector is stopped before a leader is found, nil is returned.
func (c *Connector) Connect(ctx context.Context) (*Client, error) {
	var client *Client

	// The retry strategy should be configured to retry indefinitely, until
	// the given context is done.
	err := retry.Retry(func(attempt uint) error {
		logger := c.logger.With(zap.Uint("attempt", attempt))

		select {
		case <-ctx.Done():
			// Stop retrying
			return nil
		default:
		}

		var err error
		client, err = c.connectAttemptAll(ctx, logger)
		if err != nil {
			logger.Info("connection failed", zap.String("err", err.Error()))
			return err
		}

		return nil
	}, c.config.RetryStrategies...)

	if err != nil {
		// The retry strategy should never give up until success or
		// context expiration.
		panic("connect retry aborted unexpectedly")
	}

	if ctx.Err() != nil {
		return nil, errNoAvailableLeader
	}

	return client, nil
}

// Make a single attempt to establish a connection to the leader server trying
// all addresses available in the store.
func (c *Connector) connectAttemptAll(ctx context.Context, logger *zap.Logger) (*Client, error) {
	addresses, err := c.store.Get(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get server addresses")
	}

	logger.Info("connecting to leader server", zap.Strings("addresses", addresses))

	// Make an attempt for each address until we find the leader.
	for _, address := range addresses {
		logger := logger.With(zap.String("address", address))

		ctx, cancel := context.WithTimeout(ctx, c.config.AttemptTimeout)
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
	// Establish the connection.
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, "", errors.Wrap(err, "failed to establish network connection")
	}

	// Perform the protocol handshake.
	n, err := conn.Write(c.protocol)
	if err != nil {
		conn.Close()
		return nil, "", errors.Wrap(err, "failed to send handshake")
	}
	if n != 8 {
		conn.Close()
		return nil, "", errors.Wrap(io.ErrShortWrite, "failed to send handshake")
	}

	client := newClient(conn, address, c.store, c.logger)

	// Send the initial Leader request.
	request := Message{}
	request.Init(8)
	response := Message{}
	response.Init(512)

	EncodeLeader(&request)

	if err := client.Call(ctx, &request, &response); err != nil {
		client.Close()
		return nil, "", errors.Wrap(err, "failed to send Leader request")
	}

	leader, err := DecodeServer(&response)
	if err != nil {
		client.Close()
		return nil, "", errors.Wrap(err, "failed to send Leader request")
	}

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
