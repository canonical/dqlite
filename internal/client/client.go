package client

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/CanonicalLtd/dqlite/internal/bindings"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Client connecting to a dqlite server and speaking the dqlite wire protocol.
type Client struct {
	logger  *zap.Logger   // Logger.
	address string        // Address of the connected dqlite server.
	store   ServerStore   // Update this store upon heartbeats.
	conn    net.Conn      // Underlying network connection.
	timeout time.Duration // Heartbeat timeout reported at registration.
}

func newClient(address string, store ServerStore, logger *zap.Logger) *Client {
	return &Client{
		logger:  logger.With(zap.String("target", address)),
		address: address,
		store:   store,
	}
}

// Establish a TCP network connection and send the initial handshake.
func (c *Client) connect(ctx context.Context) error {
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", c.address)
	if err != nil {
		return errors.Wrap(err, "failed to establish network connection")
	}

	// Perform the protocol handshake.
	buf := buffer{Bytes: make([]byte, 8)}
	buf.WriteUint64(bindings.ServerProtocolVersion)

	// TODO: we should keep on with short writes
	n, err := conn.Write(buf.Bytes)
	if err != nil {
		conn.Close()
		return errors.Wrap(err, "failed to send handshake")
	}
	if n != 8 {
		conn.Close()
		return errors.Wrap(io.ErrShortWrite, "failed to send handshake")
	}

	c.conn = conn

	return nil
}

func (c *Client) Close() {
	c.conn.Close()
}

// Return the address used to create the client.
func (c *Client) Address() string {
	return c.address
}

// Leader sends a Leader request and reads the response.
func (c *Client) Leader(ctx context.Context) (response Server, err error) {
	leader := Leader{}

	if err = leader.Write(c.conn); err != nil {
		return
	}

	if err = response.Read(c.conn); err != nil {
		return
	}

	return
}

func (c *Client) Heartbeat(id string) error {
	ch := make(chan int)
	<-ch
	return nil
}

// Open sends a Open request and reads the response.
func (c *Client) Open(ctx context.Context, name string, vfs string) (response Db, err error) {
	open := Open{}
	open.name = name
	open.flags = bindings.DbOpenReadWrite | bindings.DbOpenCreate
	open.vfs = vfs

	if err = open.Write(c.conn); err != nil {
		return
	}

	err = response.Read(c.conn)

	return
}

// Prepare sends a Prepare request and reads the response.
func (c *Client) Prepare(ctx context.Context, db uint32, sql string) (response Stmt, err error) {
	prepare := Prepare{}
	prepare.db = uint64(db)
	prepare.sql = sql

	if err = prepare.Write(c.conn); err != nil {
		return
	}

	err = response.Read(c.conn)

	return
}

// Exec sends a Exec request and reads the response.
func (c *Client) Exec(ctx context.Context, db uint32, stmt uint32) (response Result, err error) {
	exec := Exec{}
	exec.db = db
	exec.stmt = stmt

	if err = exec.Write(c.conn); err != nil {
		return
	}

	err = response.Read(c.conn)

	return
}

// Query sends a Query request and reads the response.
func (c *Client) Query(ctx context.Context, db uint32, stmt uint32) (response Rows, err error) {
	query := Query{}
	query.db = db
	query.stmt = stmt

	if err = query.Write(c.conn); err != nil {
		return
	}

	err = response.Read(c.conn)

	return
}

// Finalize sends a Finalize request and reads the response.
func (c *Client) Finalize(ctx context.Context, db uint32, stmt uint32) (response Empty, err error) {
	finalize := Finalize{}
	finalize.db = db
	finalize.stmt = stmt

	if err = finalize.Write(c.conn); err != nil {
		return
	}

	err = response.Read(c.conn)

	return
}

// ExecSQL sends a ExecSQL request and reads the response.
func (c *Client) ExecSQL(ctx context.Context, db uint32, sql string) (response Result, err error) {
	execSQL := ExecSQL{}
	execSQL.db = uint64(db)
	execSQL.sql = sql

	if err = execSQL.Write(c.conn); err != nil {
		return
	}

	err = response.Read(c.conn)

	return
}

// QuerySQL sends a QuerySQL request and reads the response.
func (c *Client) QuerySQL(ctx context.Context, db uint32, sql string) (response Rows, err error) {
	querySQL := QuerySQL{}
	querySQL.db = uint64(db)
	querySQL.sql = sql

	if err = querySQL.Write(c.conn); err != nil {
		return
	}

	err = response.Read(c.conn)

	return
}
