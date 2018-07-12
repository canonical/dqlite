package client

import (
	"context"
	"net"
)

// DialFunc is a function that can be used to establish a network connection.
type DialFunc func(context.Context, string) (net.Conn, error)

// TCPDial is a dial function using plain TCP to establish the network
// connection.
func TCPDial(ctx context.Context, address string) (net.Conn, error) {
	dialer := net.Dialer{}
	return dialer.DialContext(ctx, "tcp", address)
}
