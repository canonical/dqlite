package dqlite

import (
	"fmt"
	"net"
	"os"
	"runtime"
	"time"

	"github.com/CanonicalLtd/dqlite/internal/bindings"
	"github.com/CanonicalLtd/dqlite/internal/replication"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Server implements the dqlite network protocol.
type Server struct {
	logger   *zap.Logger      // Logger
	registry *Registry        // Registry wrapper
	server   *bindings.Server // Low-level C implementation
	listener net.Listener     // Queue of new connections
	runCh    chan error       // Receives the low-level C server return code
	acceptCh chan error       // Receives connection handling errors
}

// ServerOption can be used to tweak server parameters.
type ServerOption func(*serverOptions)

// WithServerLogger sets a custom Server zap logger.
func WithServerLogger(logger *zap.Logger) ServerOption {
	return func(options *serverOptions) {
		options.Logger = logger
	}
}

// WithServerLogFile sets a custom dqlite C server log file.
func WithServerLogFile(file *os.File) ServerOption {
	return func(options *serverOptions) {
		options.LogFile = file
	}
}

// WithServerAddressProvider sets a custom resolver for server addresses.
func WithServerAddressProvider(provider raft.ServerAddressProvider) ServerOption {
	return func(options *serverOptions) {
		options.AddressProvider = provider
	}
}

// NewServer creates a new Server instance.
func NewServer(raft *raft.Raft, registry *Registry, listener net.Listener, options ...ServerOption) (*Server, error) {
	o := defaultServerOptions()

	for _, option := range options {
		option(o)
	}

	methods := replication.NewMethods(registry.registry, raft)

	if replication := bindings.FindWalReplication(registry.name); replication != nil {
		return nil, fmt.Errorf("replication name already in use")
	}

	if err := bindings.RegisterWalReplication(registry.name, methods); err != nil {
		return nil, errors.Wrap(err, "failed to register WAL replication")
	}

	cluster := &cluster{
		replication: registry.name,
		raft:        raft,
		registry:    registry.registry,
		provider:    o.AddressProvider,
	}

	server, err := bindings.NewServer(o.LogFile, cluster)
	if err != nil {
		return nil, err
	}

	s := &Server{
		logger:   o.Logger,
		registry: registry,
		server:   server,
		listener: listener,
		runCh:    make(chan error),
		acceptCh: make(chan error, 1),
	}

	go s.run()

	if !s.server.Ready() {
		return nil, fmt.Errorf("server failed to start")
	}

	go s.acceptLoop()

	return s, nil
}

// Hold configuration options for a dqlite server.
type serverOptions struct {
	Logger          *zap.Logger
	LogFile         *os.File
	AddressProvider raft.ServerAddressProvider
}

// Run the server.
func (s *Server) run() {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	s.runCh <- s.server.Run()
}

func (s *Server) acceptLoop() {
	s.logger.Debug("accepting connections")

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			s.acceptCh <- nil
			return
		}

		err = s.server.Handle(conn)
		if err != nil {
			if err == bindings.ErrServerStopped {
				// Ignore failures due to the server being
				// stopped.
				err = nil
			}
			s.acceptCh <- err
			return
		}
	}
}

// Close the server, releasing all resources it created.
func (s *Server) Close() error {
	// Close the listener, which will make the listener.Accept() call in
	// acceptLoop() return an error.
	if err := s.listener.Close(); err != nil {
		return err
	}

	// Wait for the acceptLoop goroutine to exit.
	select {
	case err := <-s.acceptCh:
		if err != nil {
			return errors.Wrap(err, "accept goroutine failed")
		}
	case <-time.After(time.Second):
		return fmt.Errorf("accept goroutine did not stop within a second")
	}

	// Send a stop signal to the dqlite event loop.
	if err := s.server.Stop(); err != nil {
		return errors.Wrap(err, "server failed to stop")
	}

	// Wait for the run goroutine to exit.
	select {
	case err := <-s.runCh:
		if err != nil {
			return errors.Wrap(err, "accept goroutine failed")
		}
	case <-time.After(time.Second):
		return fmt.Errorf("server did not stop within a second")
	}

	s.server.Close()
	s.server.Free()

	bindings.UnregisterWalReplication(s.registry.name)
	s.registry.Close()

	return nil
}

// Create a serverOptions object with sane defaults.
func defaultServerOptions() *serverOptions {
	return &serverOptions{
		Logger:          defaultLogger(),
		LogFile:         os.Stdout,
		AddressProvider: nil,
	}
}
