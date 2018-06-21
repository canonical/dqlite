package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/CanonicalLtd/dqlite/internal/bindings"
	"github.com/CanonicalLtd/dqlite/internal/client"
	"github.com/Rican7/retry/backoff"
	"github.com/Rican7/retry/strategy"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// Return a new bench command.
func newBench() *cobra.Command {
	bench := &cobra.Command{
		Use:   "bench [address]",
		Short: "Bench all raft logs after the given index (included).",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			address := args[0]
			role := args[1]

			if role == "server" {
				return runServer(address)
			}

			return runClient(address)
		},
	}

	return bench
}

func runServer(address string) error {
	cluster := newTestCluster()

	server, err := bindings.NewServer(os.Stdout, cluster)
	if err != nil {
		return errors.Wrap(err, "failed to create server")
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return errors.Wrap(err, "failed to listen")
	}

	cluster.leader = listener.Addr().String()

	runCh := make(chan error)
	go func() {
		err := server.Run()
		runCh <- err
	}()

	if !server.Ready() {
		return fmt.Errorf("server not ready")
	}

	acceptCh := make(chan error)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			acceptCh <- nil
			return
		}
		err = server.Handle(conn)
		if err == bindings.ErrServerStopped {
			acceptCh <- nil
			return
		}

		//acceptCh <- err
	}()

	<-acceptCh

	return nil
}

type testCluster struct {
	leader string
}

func newTestCluster() *testCluster {
	return &testCluster{}
}

func (c *testCluster) Leader() string {
	return c.leader
}

func (c *testCluster) Servers() ([]string, error) {
	addresses := []string{
		"1.2.3.4:666",
		"5.6.7.8:666",
	}

	return addresses, nil
}

func (c *testCluster) Recover(token uint64) error {
	return nil
}

func runClient(address string) error {
	store := client.NewInmemServerStore()
	store.Set(context.Background(), []string{address})

	config := client.Config{
		AttemptTimeout: 100 * time.Millisecond,
		RetryStrategies: []strategy.Strategy{
			strategy.Backoff(backoff.BinaryExponential(time.Millisecond)),
		},
	}

	logger, err := zap.NewDevelopment()
	if err != nil {
		return errors.Wrapf(err, "failed to create logger")
	}

	connector := client.NewConnector(0, store, config, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	client, err := connector.Connect(ctx)
	if err != nil {
		return errors.Wrapf(err, "failed to connect")
	}

	db, err := client.Open(ctx, "test.db", "volatile")
	if err != nil {
		return errors.Wrapf(err, "failed to open db")
	}

	start := time.Now()

	_, err = client.ExecSQL(ctx, db.ID, "CREATE TABLE test (n INT)")
	if err != nil {
		return errors.Wrapf(err, "failed to exec")
	}

	_, err = client.ExecSQL(ctx, db.ID, "INSERT INTO test VALUES(1)")
	if err != nil {
		return errors.Wrapf(err, "failed to exec")
	}

	_, err = client.QuerySQL(ctx, db.ID, "SELECT n FROM test")
	if err != nil {
		return errors.Wrapf(err, "failed to query")
	}

	fmt.Printf("time %s\n", time.Since(start))

	return nil
}
