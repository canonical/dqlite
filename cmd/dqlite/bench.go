package main

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/CanonicalLtd/dqlite"
	"github.com/CanonicalLtd/dqlite/internal/bindings"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
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
	store, err := dqlite.DefaultServerStore(":memory:")
	if err != nil {
		return errors.Wrap(err, "failed to create server store")
	}

	if err := store.Set(context.Background(), []string{address}); err != nil {
		return errors.Wrap(err, "failed to set server address")
	}

	driver, err := dqlite.NewDriver(store)
	if err != nil {
		return errors.Wrap(err, "failed to create dqlite driver")
	}

	sql.Register("dqlite", driver)

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	db, err := sql.Open("dqlite", "test.db")
	if err != nil {
		return errors.Wrap(err, "failed to open database")
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction")
	}

	start := time.Now()

	if _, err := tx.ExecContext(ctx, "CREATE TABLE test (n INT, t TEXT)"); err != nil {
		return errors.Wrapf(err, "failed to create test table")
	}

	if _, err := tx.ExecContext(ctx, "INSERT INTO test(n,t) VALUES(?, ?)", int64(123), "hello"); err != nil {
		return errors.Wrapf(err, "failed to insert test value")
	}

	rows, err := tx.QueryContext(ctx, "SELECT n FROM test")
	if err != nil {
		return errors.Wrapf(err, "failed to query test table")
	}

	for rows.Next() {
		var n int64
		if err := rows.Scan(&n); err != nil {
			return errors.Wrap(err, "failed to scan row")
		}
	}
	if err := rows.Err(); err != nil {
		return errors.Wrap(err, "result set failure")
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "failed to commit transaction")
	}

	fmt.Printf("time %s\n", time.Since(start))

	return nil
}
