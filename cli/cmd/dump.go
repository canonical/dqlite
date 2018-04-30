package cmd

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/CanonicalLtd/dqlite/recover/dump"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// Return a new dump command.
func newDump() *cobra.Command {
	var tail int
	var replay string

	dump := &cobra.Command{
		Use:   "dump [path to raft data dir]",
		Short: "Dump or replay the content of a dqlite raft store.",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			dir := args[0]
			logs, snaps, err := dumpOpen(dir)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			options := make([]dump.Option, 0)

			if tail != 0 {
				options = append(options, dump.Tail(tail))
			}
			if replay != "" {
				options = append(options, dump.Replay(replay))
			}

			if err := dump.Dump(logs, snaps, os.Stdout, options...); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		},
	}

	flags := dump.Flags()
	flags.IntVarP(&tail, "tail", "t", 0, "limit the dump to the last N log commands")
	flags.StringVarP(&replay, "replay", "r", "", "replay the logs to the given database dir")

	return dump
}

func dumpOpen(dir string) (raft.LogStore, raft.SnapshotStore, error) {
	if _, err := os.Stat(dir); err != nil {
		return nil, nil, errors.Wrap(err, "invalid raft data dir")
	}

	logs, err := raftboltdb.NewBoltStore(filepath.Join(dir, "logs.db"))
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to open boltdb file")
	}

	snaps, err := raft.NewFileSnapshotStore(dir, 1, ioutil.Discard)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to open snapshot store")
	}

	return logs, snaps, nil
}
