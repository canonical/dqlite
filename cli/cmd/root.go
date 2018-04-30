package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// Execute is the main entry point for the dqlite command line.
func Execute() {
	root := newRoot()
	if err := root.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// Return a new root command.
func newRoot() *cobra.Command {
	root := &cobra.Command{
		Use:   "dqlite",
		Short: "Distributed SQLite for Go applications",
		Long: `Replicate a SQLite database across a cluster, using the Raft algorithm.

Complete documentation is available at https://github.com/CanonicalLtd/dqlite`,
		Run: func(cmd *cobra.Command, args []string) {
			// Do Stuff Here
		},
	}
	root.AddCommand(newDump())

	return root
}
