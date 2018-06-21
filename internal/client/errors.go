package client

import (
	"fmt"
)

var (
	errNoAvailableLeader = fmt.Errorf("no available dqlite leader server found")
	errStop              = fmt.Errorf("connector was stopped")
	errStaleLeader       = fmt.Errorf("server has lost leadership")
	errNotClustered      = fmt.Errorf("server is not clustered")
	errNegativeRead      = fmt.Errorf("reader returned negative count from Read")
)
