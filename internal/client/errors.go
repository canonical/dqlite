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
	errMessageEOF        = fmt.Errorf("message eof")
)

// ErrRequest is returned in case of request failure.
type ErrRequest struct {
	Code        uint64
	Description string
}

func (e ErrRequest) Error() string {
	return fmt.Sprintf("%s (%d)", e.Description, e.Code)
}
