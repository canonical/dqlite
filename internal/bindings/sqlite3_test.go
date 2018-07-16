package bindings_test

import (
	"testing"

	"github.com/CanonicalLtd/dqlite/internal/bindings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegisterWalReplication(t *testing.T) {
	replication := &testWalReplication{}

	err := bindings.RegisterWalReplication("test", replication)
	require.NoError(t, err)

	assert.Equal(t, replication, bindings.FindWalReplication("test"))

	err = bindings.UnregisterWalReplication("test")
	require.NoError(t, err)
}

type testWalReplication struct {
}

func (r *testWalReplication) Begin(*bindings.Conn) int {
	return 0
}

func (r *testWalReplication) Abort(*bindings.Conn) int {
	return 0
}

func (r *testWalReplication) Frames(*bindings.Conn, bindings.WalReplicationFrameList) int {
	return 0
}

func (r *testWalReplication) Undo(*bindings.Conn) int {
	return 0
}

func (r *testWalReplication) End(*bindings.Conn) int {
	return 0
}
