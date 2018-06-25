package dqlite_test

import (
	"context"
	"testing"

	"github.com/CanonicalLtd/dqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Exercise setting and getting servers in a DatabaseServerStore created with
// DefaultServerStore.
func TestDefaultServerStore(t *testing.T) {
	// Create a new default store.
	store, err := dqlite.DefaultServerStore(":memory:")
	require.NoError(t, err)

	// Set and get some targets.
	err = store.Set(context.Background(), []string{"1.2.3.4:666", "5.6.7.8:666"})
	require.NoError(t, err)

	targets, err := store.Get(context.Background())
	assert.Equal(t, []string{"1.2.3.4:666", "5.6.7.8:666"}, targets)

	// Set and get some new targets.
	err = store.Set(context.Background(), []string{"1.2.3.4:666", "9.9.9.9:666"})
	require.NoError(t, err)

	targets, err = store.Get(context.Background())
	assert.Equal(t, []string{"1.2.3.4:666", "9.9.9.9:666"}, targets)

	// Setting duplicate targets returns an error and the change is not
	// persisted.
	err = store.Set(context.Background(), []string{"1.2.3.4:666", "1.2.3.4:666"})
	assert.EqualError(t, err, "failed to insert server 1.2.3.4:666: UNIQUE constraint failed: servers.address")

	targets, err = store.Get(context.Background())
	assert.Equal(t, []string{"1.2.3.4:666", "9.9.9.9:666"}, targets)
}
