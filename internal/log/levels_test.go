package log_test

import (
	"testing"

	"github.com/CanonicalLtd/dqlite/internal/log"
	"github.com/stretchr/testify/assert"
)

func TestLevel_Unknown(t *testing.T) {
	level := log.Level(666)
	var s string
	f := func() { s = level.String() }
	assert.Panics(t, f)
}
