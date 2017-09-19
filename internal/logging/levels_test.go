package logging_test

import (
	"testing"

	"github.com/CanonicalLtd/dqlite/internal/logging"
	"github.com/stretchr/testify/assert"
)

func TestLevel_Unknown(t *testing.T) {
	level := logging.Level(666)
	f := func() { level.String() }
	assert.Panics(t, f)
}
