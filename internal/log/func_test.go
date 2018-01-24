package log_test

import (
	"bytes"
	stdlog "log"
	"os"
	"testing"

	"github.com/CanonicalLtd/dqlite/internal/log"
	"github.com/stretchr/testify/assert"
)

// The Standard log function emits logs to the stdlib logger.
func TestStandard(t *testing.T) {
	buffer := &bytes.Buffer{}
	defer stdlog.SetOutput(os.Stderr)
	defer stdlog.SetFlags(stdlog.Flags())
	stdlog.SetOutput(buffer)
	stdlog.SetFlags(0)

	f := log.Standard()
	f(log.Trace, "hello")

	assert.Equal(t, "[TRACE] hello\n", buffer.String())
}

// The Testing log function emits logs to the testing logger.
func TestTesting(t *testing.T) {
	testingT := &testing.T{}
	f := log.Testing(testingT, 1)
	f(log.Trace, "1: hello")
}
