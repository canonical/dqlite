package logging

import (
	"log"
	"testing"
)

// NewTesting returns a Logger that forwards its output to the test logger.
func NewTesting(t *testing.T) *Logger {
	out := &testingWriter{t: t}
	flags := log.Ltime | log.Lmicroseconds
	return New(log.New(out, "", flags), Trace, "")
}

// Implement io.Writer and forward what it receives to a
// t.Testing logger.
type testingWriter struct {
	t *testing.T
}

func (w *testingWriter) Write(p []byte) (n int, err error) {
	w.t.Logf(string(p))
	return len(p), nil
}
