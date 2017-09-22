package log

import (
	"fmt"
	"log"
	"testing"
)

// Backend is the interface that concrete logging implementations should
// satisfy.
type Backend interface {
	Output(level Level, message string) error
}

// StandardBackend adapts a stdlib logger to the Backend interface
func StandardBackend(logger *log.Logger) Backend { return &standardBackend{logger} }

type standardBackend struct {
	logger *log.Logger
}

func (b *standardBackend) Output(level Level, message string) error {
	return b.logger.Output(3, fmt.Sprintf("[%s] %s", level, message))
}

// Testing adapts a testing logger to the Backend interface.
func Testing(t *testing.T) Backend { return &testingBackend{t} }

// Implement io.Writer and forward what it receives to a
// t.Testing logger.
type testingBackend struct {
	t *testing.T
}

func (b *testingBackend) Output(level Level, message string) error {
	b.t.Logf("[%s] %s", level, message)
	return nil
}
