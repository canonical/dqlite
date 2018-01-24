package log

import (
	"log"
	"testing"
)

// Func is a generic interface that logs a message against a certain backend.
type Func func(level Level, message string) error

// Standard creates a log Func that delegates logging to the stdlib logger.
func Standard() Func {
	return func(level Level, message string) error {
		log.Printf("[%s] %s", level, message)
		return nil
	}
}

// Testing adapts a testing logger to the Func interface.
func Testing(t *testing.T, node int) Func {
	return func(level Level, message string) error {
		t.Logf("%d: [%s] %s", node, level, message)
		return nil
	}
}
