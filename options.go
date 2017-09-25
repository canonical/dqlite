package dqlite

import (
	"time"

	"github.com/CanonicalLtd/dqlite/internal/log"
)

// Option option to be passed to NewDriver to customize the resulting
// instance.
type Option func(*options)

// LogFunc sets the logging handler for messages emitted by the driver.
func LogFunc(f func(level, message string)) Option {
	return func(options *options) {
		// Little wrapper translating internal log.Level types to plain
		// strings.
		options.logFunc = func(level log.Level, message string) error {
			f(level.String(), message)
			return nil
		}
	}
}

// BarrierTimeout sets the maximum amount of time to wait for the FSM to catch
// up with the latest log.
func BarrierTimeout(timeout time.Duration) Option {
	return func(options *options) {
		options.barrierTimeout = timeout
	}

}

// ApplyTimeout sets the maximum amount of time to wait for a raft FSM command to
// be applied.
func ApplyTimeout(timeout time.Duration) Option {
	return func(options *options) {
		options.applyTimeout = timeout
	}

}

// AutoCheckpoint sets how many frames the WAL file of a database should have
// before a replicated checkpoint is automatically attempted. A value of 1000
// (the default if not set) is fine for most application.
func AutoCheckpoint(n int) Option {
	return func(options *options) {
		options.autoCheckpoint = n
	}
}

// Create a options instance with default values.
func newOptions() *options {
	return &options{
		logFunc:        log.Standard(),
		barrierTimeout: time.Minute,
		autoCheckpoint: 1000,
	}
}

type options struct {
	logFunc        log.Func
	applyTimeout   time.Duration
	barrierTimeout time.Duration
	autoCheckpoint int
}
