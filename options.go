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

// LogLevel sets the logging level for messages emitted by the driver.
//
// Possible values are "TRACE", "DEBUG", "INFO", "ERROR", "PANIC"
func LogLevel(level string) Option {
	return func(options *options) {
		options.logLevel = level
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

// Create a options instance with default values.
func newOptions() *options {
	return &options{
		logFunc:        log.Standard(),
		logLevel:       "INFO",
		barrierTimeout: time.Minute,
		applyTimeout:   10 * time.Second,
	}
}

type options struct {
	logFunc        log.Func
	logLevel       string
	applyTimeout   time.Duration
	barrierTimeout time.Duration
}
