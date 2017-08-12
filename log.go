package dqlite

import (
	"bytes"
	"io"
	"log"
	"sync"

	"github.com/hashicorp/logutils"
)

// NewLogger is a convenience to create a new log.Logger with a filter
// for the given logging level applied.
func NewLogger(output io.Writer, level string, flag int) *log.Logger {
	if level == "" {
		level = "INFO"
	}

	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"DEBUG", "WARN", "INFO", "ERR", "NONE"},
		MinLevel: logutils.LogLevel(level),
		Writer:   output,
	}

	return log.New(filter, "", flag)
}

// LevelFilterWithOrigin is an io.Writer that can be used with a logger that
// will filter out log messages that aren't at least a certain level or that
// don't match a certain origin.
//
// A message is considered having a certain level and origin if it's of the
// form:
//
//   "[<level>] <origin>: <body>"
//
// If no level and origin are given, the message will passthrough.
type LevelFilterWithOrigin struct {
	logutils.LevelFilter
	Origins []string

	goodOrigins map[string]struct{}
	once        sync.Once
}

func (f *LevelFilterWithOrigin) Write(p []byte) (n int, err error) {
	// Note in general that io.Writer can receive any byte sequence
	// to write, but the "log" package always guarantees that we only
	// get a single line. We use that as a slight optimization within
	// this method, assuming we're dealing with a single, complete line
	// of log data.

	if !f.Check(p) {
		return len(p), nil
	}

	return f.Writer.Write(p)
}

// Check will check if a given line should be included in the filter.
func (f *LevelFilterWithOrigin) Check(line []byte) bool {
	if !f.LevelFilter.Check(line) {
		return false
	}

	if f.Origins == nil {
		return true
	}

	f.once.Do(f.init)

	var origin string
	if x := bytes.IndexByte(line, ']'); x > 0 {
		if y := bytes.IndexByte(line, ':'); y > 0 {
			origin = string(line[x+2 : y])
		}
	}

	_, ok := f.goodOrigins[origin]
	return ok
}

// SetOrigins is used to update the accepted origins. Pass nil for all origins.
func (f *LevelFilterWithOrigin) SetOrigins(origins []string) {
	f.Origins = origins
	f.init()
}

func (f *LevelFilterWithOrigin) init() {
	f.goodOrigins = make(map[string]struct{})
	f.goodOrigins[""] = struct{}{}
	for _, origin := range f.Origins {
		f.goodOrigins[origin] = struct{}{}
	}
}
