package dqlite

import (
	"io"
	"log"

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
