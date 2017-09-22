package log

import (
	"fmt"
	"strings"
)

// Logger is a thin wrapper of stdlib's logger that provides support for log
// levels and it's prefix-based friendly.
type Logger struct {
	backend  Backend
	level    Level
	prefixes []string
}

// New creates a new level-aware logger.
func New(backend Backend, level Level) *Logger {
	return &Logger{
		backend: backend,
		level:   level,
	}

}

// Augment returns a new logger which has the same settings as this
// one, but with its prefix augmented with the given string.
func (l *Logger) Augment(prefix string) *Logger {
	return &Logger{
		backend:  l.backend,
		level:    l.level,
		prefixes: append(l.prefixes, prefix),
	}
}

// Tracef logs messages at Trace level.
func (l *Logger) Tracef(format string, v ...interface{}) {
	l.printf(Trace, format, v...)
}

// Debugf logs messages at Debug level.
func (l *Logger) Debugf(format string, v ...interface{}) {
	l.printf(Debug, format, v...)
}

// Infof logs messages at Info level.
func (l *Logger) Infof(format string, v ...interface{}) {
	l.printf(Info, format, v...)
}

// Errorf logs messages at Error level.
func (l *Logger) Errorf(format string, v ...interface{}) {
	l.printf(Error, format, v...)
}

// Panicf logs messages at Pannic level.
func (l *Logger) Panicf(format string, v ...interface{}) {
	l.printf(Panic, format, v...)

	prefix := strings.Join(l.prefixes, ": ") + ": "
	panic(fmt.Sprintf(prefix+format, v...))
}

func (l *Logger) printf(level Level, format string, v ...interface{}) {
	if level < l.level {
		return
	}

	message := ""
	for _, prefix := range l.prefixes {
		message += prefix + ": "
	}
	message += fmt.Sprintf(format, v...)

	l.backend.Output(level, message)
}
