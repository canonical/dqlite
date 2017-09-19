package logging

import (
	"fmt"
	"log"
)

// Logger is a thin wrapper of stdlib's logger that provides support for log
// levels and it's prefix-based friendly.
type Logger struct {
	logger *log.Logger
	level  Level
	prefix string
}

// New creates a new level-aware logger.
func New(logger *log.Logger, level Level, prefix string) *Logger {
	return &Logger{
		logger: logger,
		level:  level,
		prefix: prefix,
	}

}

// AugmentPrefix returns a new logger which has the same settings as the given
// one, but with its prefix augmented with the given string.
func AugmentPrefix(logger *Logger, prefix string) *Logger {
	return &Logger{
		logger: logger.logger,
		level:  logger.level,
		prefix: logger.prefix + prefix,
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
	panic(l.format(format, v...))
}

func (l *Logger) printf(level Level, format string, v ...interface{}) {
	if level < l.level {
		return
	}
	l.logger.Output(3, fmt.Sprintf("[%s] %s", level, l.format(format, v...)))
}

func (l *Logger) format(format string, v ...interface{}) string {
	format = fmt.Sprintf("%s%s", l.prefix, format)
	return fmt.Sprintf(format, v...)
}
