package log

import "fmt"

// Logger is a thin wrapper of stdlib's logger that provides support for log
// levels and it's prefix-based friendly.
type Logger struct {
	f        Func
	level    Level
	prefixes []string
}

// New creates a new level-aware logger.
func New(f Func, level Level) *Logger {
	return &Logger{
		f:     f,
		level: level,
	}

}

// Func replaces the logging function used by this logger.
func (l *Logger) Func(f Func) {
	l.f = f
}

// Augment returns a new logger which has the same settings as this
// one, but with its prefix augmented with the given string.
func (l *Logger) Augment(prefix string) *Logger {
	return &Logger{
		f:        l.f,
		level:    l.level,
		prefixes: append(l.prefixes, prefix),
	}
}

// Tracef logs messages at Trace level.
func (l *Logger) Tracef(format string, v ...interface{}) {
	l.output(Trace, format, v...)
}

// Debugf logs messages at Debug level.
func (l *Logger) Debugf(format string, v ...interface{}) {
	l.output(Debug, format, v...)
}

// Infof logs messages at Info level.
func (l *Logger) Infof(format string, v ...interface{}) {
	l.output(Info, format, v...)
}

// Errorf logs messages at Error level.
func (l *Logger) Errorf(format string, v ...interface{}) {
	l.output(Error, format, v...)
}

// Panicf logs messages at Pannic level.
func (l *Logger) Panicf(format string, v ...interface{}) {
	l.output(Panic, format, v...)
	panic(l.sprintf(format, v...))
}

func (l *Logger) output(level Level, format string, v ...interface{}) {
	if level < l.level {
		return
	}

	// FIXME: check for errors?
	l.f(level, l.sprintf(format, v...))
}

func (l *Logger) sprintf(format string, v ...interface{}) string {
	s := ""
	for _, prefix := range l.prefixes {
		s += prefix + ": "
	}
	s += fmt.Sprintf(format, v...)
	return s
}
