package log

import "fmt"

// Level defines the verbosity of Logger.
type Level int

// Possible logging levels
const (
	Trace Level = iota
	Debug
	Info
	Error
	Panic
)

func (l Level) String() string {
	switch l {
	case Trace:
		return "TRACE"
	case Debug:
		return "DEBUG"
	case Info:
		return "INFO"
	case Error:
		return "ERROR"
	case Panic:
		return "PANIC"
	default:
		panic(fmt.Sprintf("unknown level: %d", l))
	}
}
