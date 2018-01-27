package timeline

import (
	"fmt"
	"time"
)

// Event represents a single point in a timeline.
type Event struct {
	label     string        // Label of the thread that added the event.
	timestamp time.Time     // Time at which the event was added.
	message   string        // Message format for the event.
	args      []interface{} // Arguments to the message format.
}

func newEvent(label string, timestamp time.Time, message string, args ...interface{}) *Event {
	return &Event{
		label:     label,
		timestamp: timestamp,
		message:   message,
		args:      args,
	}
}

func (e *Event) String() string {
	format := fmt.Sprintf("%v: %s: %s", e.timestamp.Format("2006-01-02 15:04:05.00000"), e.label, e.message)
	return fmt.Sprintf(format, e.args...)
}
