package timeline

import (
	"time"

	"github.com/CanonicalLtd/dqlite/internal/log"
)

// Thread captures a single thread of events in a timeline.
type Thread struct {
	label  string
	cursor cursor
	events []*Event
	now    func() time.Time
	logger *log.Logger
}

// New creates a new timeline which will save recent events.
//
// The 'size' parameter indicates how many events to retain at most. Older
// events will be progressively discarded.
func newThread(label string, size int, logger *log.Logger) *Thread {
	return &Thread{
		label:  label,
		cursor: newCursor(0, size),
		events: make([]*Event, size),
		now:    time.Now,
		logger: logger,
	}
}

// Add a new event in the timeline.
func (t *Thread) Add(message string, args ...interface{}) {
	event := newEvent(t.label, t.now(), message, args...)

	t.events[t.cursor.Position()] = event
	t.cursor.Advance()

	if t.logger != nil {
		t.logger.Tracef("%s", event)
	}
}

// Return how many events are retained at most.
func (t *Thread) size() int {
	return len(t.events)
}

// Return the list of current events in the thread.
func (t *Thread) current() []*Event {
	events := make([]*Event, 0)
	cursor := newCursor(t.cursor.Position(), t.size())

	for i := 0; i < t.size(); i++ {
		cursor.Retract()
		event := t.events[cursor.Position()]
		if event == nil {
			break
		}
		events = append([]*Event{event}, events...)
	}

	return events
}
