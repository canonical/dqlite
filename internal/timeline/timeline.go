package timeline

import (
	"fmt"
	"sort"

	"github.com/CanonicalLtd/dqlite/internal/log"
)

// Timeline captures the most recent events happened in dqlite.
type Timeline struct {
	threads    map[string]*Thread
	threadSize int
	logger     *log.Logger
}

// New creates a new timeline which will save recent events.
//
// Each timeline has a number of 'threads', each capturing a different stream
// of events, and retaining at most 'threadSize' events.
//
// Each thread is not concurrency-safe.
func New(threadSize int) *Timeline {
	return &Timeline{
		threads:    make(map[string]*Thread),
		threadSize: threadSize,
	}
}

// Thread returns the thread with the given label, creating it if does not
// exist yet.
func (t *Timeline) Thread(label string) *Thread {
	thread, ok := t.threads[label]

	if !ok {
		thread = newThread(label, t.threadSize, t.logger)
		t.threads[label] = thread
	}

	return thread
}

// Logger attaches a logger to the timeline. Events will also be forwarded to it.
func (t *Timeline) Logger(logger *log.Logger) {
	t.logger = logger

	// Update current threads.
	for _, thread := range t.threads {
		thread.logger = t.logger
	}
}

// String returns a string representing all current events, in all current
// threads, ordered by timestamp.
func (t *Timeline) String() string {
	events := make([]*Event, 0)

	for _, thread := range t.threads {
		events = append(events, thread.current()...)
	}

	sort.Slice(events, func(i, j int) bool {
		return events[i].timestamp.Before(events[j].timestamp)
	})

	s := ""
	for _, event := range events {
		s += fmt.Sprintf("%s\n", event)
	}

	return s
}
