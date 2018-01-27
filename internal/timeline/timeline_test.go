package timeline_test

import (
	"testing"
	"time"

	"github.com/CanonicalLtd/dqlite/internal/log"
	"github.com/CanonicalLtd/dqlite/internal/timeline"
	"github.com/stretchr/testify/assert"
)

func TestTimeline_String(t *testing.T) {
	logger := log.New(log.Testing(t, 0), log.Trace)

	// Wrap thread.Add by setting a deterministic timestamp.
	var i int64
	add := func(thread *timeline.Thread, message string) {
		i++
		now := func() time.Time { return time.Unix(123450+i, 123450000) }
		thread.Now(now)
		thread.Add(message)
	}

	timeline := timeline.New(3)

	thread1 := timeline.Thread("x")
	thread2 := timeline.Thread("y")
	thread3 := timeline.Thread("z")

	timeline.Logger(logger)

	add(thread3, "a")
	add(thread1, "b")
	add(thread2, "c")
	add(thread3, "d")
	add(thread3, "e")
	add(thread2, "f")
	add(thread2, "g")
	add(thread3, "h")

	s := `1970-01-02 10:17:32.12345: x: b
1970-01-02 10:17:33.12345: y: c
1970-01-02 10:17:34.12345: z: d
1970-01-02 10:17:35.12345: z: e
1970-01-02 10:17:36.12345: y: f
1970-01-02 10:17:37.12345: y: g
1970-01-02 10:17:38.12345: z: h
`

	assert.Equal(t, s, timeline.String())
}
