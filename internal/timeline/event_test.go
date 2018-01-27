package timeline_test

import (
	"testing"
	"time"

	"github.com/CanonicalLtd/dqlite/internal/timeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimelineEvent_String(t *testing.T) {
	format := "Jan 2, 2006 at 3:04am"
	timestamp, err := time.Parse(format, "Jan 27, 2018 at 10:55am")
	require.NoError(t, err)

	event := timeline.NewEvent("x", timestamp, "hello")
	assert.Equal(t, "2018-01-27 10:55:00.00000: x: hello", event.String())

	event = timeline.NewEvent("x", timestamp, "hello %s %d", "world", 42)
	assert.Equal(t, "2018-01-27 10:55:00.00000: x: hello world 42", event.String())
}
