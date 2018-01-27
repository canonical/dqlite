package timeline_test

import (
	"testing"

	"github.com/CanonicalLtd/dqlite/internal/timeline"
	"github.com/stretchr/testify/assert"
)

func TestThread_Current(t *testing.T) {
	thread := timeline.NewThread("x", 3, nil)

	thread.Add("foo")
	assert.Len(t, thread.Current(), 1)
	thread.Add("bar")
	assert.Len(t, thread.Current(), 2)
	thread.Add("egg")
	assert.Len(t, thread.Current(), 3)
	thread.Add("baz")
	assert.Len(t, thread.Current(), 3)
	thread.Add("yuk")
	assert.Len(t, thread.Current(), 3)

	messages := []string{"egg", "baz", "yuk"}

	for i, event := range thread.Current() {
		assert.Equal(t, messages[i], event.Message())
	}
}
