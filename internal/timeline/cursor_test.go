package timeline_test

import (
	"testing"

	"github.com/CanonicalLtd/dqlite/internal/timeline"
	"github.com/stretchr/testify/assert"
)

func TestTimelineCursor_AdvanceAndRetract(t *testing.T) {
	cursor := timeline.NewCursor(0, 3)
	assert.Equal(t, 0, cursor.Position())

	cursor.Advance()
	assert.Equal(t, 1, cursor.Position())

	cursor.Advance()
	assert.Equal(t, 2, cursor.Position())

	cursor.Advance()
	assert.Equal(t, 0, cursor.Position())

	cursor.Retract()
	assert.Equal(t, 2, cursor.Position())

	cursor.Retract()
	assert.Equal(t, 1, cursor.Position())

	cursor.Retract()
	assert.Equal(t, 0, cursor.Position())
}
