package timeline

// A cursor holds the index of an entry of a circular buffer.
type cursor struct {
	position int // Current position of the cursor
	length   int // Lenght of the circular buffer.
}

func newCursor(position, length int) cursor {
	return cursor{
		position: position,
		length:   length,
	}
}

func (c *cursor) Position() int {
	return c.position
}

func (c *cursor) Advance() {
	c.position = (c.position + c.length + 1) % c.length
}

func (c *cursor) Retract() {
	c.position = (c.position + c.length - 1) % c.length
}
