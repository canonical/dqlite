package timeline

import "time"

var NewThread = newThread

func (t *Thread) Current() []*Event {
	return t.current()
}

func (t *Thread) Now(now func() time.Time) {
	t.now = now
}
