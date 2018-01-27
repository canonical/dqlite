package timeline

var NewEvent = newEvent

// Message returns the message associated with the event.
func (e *Event) Message() string {
	return e.message
}
