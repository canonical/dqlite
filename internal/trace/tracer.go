// Copyright 2017 Canonical Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package trace

import "fmt"

// Tracer holds a buffer of recent trace entries in a trace Registry.
type Tracer struct {
	cursor  *cursor
	entries []entry
	fields  [maxFields]Field
	now     now
	panic   func(string)
	forward func(entry)
}

// Creates a new tracer.
func newTracer(cursor *cursor, entries []entry, now now, panic func(string)) *Tracer {
	return &Tracer{
		cursor:  cursor,
		entries: entries,
		fields:  [maxFields]Field{},
		now:     now,
		panic:   panic,
	}
}

// Message emits a new trace message.
func (t *Tracer) Message(message string, args ...interface{}) {
	if n := len(args); n > maxArgs {
		panic(fmt.Sprintf("a trace entry can have at most %d args, but %d were given", maxArgs, n))
	}
	t.emit(message, args, nil)
}

// Emit a new trace entry with an error attached.
func (t *Tracer) Error(message string, err error) {
	t.emit(message, nil, err)
}

// Panic causes a Go panic which will print all trace entries across all
// tracers.
func (t *Tracer) Panic(message string, v ...interface{}) {
	t.panic(fmt.Sprintf(message, v...))
}

// Emit a new trace entry.
func (t *Tracer) emit(message string, args []interface{}, err error) {
	entry := entry{
		timestamp: t.now(),
		message:   message,
		error:     err,
		fields:    &t.fields,
	}
	for i, arg := range args {
		entry.args[i] = arg
	}

	t.entries[t.cursor.Position()] = entry
	t.cursor.Advance()

	if t.forward != nil {
		t.forward(entry)
	}
}

// With returns a new Tracer instance emitting entries in the same buffer of this
// tracer, but with additional predefined fields.
func (t *Tracer) With(fields ...Field) *Tracer {
	if n := len(fields); n > maxFields {
		panic(fmt.Sprintf("a trace entry can have at most %d fields, but %d were given", maxFields, n))
	}

	// Create the child tracer, cloning the parent and using its entries
	// buffer.
	tracer := &Tracer{
		cursor:  t.cursor,
		entries: t.entries,
		fields:  [maxFields]Field{},
		now:     t.now,
		panic:   t.panic,
		forward: t.forward,
	}

	// Copy the fields of the parent into the child.
	i := 0
	for ; t.fields[i].key != ""; i++ {
		tracer.fields[i] = t.fields[i]
	}

	// Add the child fields.
	for j := range fields {
		tracer.fields[i+j] = fields[j]
	}

	return tracer
}

// Return how many entries are retained at most.
func (t *Tracer) size() int {
	return len(t.entries)
}

// Return the list of current entrys in the tracer.
func (t *Tracer) current() []entry {
	entries := make([]entry, 0)

	// We don't keep track of the actual number of entries in the buffer,
	// instead we iterate them backwards until we find a "null" entry.
	cursor := newCursor(t.cursor.Position(), t.size())
	for i := 0; i < t.size(); i++ {
		cursor.Retract()
		previous := t.entries[cursor.Position()]
		if previous.timestamp.Unix() == epoch {
			break
		}
		entries = append([]entry{previous}, entries...)
	}

	return entries
}
