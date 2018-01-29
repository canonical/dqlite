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

package trace_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/CanonicalLtd/dqlite/internal/trace"
	"github.com/stretchr/testify/assert"
)

func TestTracer_Message(t *testing.T) {
	tracer := newTracer()
	tracer.Message("hello")

	// The cursor has advanced.
	assert.Equal(t, 1, tracer.Cursor().Position())
}

func TestTracer_MessagePanic(t *testing.T) {
	tracer := newTracer()

	f := func() {
		tracer.Message("hello", 1, 2, 3, 4, 5)
	}

	// The maximum number of arguments is 4.
	assert.PanicsWithValue(t, "a trace entry can have at most 4 args, but 5 were given", f)
}

func TestTracer_Error(t *testing.T) {
	tracer := newTracer()
	tracer.Error("hello", fmt.Errorf("boom"))

	// The cursor has advanced.
	assert.Equal(t, 1, tracer.Cursor().Position())
}

func TestTracer_Panic(t *testing.T) {
	tracer := newTracer()

	f := func() {
		tracer.Panic("hello %d", 123)
	}

	// The maximum number of arguments is 4.
	assert.PanicsWithValue(t, "hello 123", f)
}

func TestTracer_WithPanic(t *testing.T) {
	tracer := newTracer()

	f := func() {
		fields := make([]trace.Field, 7)
		for i := range fields {
			fields[i] = trace.String("x", "y")
		}
		tracer.With(fields...)
	}

	// The maximum number of fields is 6.
	assert.PanicsWithValue(t, "a trace entry can have at most 6 fields, but 7 were given", f)
}

func newTracer() *trace.Tracer {
	cursor := trace.NewCursor(0, 3)
	entries := trace.NewEntries(3)

	panic := func(message string) {
		panic(message)
	}

	return trace.NewTracer(cursor, entries, time.Now, panic)
}
