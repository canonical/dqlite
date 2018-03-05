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

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTracer_MessagePanic(t *testing.T) {
	set := NewSet(3)
	tracer := set.Add("foo")

	f := func() {
		tracer.Message("hello", 1, 2, 3, 4, 5)
	}

	// The maximum number of arguments is 4.
	assert.PanicsWithValue(t, "a trace entry can have at most 4 args, but 5 were given", f)
}

func TestTracer_Error(t *testing.T) {
	set := NewSet(3)
	tracer := set.Add("foo")
	tracer.Error("hello", fmt.Errorf("boom"))

	// The cursor has advanced.
	assert.Equal(t, 1, tracer.buffer.cursor.Position())
}

func TestTracer_Panic(t *testing.T) {
	set := NewSet(3)
	set.Testing(t, 1)
	tracer := set.Add("foo")

	f := func() {
		tracer.Panic("hello %d", 123)
	}

	// The maximum number of arguments is 4.
	assert.PanicsWithValue(t, "hello 123", f)
}

func TestTracer_WithPanic(t *testing.T) {
	set := NewSet(3)
	tracer := set.Add("foo")

	f := func() {
		fields := make([]Field, 7)
		for i := range fields {
			fields[i] = String("x", "y")
		}
		tracer.With(fields...)
	}

	// The maximum number of fields is 6.
	assert.PanicsWithValue(t, "a trace entry can have at most 6 fields, but 7 were given", f)
}
