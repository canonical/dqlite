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
	"strings"
	"testing"
	"time"

	"github.com/CanonicalLtd/dqlite/internal/trace"
	"github.com/stretchr/testify/assert"
)

func TestSet_Errors(t *testing.T) {
	cases := []struct {
		name  string
		f     func(*trace.Set)
		panic string
	}{
		{
			`remove non-existing tracer`,
			func(set *trace.Set) {
				set.Del("x")
			},
			"no tracer named x is registered",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			set := trace.NewSet(3)
			f := func() { c.f(set) }
			assert.PanicsWithValue(t, c.panic, f)
		})
	}
}

func TestSet_String(t *testing.T) {
	// Wrap tracer.Add by setting a deterministic timestamp.
	var i int64
	add := func(tracer *trace.Tracer, message string, args ...interface{}) {
		i++
		tracer.Message(message, args...)
	}

	now := func() time.Time { return time.Unix(123450+i, 123450000) }
	set := trace.NewSet(3)
	set.Now(now)

	tracer1 := set.Add("x")
	tracer2 := set.Add("y")
	tracer3 := set.Add("z")
	tracer4 := tracer3.With(trace.Integer("foo", 1))
	tracer5 := tracer4.With(trace.Integer("bar", 2))

	add(tracer3, "a %d", 123)
	add(tracer1, "b %s", "abc")
	add(tracer2, "c")
	add(tracer3, "d")
	add(tracer3, "e")
	add(tracer2, "f %s %d", "abc", 123)
	add(tracer2, "g")
	add(tracer4, "h")
	add(tracer5, "i %s", "abc")

	s := `1970-01-02 10:17:32.12345: x: b abc
1970-01-02 10:17:33.12345: y: c
1970-01-02 10:17:35.12345: z: e
1970-01-02 10:17:36.12345: y: f abc 123
1970-01-02 10:17:37.12345: y: g
1970-01-02 10:17:38.12345: z: foo=1 h
1970-01-02 10:17:39.12345: z: foo=1 bar=2 i abc
`
	assert.Equal(t, s, set.String())

	// Each tracer has saved at most 3 entries.
	assert.Equal(t, 1, strings.Count(s, ": x:"))
	assert.Equal(t, 3, strings.Count(s, ": y:"))
	assert.Equal(t, 3, strings.Count(s, ": z:"))

	// A panic includes all trace entries.
	f := func() {
		tracer3.Panic("boom: %s", "bye bye")
	}
	assert.PanicsWithValue(t, "boom: bye bye\n\ntrace:\n"+s, f)

	// If forwarding to the test logger is enabled, trace entries are not
	// included in the panic message.
	set.Testing(&testing.T{}, 1)
	tracer1.Message("hello")
	assert.PanicsWithValue(t, "boom: bye bye", f)
}

func TestSet_Testing(t *testing.T) {
	set := trace.NewSet(3)
	set.Testing(&testing.T{}, 1)
	set.Add("foo").Message("hello")
}
