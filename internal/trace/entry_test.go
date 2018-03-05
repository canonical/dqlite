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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEntry_Message(t *testing.T) {
	cases := []struct {
		message string
		args    []interface{}
		error   error
		fields  []Field
		result  string
	}{
		{
			"hello %d",
			[]interface{}{uint64(123)},
			nil,
			nil,
			"hello 123",
		},
		{
			"hello",
			nil,
			fmt.Errorf("boom"),
			nil,
			"hello: boom",
		},
		{
			"hello",
			nil,
			nil,
			[]Field{String("x", "y"), Integer("z", int64(1))},
			"x=y z=1 hello",
		},
		{
			"hello %s %d",
			[]interface{}{"world", 123},
			fmt.Errorf("boom"),
			[]Field{String("x", "y"), Integer("z", int64(1))},
			"x=y z=1 hello world 123: boom",
		},
	}
	for _, c := range cases {
		entry := entry{
			timestamp: time.Now(),
			message:   c.message,
			error:     c.error,
		}
		for i, arg := range c.args {
			entry.args[i] = arg
		}
		entry.fields = &fields{}
		for i, field := range c.fields {
			entry.fields[i] = field
		}
		assert.Equal(t, c.result, entry.Message())
	}
}

func TestEntry_Timestamp(t *testing.T) {
	format := "Jan 2, 2006 at 3:04am"
	timestamp, err := time.Parse(format, "Jan 27, 2018 at 10:55am")
	require.NoError(t, err)

	entry := entry{timestamp: timestamp}
	assert.Equal(t, "2018-01-27 10:55:00.00000", entry.Timestamp())
}
