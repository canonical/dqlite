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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Append a new entry to the buffer.
func TestBuffer_Append(t *testing.T) {
	buffer := newBuffer(3)
	buffer.Append(time.Now(), "hello %d", []interface{}{1}, nil, &fields{})
	entries := buffer.Entries()
	assert.Len(t, entries, 1)
	assert.Equal(t, "hello 1", entries[0].Message())
	// The cursor has advanced.
	assert.Equal(t, 1, buffer.cursor.Position())
}

// Old entries are evicted as new entries are added.
func TestBuffer_Entries(t *testing.T) {
	max := 3
	for n := 1; n < 5; n++ {
		t.Run(fmt.Sprintf("%d entries", n), func(t *testing.T) {
			buffer := newBuffer(max)
			for i := 1; i <= n; i++ {
				message := fmt.Sprintf("%d", i)
				buffer.Append(time.Now(), message, nil, nil, &fields{})
			}

			// There are at most 'max' entries.
			entries := buffer.Entries()
			assert.Len(t, entries, int(math.Min(float64(n), float64(max))))

			// The cursor has advanced.
			assert.Equal(t, n%max, buffer.cursor.Position())

			// Check the last inserted entry.
			entry := buffer.Last()
			assert.Equal(t, fmt.Sprintf("%d", n), entry.Message())
		})
	}
}
