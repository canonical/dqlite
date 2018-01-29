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

import "time"

func NewEntry(timestamp time.Time, message string) entry {
	return entry{
		timestamp: timestamp,
		message:   message,
		args:      [maxArgs]interface{}{},
		fields:    &[maxFields]Field{},
	}
}

func NewEntries(n int) []entry {
	return make([]entry, n)
}

func (e *entry) Set(args []interface{}, err error, fields []Field) {
	if args != nil {
		for i, arg := range args {
			e.args[i] = arg
		}
	}
	if err != nil {
		e.error = err
	}
	if fields != nil {
		for i, field := range fields {
			e.fields[i] = field
		}
	}
}
