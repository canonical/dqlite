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
	"sort"
	"sync"
	"testing"
	"time"
)

// Registry holding a collection of Tracer objects indexed by name.
type Registry struct {
	tracers map[string]*Tracer // Index of available tracers by name.
	retain  int                // Number of entries each tracer will retain.
	now     now                // Function returning the current time.
	mu      sync.RWMutex       // Serialize access to the tracers index.

	// For testing only.
	testing testing.TB // Emitted entries will also be sent to the test logger.
	node    int        // Index of the node emitting the entries.
}

// NewRegistry creates a new trace Registry.
//
// Each Registry has a number of 'tracers', each holding a different buffer
// of trace entries, and each retaining at most 'retain' entrier.
//
// Each tracer is not concurrency-safe.
func NewRegistry(retain int) *Registry {
	return &Registry{
		tracers: make(map[string]*Tracer),
		retain:  retain,
		now:     time.Now,
	}
}

// Add a new tracer to the registry.
func (r *Registry) Add(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.tracers[name]; ok {
		panic(fmt.Sprintf("a tracer named %s is already registered", name))
	}
	cursor := newCursor(0, r.retain)
	entries := make([]entry, r.retain)

	tracer := newTracer(cursor, entries, r.now, r.panic)

	if r.testing != nil {
		r.testingForward(name, tracer)
	}

	r.tracers[name] = tracer
}

// Get the tracer with the given name.
func (r *Registry) Get(name string) *Tracer {
	r.mu.RLock()
	defer r.mu.RUnlock()

	tracer, ok := r.tracers[name]
	if !ok {
		panic(fmt.Sprintf("no tracer named %s is registered", name))
	}
	return tracer
}

// Remove the tracer with the given name.
func (r *Registry) Remove(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	_, ok := r.tracers[name]
	if !ok {
		panic(fmt.Sprintf("no tracer named %s is registered", name))
	}

	delete(r.tracers, name)
}

// Testing sets the tracers to log emitted entries through the given testing
// instance.
func (r *Registry) Testing(t testing.TB, node int) {
	r.testing = t
	r.node = node

	for name, tracer := range r.tracers {
		r.testingForward(name, tracer)
	}
}

func (r *Registry) testingForward(name string, tracer *Tracer) {
	tracer.forward = func(entry entry) {
		r.testing.Logf("%d: %s: %s: %s\n", r.node, entry.Timestamp(), name, entry.Message())
	}
}

// String returns a string representing all current entries, in all current
// tracers, ordered by timestamp.
func (r *Registry) String() string {
	entries := make([]struct {
		e entry  // Actual entry object
		n string // Name of the tracer that emitted the entry
	}, 0)

	for name, tracer := range r.tracers {
		for _, e := range tracer.current() {
			entries = append(entries, struct {
				e entry
				n string
			}{e, name})
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].e.timestamp.Before(entries[j].e.timestamp)
	})

	result := ""

	for _, entry := range entries {
		result += fmt.Sprintf(
			"%s: %s: %s\n", entry.e.Timestamp(), entry.n, entry.e.Message())
	}

	return result
}

func (r *Registry) panic(message string) {
	if r.testing == nil {
		message += fmt.Sprintf("\n\ntrace:\n%s", r)
	}
	panic(message)
}
