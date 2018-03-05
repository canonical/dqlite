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

package registry

import (
	"reflect"
	"sync"
)

// HookSyncSet creates a new hookSync instance associated with this Registry.
func (r *Registry) HookSyncSet() {
	r.hookSyncEnsureUnset()
	r.hookSync = newHookSync()
}

// HookSyncAdd adds a new log command data to the underlying hookSync, which is
// expected to match the Log.Data bytes received next FSM.Apply() call.
func (r *Registry) HookSyncAdd(data []byte) {
	r.hookSyncEnsureSet()
	r.hookSync.Add(data)
}

// HookSyncMatches checks whether the Log.Data bytes that an FSM.Apply() call
// is about to process match the ones that were last added to the hookSync via
// HookSyncAdd(). If no hookSync is set on the registry, always return true.
func (r *Registry) HookSyncMatches(data []byte) bool {
	if r.hookSync == nil {
		return true
	}
	return r.hookSync.Matches(data)
}

// HookSyncWait blocks until the underlying hookSync is done.
//
// It assumes that the lock is held, releasing it before blocking and requiring
// it thereafter.
func (r *Registry) HookSyncWait() {
	r.hookSyncEnsureSet()
	hookSync := r.hookSync
	r.Unlock()
	hookSync.Wait()
	r.Lock()
}

// HookSyncReset clears the hookSync instance created with HookSyncSet.
func (r *Registry) HookSyncReset() {
	r.hookSyncEnsureSet()
	r.hookSync.Done() // Unblock any FSM.Apply() call waiting on this hookSync.
	r.hookSync = nil
}

// Ensure that a hookSync instance is set.
func (r *Registry) hookSyncEnsureSet() {
	if r.hookSync == nil {
		panic("no hookSync instance set on this registry")
	}
}

// Ensure that a hookSync instance is not set.
func (r *Registry) hookSyncEnsureUnset() {
	if r.hookSync != nil {
		panic("a hookSync instance is set on this registry")
	}
}

// HookSync is used to synchronize a Methods instance and an FSM instance
// between each other.
//
// The goal is that if a replication hook of Methods instance is in progress,
// the associated FSM instance should only execute log commands applied by that
// hook, and block the execution of any log command not applied by the hook
// until the hook is done.
//
// The semantics of HookSync is somewhat similar to sync.WaitGroup.
//
// The synchronization protocol goes through the following steps:
//
//  - The Methods instance starts executing a replication hook.
//
//  - The Methods instance acquires the the Registry lock and creates a new
//    HookSync instance.
//
//  - Whenever the Methods instance is about to apply a log command, it calls
//    HookSync.Add(), which saves the reference to the data bytes slice to be
//    applied in HookSync.data, and increases by one the number of reader locks
//    on HookSync.mu lock. The Methods instance then releases the Registry
//    lock.
//
//  - The FSM starts executing a log command.
//
//  - The FSM acquires the Registry lock and check if a HookSync instance
//    is set.
//
//  - If no HookSync instance is set, the FSM continues normally.
//
//  - If the HookSync instance is set and HookSync.Matches() returns true, then
//    the HookSync.data field matches the Log.Data field of the log command
//    being applied, and the FSM continues normally.
//
//  - If the HookSync instance is set and HookSync.Matches() returns false,
//    then the HookSync.data field does not match the Log.Data field of the log
//    command being applied, the FSM releases the lock on the Registry and
//    calls HookSync.Wait() which tries to acquire the HookSync.mu lock (which
//    is being held by the Methods instance running the replication hook).
//
//  - When control eventually returns to the Methods instance after the
//    Raft.Apply() call returns, the Methods instance re-acquires the Registry
//    lock and resumes the execution of the replication hook. When the hook is
//    about to finish, the Methods instance calls HookSync.Done(), which
//    releases all the hookSync.mu reader locks previously acquired. Finally,
//    the Methods instance releases the Registry lock.
//
//  - If the FSM was blocked on HookSync.Wait(), it's now free to proceed.
//
// See also Methods.Begin, Methods.Frames, Methods.Undo and FSM.Apply for
// details.
type hookSync struct {
	// Use an RW mutex because it might be acquired multiple times by a
	// Methods instance applying more than one log command.
	mu sync.RWMutex

	// Track the number of times the mu mutex was locked with RLock.
	n int

	// Reference to the Log.Data payload of the last log command applied.
	data []byte
}

func newHookSync() *hookSync {
	return &hookSync{}
}

// Add is invoked by a Methods instance before calling Raft.Apply(). It
// sets the data beying applied and increases the number of lock readers by
// one.
//
// This can be called multiple times by a Methods instance during the execution
// of a replication hook.
func (s *hookSync) Add(data []byte) {
	s.mu.RLock()
	s.n++
	s.data = data
}

// Matches returns true if the data referenced by this HookSync matches the
// one of the given raft.Log.Data.
func (s *hookSync) Matches(data []byte) bool {
	return reflect.ValueOf(s.data).Pointer() == reflect.ValueOf(data).Pointer()
}

// Wait blocks until our mutex has no more readers, i.e. the replication hook
// that created us has completed.
func (s *hookSync) Wait() {
	s.mu.Lock()
}

// Done releases all reader locks acquired during our life cycle.
func (s *hookSync) Done() {
	for i := 0; i < s.n; i++ {
		s.mu.RUnlock()
	}
}
