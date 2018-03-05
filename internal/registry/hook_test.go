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

package registry_test

import (
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/CanonicalLtd/raft-test"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test the synchronization protocol flow in the normal leader case, mimicking
// a Methods instance applying log commands against a raft.Raft leader instance
// and the raft's FSM executing them.
func TestRegistry_SyncHook_Leader(t *testing.T) {
	registry, cleanup := newRegistry(t)
	defer cleanup()

	// Mimick a Methods starting a replication hook.
	registry.Lock()
	registry.HookSyncSet()

	// Mimick a Methods instance applying a log command.
	data1 := []byte("hello")
	registry.HookSyncAdd(data1)
	registry.Unlock()

	// Mimick an FSM instance applying the log command.
	registry.Lock()
	assert.True(t, registry.HookSyncMatches(data1))
	registry.Unlock()

	// Mimick a Methods instance applying another log command.
	data2 := []byte("world")
	registry.Lock()
	registry.HookSyncAdd(data2)
	registry.Unlock()

	// Mimick an FSM instance applying the second log command.
	registry.Lock()
	assert.True(t, registry.HookSyncMatches(data2))
	registry.Unlock()

	// Mimick a Methods instance finish off the replication hook.
	registry.Lock()
	registry.HookSyncReset()
	registry.Unlock()
}

// Test the synchronization protocol flow in the normal folloer case, mimicking
// an FSM instance associated with leader raft.Raft instance applying log
// commands coming from the leader.
func TestRegistry_SyncHook_Follower(t *testing.T) {
	registry, cleanup := newRegistry(t)
	defer cleanup()

	// Mimick an FSM instance applying the log command.
	registry.Lock()
	assert.True(t, registry.HookSyncMatches([]byte("hello")))
	registry.Unlock()

}

// Test the synchronization protocol flow in the case that leadership is lost
// while applying a log command, where the FSM must wait to execute further
// commands until the Methods hook is done.
func TestRegistry_SyncHook_LeadershipLost(t *testing.T) {
	registry, cleanup := newRegistry(t)
	defer cleanup()

	// Mimick a Methods starting a replication hook.
	registry.Lock()
	registry.HookSyncSet()

	// Mimick a Methods instance applying a log command.
	data1 := []byte("hello")
	registry.HookSyncAdd(data1)
	registry.Unlock()

	// Mimick an FSM instance applying the log command.
	registry.Lock()
	assert.True(t, registry.HookSyncMatches(data1))
	registry.Unlock()

	// Mimick a Methods instance applying another log command, which won't
	// be executed immediately because leadership is lost.
	registry.Lock()
	registry.HookSyncAdd([]byte("world"))
	registry.Unlock()

	// Mimick an FSM instance applying the same log command, but received
	// over the wire, i.e. as follower FSM.
	registry.Lock()
	assert.False(t, registry.HookSyncMatches([]byte("wolrd")))
	done := make(chan struct{})
	go func() {
		registry.HookSyncWait()
		done <- struct{}{}
	}()

	// Mimick a Methods instance resuming hook execution and completing it.
	registry.Lock()
	registry.HookSyncReset()
	registry.Unlock()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("simulated FSM log command execution did not resume")
	}
}

// Test that when the FSM of a leader raft instance applies a command that was
// issued by the leader itself, the Log.Data field references the exact same
// bytes slice that was passed to the Raft.Apply() call on the raft leader
// instance. Similarly, when the FSM of raft follower instance applies a
// command, the Log.Data field does not reference the same bytes slice that was
// passed to the Raft.Apply() call on the leader.
//
// This is an internal raft implementation detail that we depend on.
func TestHookSync_RaftDataReference_Follower(t *testing.T) {
	// Use an actual network transport, so the Log.Data bytes will be
	// copied over the wire when sent to the follower, as opposed to the
	// behavior of an in-memory transport.
	transports := make([]raft.Transport, 2)
	addresses := make([]raft.ServerAddress, 2)
	for i := range transports {
		transport, err := raft.NewTCPTransport("127.0.0.1:0", nil, 1, time.Second, ioutil.Discard)
		require.NoError(t, err)
		transports[i] = transport
		addresses[i] = transport.LocalAddr()
	}

	fsms := []*testHookSyncFSM{&testHookSyncFSM{}, &testHookSyncFSM{}}
	_, control := rafttest.Cluster(
		t,
		[]raft.FSM{fsms[0], fsms[1]},
		rafttest.Servers(0, 1),
		rafttest.Transport(func(i int) raft.Transport { return transports[i] }),
	)
	defer control.Close()

	r1 := control.LeadershipAcquired(time.Second)
	data := []byte("hello")
	require.NoError(t, r1.Apply(data, time.Second).Error())

	r2 := control.Other(r1)
	control.WaitIndex(r2, 3, time.Second)

	fsm1 := fsms[control.Index(r1)]
	fsm2 := fsms[control.Index(r2)]

	assert.NotNil(t, fsm1.Data)
	assert.NotNil(t, fsm2.Data)

	assert.True(t, reflect.ValueOf(fsm1.Data).Pointer() == reflect.ValueOf(data).Pointer())
	assert.False(t, reflect.ValueOf(fsm2.Data).Pointer() == reflect.ValueOf(data).Pointer())
}

// Test that when the FSM of a leader raft instance applies a command that was
// issued by the leader itself, but after it has lost leadership, the Log.Data
// reference is different from the original one passed by the leader to
// Raft.Apply((), since the log is fetched from the store and was not part of
// the leader's inflight queue.
//
// This is an internal raft implementation detail that we depend on.
func TestHookSync_RaftDataReference_Leader(t *testing.T) {
	// Use an actual boltdb store, so the Log.Data bytes will be
	// copied when fetching a log from the store
	stores := make([]raft.LogStore, 3)
	for i := range stores {
		file, err := ioutil.TempFile("", "dqlite-registry-hook-sync-test-")
		require.NoError(t, err)
		file.Close()
		defer os.Remove(file.Name())

		store, err := raftboltdb.NewBoltStore(file.Name())
		require.NoError(t, err)

		stores[i] = store
	}

	fsms := []*testHookSyncFSM{&testHookSyncFSM{}, &testHookSyncFSM{}, &testHookSyncFSM{}}
	_, control := rafttest.Cluster(
		t,
		[]raft.FSM{fsms[0], fsms[1], fsms[1]},
		rafttest.LogStore(func(i int) raft.LogStore { return stores[i] }),
	)
	defer control.Close()

	data := []byte("hello")
	stage1 := func(r *raft.Raft) {}
	stage2 := func(r *raft.Raft) {
		err := r.Apply(data, time.Second).Error()
		require.Equal(t, raft.ErrLeadershipLost, err)
	}
	stage3 := func(r *raft.Raft) {
		fsm := fsms[control.Index(r)]
		assert.False(t, reflect.ValueOf(fsm.Data).Pointer() == reflect.ValueOf(data).Pointer())
	}
	stage4 := stage1
	stage5 := stage1

	scenario := rafttest.LeadershipLostQuorumSameLeaderScenario(3)
	scenario(control, stage1, stage2, stage3, stage4, stage5)
	assert.Len(t, data, 5)
}

// A raft.FSM used to assert the assumption that Log.Data matches whatever
// passed to raft.Apply().
type testHookSyncFSM struct {
	Data []byte
}

// Apply always return a nil error without doing anything.
func (f *testHookSyncFSM) Apply(log *raft.Log) interface{} {
	f.Data = log.Data
	return nil
}

// Snapshot always return a dummy snapshot and no error without doing
// anything.
func (f *testHookSyncFSM) Snapshot() (raft.FSMSnapshot, error) { return &testHookSyncSnapshot{}, nil }

// Restore always return a nil error without reading anything from
// the reader.
func (f *testHookSyncFSM) Restore(io.ReadCloser) error { return nil }

// fsmSnapshot a dummy implementation of an fsm snapshot.
type testHookSyncSnapshot struct{}

// Persist always return a nil error without writing anything
// to the sink.
func (s *testHookSyncSnapshot) Persist(sink raft.SnapshotSink) error { return nil }

// Release is a no-op.
func (s *testHookSyncSnapshot) Release() {}
