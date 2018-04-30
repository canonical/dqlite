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

package replication_test

import (
	"database/sql/driver"
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/CanonicalLtd/dqlite/internal/protocol"
	"github.com/CanonicalLtd/dqlite/internal/registry"
	"github.com/CanonicalLtd/dqlite/internal/replication"
	"github.com/CanonicalLtd/go-sqlite3"
	"github.com/CanonicalLtd/raft-test"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Leadership is lost before the Begin hook fires, the same leader gets
// re-elected.
func TestIntegration_Begin_HookCheck_NotLeader_SameLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: begin(),
				2: insertOne().Expect(sqlite3.ErrIoErrNotLeader),
			},
			Depose: stageDepose{
				When: 2,
				Is:   committed,
			},
		},
		stageTwo{
			Elect: "0",
			Steps: []stageStep{
				begin(),
				insertOne(),
				commit(),
			},
			Inserted: 1,
		},
	}, assertEqualDatabaseFiles)
}

// Leadership is lost before the Begin hook fires. A different leader gets elected.
func TestIntegration_Begin_HookCheck_NotLeader_OtherLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: begin(),
				2: insertOne().Expect(sqlite3.ErrIoErrNotLeader),
			},
			Depose: stageDepose{
				When: 2,
				Is:   committed,
			},
		},
		stageTwo{
			Elect: "1",
			Steps: []stageStep{
				0: begin(),
				1: insertOne(),
				2: commit(),
			},
			Inserted: 1,
		},
	}, assertEqualDatabaseFiles)
}

// Leadership is lost before trying to apply the Open command to open follower
// connections. The same leader gets re-elected.
func TestIntegration_Begin_OpenFollower_NotLeader_SameLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: begin(),
				1: createTable().Expect(sqlite3.ErrIoErrNotLeader),
			},
			Depose: stageDepose{
				When: -1,
			},
		},
		stageTwo{
			Elect: "0",
			Steps: []stageStep{
				0: begin(),
				1: createTable(),
				2: insertOne(),
				3: commit(),
			},
			Inserted: 1,
		},
	}, noLeaderCheck(1), assertEqualDatabaseFiles)
}

// Leadership is lost before trying to apply the Open command to open follower
// connections. A different leader gets elected.
func TestIntegration_Begin_OpenFollower_NotLeader_OtherLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: begin(),
				1: createTable().Expect(sqlite3.ErrIoErrNotLeader),
			},
			Depose: stageDepose{
				When: -1,
			},
		},
		stageTwo{
			Elect: "1",
			Steps: []stageStep{
				0: begin(),
				1: createTable(),
				2: insertOne(),
				3: commit(),
			},
			Inserted: 1,
		},
	}, noLeaderCheck(1), assertEqualDatabaseFiles)
}

// Leadership is lost when committing the Open command to open follower
// connections. A quorum is not reached and the same leader gets re-elected.
func TestIntegration_Begin_OpenFollower_LeadershipLost_NoQuorum_SameLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: begin(),
				1: createTable().Expect(sqlite3.ErrIoErrLeadershipLost),
			},
			Depose: stageDepose{
				When: 1,
				Is:   enqueued,
			},
		},
		stageTwo{
			Elect: "0",
			Steps: []stageStep{
				0: begin(),
				1: createTable(),
				2: insertOne(),
				3: commit(),
			},
			Inserted: 1,
		},
	}, noLeaderCheck(1), assertEqualDatabaseFiles)
}

// Leadership is lost when committing the Open command to open follower
// connections. A quorum is not reached and a different leader gets elected.
func TestIntegration_Begin_OpenFollower_LeadershipLost_NoQuorum_OtherLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: begin(),
				1: createTable().Expect(sqlite3.ErrIoErrLeadershipLost),
			},
			Depose: stageDepose{
				When: 1,
				Is:   enqueued,
			},
		},
		stageTwo{
			Elect: "1",
			Steps: []stageStep{
				0: begin(),
				1: createTable(),
				2: insertOne(),
				3: commit(),
			},
			Inserted: 1,
		},
	}, noLeaderCheck(1), assertEqualDatabaseFiles)
}

// Leadership is lost when committing the Open command to open follower
// connections. A quorum is reached and the same leader get re-elected.
func TestIntegration_Begin_OpenFollower_LeadershipLost_Quorum_SameLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: begin(),
				1: createTable().Expect(sqlite3.ErrIoErrLeadershipLost),
			},
			Depose: stageDepose{
				When: 1,
				Is:   appended,
			},
		},
		stageTwo{
			Elect: "0",
			Steps: []stageStep{
				0: begin(),
				1: createTable(),
				2: insertOne(),
				3: commit(),
			},
			Inserted: 1,
		},
	}, noLeaderCheck(1), assertEqualDatabaseFiles)
}

// Leadership is lost when committing the Open command to open follower
// connections. A quorum is reached and a different leader gets elected.
func TestIntegration_Begin_OpenFollower_LeadershipLost_Quorum_OtherLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: begin(),
				1: createTable().Expect(sqlite3.ErrIoErrLeadershipLost),
			},
			Depose: stageDepose{
				When: 1,
				Is:   appended,
			},
		},
		stageTwo{
			Elect: "1",
			Steps: []stageStep{
				0: begin(),
				1: createTable(),
				2: insertOne(),
				3: commit(),
			},
			Inserted: 1,
		},
	}, noLeaderCheck(1), assertEqualDatabaseFiles)
}

// A transaction on another leader connection is in progress, the Begin hook
// returns ErrBusy when trying to execute a new transaction. It eventually
// times out if the in-progress transaction does not end.
func TestIntegration_Begin_BusyTimeout(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				// Start a transaction on conn 0.
				0: createTable(),
				1: begin(),
				2: insertOne(),
				// Start a concurrent transaction on conn 1, which will fail.
				3: begin().Conn(1),
				4: insertTwo().Conn(1).Expect(sqlite3.ErrNoExtended(sqlite3.ErrBusy)),
				// Commit the transaction on con 0.
				5: commit(),
			},
		},
		stageTwo{
			// Try again to start a write transaction on conn2 now that the one on
			// conn1 is done.
			Steps: []stageStep{
				0: rollback().Conn(1),
				1: begin().Conn(1),
				2: insertTwo().Conn(1),
				3: commit().Conn(1),
			},
			Inserted: 2,
		},
	}, assertEqualDatabaseFiles)
}

// A transaction on another leader connection is in progress, the Begin hook
// returns ErrBusy when trying to execute a new transaction. It eventually
// succeeds if the in-progress transaction ends.
func TestIntegration_Begin_BusyRetry(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			// Start a transaction on conn 0, and commit it
			// concurrently after a short while.
			Steps: []stageStep{
				0: createTable(),
				1: begin(),
				2: insertOne(),
				3: commit().Concurrent(25 * time.Millisecond),
			},
		},
		stageTwo{
			// Start a write transaction on conn2, which will be retried by the
			// busy handler until the transaction above commits.
			Steps: []stageStep{
				0: begin().Conn(1),
				1: insertTwo().Conn(1),
				2: commit().Conn(1),
			},
			Inserted: 2,
		},
	}, assertEqualDatabaseFiles)
}

// Trying to start two write transaction on the same connection fails.
func TestIntegration_Begin_TransactionSameConn(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			// Start a transaction on conn 0, and commit it
			// concurrently after a short while.
			Steps: []stageStep{
				0: createTable(),
				1: begin(),
				2: insertOne(),
				3: begin().Expect(sqlite3.ErrNoExtended(sqlite3.ErrError)),
				4: commit(),
			},
		},
		stageTwo{
			// Start a write transaction on conn2, which will be retried by the
			// busy handler until the transaction above commits.
			Steps:    []stageStep{},
			Inserted: 1,
		},
	}, assertEqualDatabaseFiles)
}

// The server is not the leader anymore when the Frames hook for a commit
// frames fires, the same server gets re-elected.
func TestIntegration_Frames_HookCheck_Commit_NotLeader_SameLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: begin(),
				2: insertOne(),
				3: commit().Expect(sqlite3.ErrIoErrNotLeader),
			},
			Depose: stageDepose{
				When: 2,
				Is:   committed,
			},
		},
		stageTwo{
			Elect: "0",
			Steps: []stageStep{
				0: begin(),
				1: insertOne(),
				2: commit(),
			},
			Inserted: 1,
		},
	}, noLeaderCheck(3), assertEqualDatabaseFiles)
}

// The server is not the leader anymore when the Frames for a commit frames
// hook fires, another server gets elected.
func TestIntegration_Frames_HookCheck_Commit_NotLeader_OtherLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: begin(),
				2: insertOne(),
				3: commit().Expect(sqlite3.ErrIoErrNotLeader),
			},
			Depose: stageDepose{
				When: 2,
				Is:   committed,
			},
		},
		stageTwo{
			Elect: "1",
			Steps: []stageStep{
				0: begin(),
				1: insertOne(),
				2: commit(),
			},
			Inserted: 1,
		},
	}, noLeaderCheck(3), assertEqualDatabaseFiles)
}

// The server is not the leader anymore when the Frames hook tries to apply the
// Frames command for a commit frames. The same server gets re-elected.
func TestIntegration_Frames_NotLeader_Commit_SameLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: begin(),
				2: insertOne(),
				3: commit().Expect(sqlite3.ErrIoErrNotLeader),
			},
			Depose: stageDepose{
				When: 2,
				Is:   committed,
			},
		},
		stageTwo{
			Elect: "0",
			Steps: []stageStep{
				0: begin(),
				1: insertOne(),
				2: commit(),
			},
			Inserted: 1,
		},
	}, noLeaderCheck(4), assertEqualDatabaseFiles)
}

// The server is not the leader anymore when the Frames hook tries to apply the
// Frames command for a commit frames. Another server gets elected.
func TestIntegration_Frames_NotLeader_Commit_OtherLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: begin(),
				2: insertOne(),
				3: commit().Expect(sqlite3.ErrIoErrNotLeader),
			},
			Depose: stageDepose{
				When: 2,
				Is:   committed,
			},
		},
		stageTwo{
			Elect: "1",
			Steps: []stageStep{
				0: begin(),
				1: insertOne(),
				2: commit(),
			},
			Inserted: 1,
		},
	}, noLeaderCheck(4), assertEqualDatabaseFiles)
}

// The node loses leadership when the Frames hook tries to apply the Frames
// command. The frames is a commit one, and no quorum is reached for the
// inflight Frames command. The same leader gets re-elected.
func TestIntegration_Frames_LeadershipLost_Commit_NoQuorum_SameLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: begin(),
				2: insertOne(),
				3: commit().Expect(sqlite3.ErrIoErrLeadershipLost),
			},
			Depose: stageDepose{
				When: 3,
				Is:   enqueued,
			},
		},
		stageTwo{
			Elect: "0",
			Steps: []stageStep{
				0: begin(),
				1: insertTwo(),
				2: commit(),
			},
			Inserted: 2,
		},
	}, assertEqualDatabaseFiles)
}

// The node loses leadership when the Frames hook tries to apply the Frames
// command. The frames is a commit one, and no quorum is reached for the
// inflight Frames command. A different leader gets elected.
func TestIntegration_Frames_LeadershipLost_Commit_NoQuorum_OtherLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: begin(),
				2: insertOne(),
				3: commit().Expect(sqlite3.ErrIoErrLeadershipLost),
			},
			Depose: stageDepose{
				When: 3,
				Is:   enqueued,
			},
		},
		stageTwo{
			Elect: "1",
			Steps: []stageStep{
				0: begin(),
				1: insertOne(),
				2: commit(),
			},
			Inserted: 1,
		},
	}, assertEqualDatabaseFiles)
}

// Leadership is lost when applying a commit Frames command, but a quorum is
// reached and the command actually gets committed. The same node that lost
// leadership gets re-elected.
func TestIntegration_Frames_LeadershipLost_Commit_Quorum_SameLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: begin(),
				2: insertOne(),
				3: commit().Expect(sqlite3.ErrIoErrLeadershipLost),
			},
			Depose: stageDepose{
				When: 3,
				Is:   appended,
			},
		},
		stageTwo{
			Elect: "0",
			Steps: []stageStep{
				0: begin(),
				1: insertTwo(),
				2: commit(),
			},
			Inserted: 2,
		},
	}, assertEqualDatabaseFiles)
}

// Leadership is lost when applying a commit Frames command, but a quorum is
// reached and the command actually gets committed. A different node than the
// one that lost leadership gets re-elected.
func TestIntegration_Frames_LeadershipLost_Commit_Quorum_OtherLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: begin(),
				2: insertOne(),
				3: commit().Expect(sqlite3.ErrIoErrLeadershipLost),
			},
			Depose: stageDepose{
				When: 3,
				Is:   appended,
			},
		},
		stageTwo{
			Elect: "1",
			Steps: []stageStep{
				0: begin(),
				1: insertTwo(),
				2: commit(),
			},
			Inserted: 2,
		},
	}, assertEqualDatabaseFiles)
}

// The server is not the leader anymore when the first Frames hook for a non-commit
// frames fires. The same leader gets re-elected.
func TestIntegration_Frames_HookCheck_First_NonCommit_NotLeader_SameLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: lowerCacheSize(),
				2: begin(),
				3: insertN(500).Expect(sqlite3.ErrIoErrNotLeader),
			},
			Depose: stageDepose{
				When: 2,
				Is:   committed,
			},
		},
		stageTwo{
			Elect: "0",
			Steps: []stageStep{
				0: begin(),
				1: insertN(500),
				2: commit(),
			},
			Inserted: 500,
		},
	}, assertEqualDatabaseFiles)
}

// The server is not the leader anymore when the first Frames hook for a non-commit
// frames fires. Another leader gets elected.
func TestIntegration_Frames_HookCheck_First_NonCommit_NotLeader_OtherLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: lowerCacheSize(),
				2: begin(),
				3: insertN(500).Expect(sqlite3.ErrIoErrNotLeader),
			},
			Depose: stageDepose{
				When: 2,
				Is:   committed,
			},
		},
		stageTwo{
			Elect: "1",
			Steps: []stageStep{
				0: begin(),
				1: insertN(500),
				2: commit(),
			},
			Inserted: 500,
		},
	}, assertEqualDatabaseFiles)
}

// The server is not the leader anymore when the second Frames hook for a non-commit
// frames fires. The same leader gets re-elected.
func TestIntegration_Frames_HookCheck_Second_NonCommit_NotLeader_SameLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: lowerCacheSize(),
				2: begin(),
				3: insertN(500),
				4: insertN(500).Expect(sqlite3.ErrIoErrNotLeader),
			},
			Depose: stageDepose{
				When: 3,
				Is:   committed,
			},
		},
		stageTwo{
			Elect: "0",
			Steps: []stageStep{
				0: begin(),
				1: insertN(500),
				2: commit(),
			},
			Inserted: 500,
		},
	}, assertEqualDatabaseFiles)
}

// The server is not the leader anymore when the second Frames hook for a non-commit
// frames fires. Another leader gets re-elected.
func TestIntegration_Frames_HookCheck_Second_NonCommit_NotLeader_OtherLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: lowerCacheSize(),
				2: begin(),
				3: insertN(500),
				4: insertN(500).Expect(sqlite3.ErrIoErrNotLeader),
			},
			Depose: stageDepose{
				When: 3,
				Is:   committed,
			},
		},
		stageTwo{
			Elect: "1",
			Steps: []stageStep{
				0: begin(),
				1: insertN(500),
				2: commit(),
			},
			Inserted: 500,
		},
	}, assertEqualDatabaseFiles)
}

// The server loses leadership when the first Frames hook for a non-commit frames
// fires. No quorum is reached for the inflight Frames command. The same leader
// gets re-elected.
func TestIntegration_Frames_First_NonCommit_LeadershipLost_NoQuorum_SameLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: lowerCacheSize(),
				2: begin(),
				3: insertN(500).Expect(sqlite3.ErrIoErrLeadershipLost),
			},
			Depose: stageDepose{
				When: 3,
				Is:   enqueued,
			},
		},
		stageTwo{
			Elect: "0",
			Steps: []stageStep{
				0: begin(),
				1: insertN(500),
				2: commit(),
			},
			Inserted: 500,
		},
	}, assertEqualDatabaseFiles)
}

// The server loses leadership when the first Frames hook for a non-commit frames
// fires. No quorum is reached for the inflight Frames command. Another leader
// gets elected.
func TestIntegration_Frames_First_NonCommit_LeadershipLost_NoQuorum_OtherLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: lowerCacheSize(),
				2: begin(),
				3: insertN(500).Expect(sqlite3.ErrIoErrLeadershipLost),
			},
			Depose: stageDepose{
				When: 3,
				Is:   enqueued,
			},
		},
		stageTwo{
			Elect: "1",
			Steps: []stageStep{
				0: begin(),
				1: insertN(500),
				2: commit(),
			},
			Inserted: 500,
		},
	}, assertEqualDatabaseFiles)
}

// The server loses leadership when the second Frames hook for a non-commit
// frames fires.  No quorum is reached for the inflight Frames command. The
// same leader gets re-elected.
func TestIntegration_Frames_Second_NonCommit_LeadershipLost_NoQuorum_SameLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: lowerCacheSize(),
				2: begin(),
				3: insertN(500),
				4: insertN(500).Expect(sqlite3.ErrIoErrLeadershipLost),
			},
			Depose: stageDepose{
				When: 4,
				Is:   enqueued,
			},
		},
		stageTwo{
			Elect: "0",
			Steps: []stageStep{
				0: begin(),
				1: insertN(500),
				2: commit(),
			},
			Inserted: 500,
		},
	}, assertEqualDatabaseFiles)
}

// The server loses leadership when the second Frames hook for a non-commit
// frames fires.  No quorum is reached for the inflight Frames command. Another
// leader gets elected.
func TestIntegration_Frames_Second_NonCommit_LeadershipLost_NoQuorum_OtherLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: lowerCacheSize(),
				2: begin(),
				3: insertN(500),
				4: insertN(500).Expect(sqlite3.ErrIoErrLeadershipLost),
			},
			Depose: stageDepose{
				When: 4,
				Is:   enqueued,
			},
		},
		stageTwo{
			Elect: "1",
			Steps: []stageStep{
				0: begin(),
				1: insertN(500),
				2: commit(),
			},
			Inserted: 500,
		},
	}, assertEqualDatabaseFiles)
}

// The server loses leadership when the first Frames hook for a non-commit frames
// fires. A quorum is reached for the inflight Frames command. The same leader
// gets elected.
func TestIntegration_Frames_First_NonCommit_LeadershipLost_Quorum_SameLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: lowerCacheSize(),
				2: begin(),
				3: insertN(500).Expect(sqlite3.ErrIoErrLeadershipLost),
			},
			Depose: stageDepose{
				When: 3,
				Is:   appended,
			},
		},
		stageTwo{
			Elect: "0",
			Steps: []stageStep{
				0: begin(),
				1: insertN(500),
				2: commit(),
			},
			Inserted: 500,
		},
	}, assertEqualDatabaseFiles)
}

// The server loses leadership when the first Frames hook for a non-commit frames
// fires. A quorum is reached for the inflight Frames command. Another leader
// gets elected.
func TestIntegration_Frames_First_NonCommit_LeadershipLost_Quorum_OtherLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: lowerCacheSize(),
				2: begin(),
				3: insertN(500).Expect(sqlite3.ErrIoErrLeadershipLost),
			},
			Depose: stageDepose{
				When: 3,
				Is:   appended,
			},
		},
		stageTwo{
			Elect: "1",
			Steps: []stageStep{
				0: begin(),
				1: insertN(500),
				2: commit(),
			},
			Inserted: 500,
		},
	}, assertEqualDatabaseFiles)
}

// The server loses leadership when the second Frames hook for a non-commit
// frames fires. A quorum is reached for the inflight Frames command. The same
// leader gets re-elected.
func TestIntegration_Frames_Second_NonCommit_LeadershipLost_Quorum_SameLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: lowerCacheSize(),
				2: begin(),
				3: insertN(500),
				4: insertN(500).Expect(sqlite3.ErrIoErrLeadershipLost),
			},
			Depose: stageDepose{
				When: 4,
				Is:   appended,
			},
		},
		stageTwo{
			Elect: "0",
			Steps: []stageStep{
				0: begin(),
				1: insertN(500),
				2: commit(),
			},
			Inserted: 500,
		},
	}, assertEqualDatabaseFiles)
}

// The server loses leadership when the second Frames hook for a non-commit
// frames fires. A quorum is reached for the inflight Frames command. Another
// leader gets elected.
func TestIntegration_Frames_Second_NonCommit_LeadershipLost_Quorum_OtherLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: lowerCacheSize(),
				2: begin(),
				3: insertN(500),
				4: insertN(500).Expect(sqlite3.ErrIoErrLeadershipLost),
			},
			Depose: stageDepose{
				When: 4,
				Is:   appended,
			},
		},
		stageTwo{
			Elect: "1",
			Steps: []stageStep{
				0: begin(),
				1: insertN(500),
				2: commit(),
			},
			Inserted: 500,
		},
	}, assertEqualDatabaseFiles)
}

// The server is not the leader anymore when the Frames hook for a commit
// frames fires. A non-commit frame command were committed before this last
// one. The same leader gets re-elected.
func TestIntegration_Frames_HookCheck_Commit_After_NonCommit_NotLeader_SameLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: lowerCacheSize(),
				2: begin(),
				3: insertN(500),
				4: insertOneAfterN(500),
				5: commit().Expect(sqlite3.ErrIoErrNotLeader),
			},
			Depose: stageDepose{
				When: 3,
				Is:   committed,
			},
		},
		stageTwo{
			Elect: "0",
			Steps: []stageStep{
				0: begin(),
				1: insertN(500),
				2: commit(),
			},
			Inserted: 500,
		},
	}, assertEqualDatabaseFiles)
}

// The server is not the leader anymore when the Frames hook for a commit
// frames fires. A non-commit frame command were committed before this last
// one. A different leader gets elected.
func TestIntegration_Frames_HookCheck_Commit_After_NonCommit_NotLeader_OtherLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: lowerCacheSize(),
				2: begin(),
				3: insertN(500),
				4: insertOneAfterN(500),
				5: commit().Expect(sqlite3.ErrIoErrNotLeader),
			},
			Depose: stageDepose{
				When: 3,
				Is:   committed,
			},
		},
		stageTwo{
			Elect: "1",
			Steps: []stageStep{
				0: begin(),
				1: insertN(500),
				2: commit(),
			},
			Inserted: 500,
		},
	}, assertEqualDatabaseFiles)
}

// The server loses leadership when the Frames hook for a commit frames
// fires. Some non-commit frames were committed before this last one. No quorum
// is reached for the lost frames command. The same leader gets re-elected.
func TestIntegration_Frames_Commit_After_NonCommit_LeadershipLost_NoQuorum_SameLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: lowerCacheSize(),
				2: begin(),
				3: insertN(500),
				4: insertOneAfterN(500),
				5: commit().Expect(sqlite3.ErrIoErrLeadershipLost),
			},
			Depose: stageDepose{
				When: 4,
				Is:   enqueued,
			},
		},
		stageTwo{
			Elect: "0",
			Steps: []stageStep{
				0: begin(),
				1: insertOneAfterN(501),
				2: commit(),
			},
			Inserted: 502,
		},
	}, assertEqualDatabaseFiles)
}

// The server loses leadership when the Frames hook for a commit frames
// fires. Some non-commit frames were committed before this last one. No quorum
// is reached for the lost frames command. A different leader gets elected.
func TestIntegration_Frames_Commit_After_NonCommit_LeadershipLost_NoQuorum_OtherLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: lowerCacheSize(),
				2: begin(),
				3: insertN(500),
				4: insertOneAfterN(500),
				5: commit().Expect(sqlite3.ErrIoErrLeadershipLost),
			},
			Depose: stageDepose{
				When: 4,
				Is:   enqueued,
			},
		},
		stageTwo{
			Elect: "1",
			Steps: []stageStep{
				0: begin(),
				1: insertN(500),
				2: commit(),
			},
			Inserted: 500,
		},
	}, assertEqualDatabaseFiles)
}

// The server loses leadership when the Frames hook for a commit frames
// fires. Some non-commit frames were committed before this last one. A quorum
// is reached for the lost frames command. The same leader gets re-elected.
func TestIntegration_Frames_Commit_After_NonCommit_LeadershipLost_Quorum_SameLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: lowerCacheSize(),
				2: begin(),
				3: insertN(500),
				4: insertOneAfterN(500),
				5: commit().Expect(sqlite3.ErrIoErrLeadershipLost),
			},
			Depose: stageDepose{
				When: 4,
				Is:   appended,
			},
		},
		stageTwo{
			Elect: "0",
			Steps: []stageStep{
				0: begin(),
				1: insertOneAfterN(501),
				2: commit(),
			},
			Inserted: 502,
		},
	}, assertEqualDatabaseFiles)
}

// The server loses leadership when the Frames hook for a commit frames
// fires. Some non-commit frames were committed before this last one. A quorum
// is reached for the lost frames command. A different server gets elected.
func TestIntegration_Frames_Commit_After_NonCommit_LeadershipLost_Quorum_OtherLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: lowerCacheSize(),
				2: begin(),
				3: insertN(500),
				4: insertOneAfterN(500),
				5: commit().Expect(sqlite3.ErrIoErrLeadershipLost),
			},
			Depose: stageDepose{
				When: 4,
				Is:   appended,
			},
		},
		stageTwo{
			Elect: "1",
			Steps: []stageStep{
				0: begin(),
				1: insertOneAfterN(501),
				2: commit(),
			},
			Inserted: 502,
		},
	}, assertEqualDatabaseFiles)
}

// The server is not the leader anymore when the Undo hook fires and one or
// more frames commands were already committed. The same leader gets
// re-elected.
func TestIntegration_Undo_HookCheck_NotLeader_SameLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: lowerCacheSize(),
				2: begin(),
				3: insertN(500),
				4: rollback(),
			},
			Depose: stageDepose{
				When: 3,
				Is:   committed,
			},
		},
		stageTwo{
			Elect: "0",
			Steps: []stageStep{
				0: begin(),
				1: insertN(500),
				2: commit(),
			},
			Inserted: 500,
		},
	}, assertEqualDatabaseFiles)
}

// The server is not the leader anymore when the Undo hook fires and one or
// more frames commands were already committed. A different leader gets
// elected.
func TestIntegration_Undo_HookCheck_NotLeader_OtherLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: lowerCacheSize(),
				2: begin(),
				3: insertN(500),
				4: rollback(),
			},
			Depose: stageDepose{
				When: 3,
				Is:   committed,
			},
		},
		stageTwo{
			Elect: "1",
			Steps: []stageStep{
				0: begin(),
				1: insertN(500),
				2: commit(),
			},
			Inserted: 500,
		},
	}, assertEqualDatabaseFiles)
}

// The node is not the leader anymore when the Undo hook tries to apply the
// Undo command and one or more frames commands were already committed. The
// same leader gets re-elected.
func TestIntegration_Undo_NotLeader_SameLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: lowerCacheSize(),
				2: begin(),
				3: insertN(500),
				4: rollback(),
			},
			Depose: stageDepose{
				When: 3,
				Is:   committed,
			},
		},
		stageTwo{
			Elect: "0",
			Steps: []stageStep{
				0: begin(),
				1: insertN(500),
				2: commit(),
			},
			Inserted: 500,
		},
	}, noLeaderCheck(5), assertEqualDatabaseFiles)
}

// The node is not the leader anymore when the Undo hook tries to apply the
// Undo command and one or more frames commands were already committed. A
// different leader gets elected.
func TestIntegration_Undo_NotLeader_OtherLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: lowerCacheSize(),
				2: begin(),
				3: insertN(500),
				4: rollback(),
			},
			Depose: stageDepose{
				When: 3,
				Is:   committed,
			},
		},
		stageTwo{
			Elect: "1",
			Steps: []stageStep{
				0: begin(),
				1: insertN(500),
				2: commit(),
			},
			Inserted: 500,
		},
	}, noLeaderCheck(5), assertEqualDatabaseFiles)
}

// The node loses leadership when the Undo hook tries to apply the Undo command
// and one or more frames commands were already committed. No quorum is reached
// for the inflight Undo command. The same leader gets re-elected.
func TestIntegration_Undo_LeadershipLost_NoQuorum_SameLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: lowerCacheSize(),
				2: begin(),
				3: insertN(500),
				4: rollback(),
			},
			Depose: stageDepose{
				When: 4,
				Is:   enqueued,
			},
		},
		stageTwo{
			Elect: "0",
			Steps: []stageStep{
				0: begin(),
				1: insertN(500),
				2: commit(),
			},
			Inserted: 500,
		},
	}, assertEqualDatabaseFiles)
}

// The node loses leadership when the Undo hook tries to apply the Undo command
// and one or more frames commands were already committed. No quorum is reached
// for the inflight Undo command. A different leader gets elected.
func TestIntegration_Undo_LeadershipLost_NoQuorum_OtherLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: lowerCacheSize(),
				2: begin(),
				3: insertN(500),
				4: rollback(),
			},
			Depose: stageDepose{
				When: 4,
				Is:   enqueued,
			},
		},
		stageTwo{
			Elect: "1",
			Steps: []stageStep{
				0: begin(),
				1: insertN(500),
				2: commit(),
			},
			Inserted: 500,
		},
	}, assertEqualDatabaseFiles)
}

// Leadership is lost when applying the Undo command and one or more frames
// commands were already committed. A quorum is reached for the inflight
// command. The same node that lost leadership gets re-elected.
func TestIntegration_Undo_LeadershipLost_Quorum_SameLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: lowerCacheSize(),
				2: begin(),
				3: insertN(500),
				4: rollback(),
			},
			Depose: stageDepose{
				When: 4,
				Is:   appended,
			},
		},
		stageTwo{
			Elect: "0",
			Steps: []stageStep{
				0: begin(),
				1: insertN(500),
				2: commit(),
			},
			Inserted: 500,
		},
	}, assertEqualDatabaseFiles)
}

// Leadership is lost when applying the Undo command and one or more frames
// commands were already committed. A quorum is reached for the inflight
// command. A different not gets elected.
func TestIntegration_Undo_LeadershipLost_Quorum_OtherLeader(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: lowerCacheSize(),
				2: begin(),
				3: insertN(500),
				4: rollback(),
			},
			Depose: stageDepose{
				When: 4,
				Is:   appended,
			},
		},
		stageTwo{
			Elect: "1",
			Steps: []stageStep{
				0: begin(),
				1: insertN(500),
				2: commit(),
			},
			Inserted: 500,
		},
	}, assertEqualDatabaseFiles)
}

// Test a successful rollback.
func TestIntegration_Undo(t *testing.T) {
	runScenario(t, scenario{
		stageOne{
			Steps: []stageStep{
				0: createTable(),
				1: lowerCacheSize(),
				2: begin(),
				3: insertN(500),
				4: rollback(),
			},
		},
		stageTwo{
			Steps: []stageStep{
				0: begin(),
				1: insertOne(),
				2: commit(),
			},
			Inserted: 1,
		},
	}, assertEqualDatabaseFiles)
}

// Exercise creating and restoring snapshots.
func TestIntegration_Snapshot(t *testing.T) {
	conns, control, cleanup := newCluster(t)
	defer cleanup()

	term := control.Elect("0")

	// Get a leader connection on the leader node and create a table.
	conn := conns["0"][0]
	_, err := conn.Exec("CREATE TABLE test (n INT, UNIQUE(n))", nil)
	require.NoError(t, err)

	control.Barrier()

	// Disconnect the follower immediately, so it will be forced to use the
	// snapshot at reconnection.
	term.Disconnect("1")

	// Run a few of WAL-writing queries.
	for i := 1; i < 4; i++ {
		_, err := conn.Exec(fmt.Sprintf("INSERT INTO test(n) VALUES (%d)", i), nil)
		require.NoError(t, err)
	}

	// Take a snapshot on the leader after this first batch of queries.
	term.Snapshot("0")

	// Make sure snapshot is taken by the leader.
	control.Barrier()
	assert.Equal(t, uint64(1), control.Snapshots("0"))

	term.Reconnect("1")

	// Run an extra query to proof that the follower with the restored
	// snapshot is still functional.
	_, err = conn.Exec("INSERT INTO test VALUES(4)", nil)
	require.NoError(t, err)

	// The follower will now have to restore the snapshot.
	control.Barrier()

	// Figure out the database name
	rows, err := conn.Query("SELECT file FROM pragma_database_list WHERE name='main'", nil)
	require.NoError(t, err)
	values := make([]driver.Value, 1)
	require.NoError(t, rows.Next(values))
	require.NoError(t, rows.Close())
	path := string(values[0].([]byte))

	// Open a new connection since the database file has been replaced.
	methods := sqlite3.NoopReplicationMethods()
	conn, err = connection.OpenLeader(path, methods)
	rows, err = conn.Query("SELECT n FROM test", nil)
	require.NoError(t, err)
	defer rows.Close()
	values = make([]driver.Value, 1)
	require.NoError(t, rows.Next(values))
	assert.Equal(t, int64(1), values[0].(int64))
	require.NoError(t, rows.Next(values))
	assert.Equal(t, int64(2), values[0].(int64))
	require.NoError(t, rows.Next(values))
	assert.Equal(t, int64(3), values[0].(int64))
	require.NoError(t, rows.Next(values))
	assert.Equal(t, int64(4), values[0].(int64))

}

func runScenario(t *testing.T, s scenario, options ...clusterOption) {
	t.Helper()

	conns, control, cleanup := newCluster(t, options...)
	defer cleanup()

	// Stage one.
	term := control.Elect("0")
	if s.StageOne.Depose.When < 0 {
		// Depose immediately
		control.Depose()
	}
	if s.StageOne.Depose.When > 0 {
		dispatch := term.When().Command(uint64(s.StageOne.Depose.When))
		switch s.StageOne.Depose.Is {
		case enqueued:
			dispatch.Enqueued().Depose()
		case appended:
			dispatch.Appended().Depose()
		case committed:
			dispatch.Committed().Depose()
		}
	}

	for i, step := range s.StageOne.Steps {
		conn := conns["0"][step.conn]
		if step.concurrent {
			go func() {
				time.Sleep(rafttest.Duration(step.delay))
				step.f(t, 1, i, conn)
			}()
			continue
		}
		err := step.f(t, 1, i, conn)
		if step.errno != 0 {
			sqliteErr, ok := err.(sqlite3.Error)
			if !ok {
				t.Fatalf("stage 1: step %d: expected sqlite3.Error, but got: %v", i, err)
			}
			expect := step.errno
			got := sqliteErr.ExtendedCode
			if expect != got {
				t.Fatalf("stage 1: step %d: expected code %d, but got %d:", i, expect, got)
			}
		} else {
			require.NoError(t, err)
		}
	}

	// Stage two.
	leader := s.StageTwo.Elect
	if leader != "" {
		control.Elect(leader)
		control.Barrier()
	} else {
		leader = "0"
	}

	for i, step := range s.StageTwo.Steps {
		conn := conns[leader][step.conn]
		require.NoError(t, step.f(t, 2, i, conn))
	}
	control.Barrier()

	selectN(t, conns["0"][0], s.StageTwo.Inserted)
	selectN(t, conns["1"][0], s.StageTwo.Inserted)
	selectN(t, conns["2"][0], s.StageTwo.Inserted)
}

// Select N rows from the test table and check that their value is progressing
// and no other rows are there.
func selectN(t *testing.T, conn *sqlite3.SQLiteConn, n int) {
	t.Helper()
	rows, err := conn.Query("SELECT n FROM test ORDER BY n", nil)
	require.NoError(t, err)
	values := make([]driver.Value, 1)
	for i := 0; i < n; i++ {
		require.NoError(t, rows.Next(values))
		assert.Equal(t, int64(i+1), values[0])
	}
	assert.Equal(t, io.EOF, rows.Next(values))
	require.NoError(t, rows.Close())
}

type scenario struct {
	StageOne stageOne
	StageTwo stageTwo
}

type stageStep struct {
	conn       int
	f          func(t *testing.T, stage int, step int, conn *sqlite3.SQLiteConn) error
	errno      sqlite3.ErrNoExtended
	concurrent bool
	delay      time.Duration
}

func (s stageStep) Conn(conn int) stageStep {
	s.conn = conn
	return s
}

func (s stageStep) Expect(errno sqlite3.ErrNoExtended) stageStep {
	s.errno = errno
	return s
}

func (s stageStep) Concurrent(delay time.Duration) stageStep {
	s.concurrent = true
	s.delay = delay
	return s
}

type stageOne struct {
	Steps  []stageStep
	Depose stageDepose
}

type stageDepose struct {
	When int
	Is   int
}

const (
	enqueued int = iota
	appended
	committed
)

type stageTwo struct {
	Elect    raft.ServerID
	Steps    []stageStep
	Inserted int
}

// Option to create a test table at setup time.
func createTable() stageStep {
	f := func(t *testing.T, stage int, step int, conn *sqlite3.SQLiteConn) error {
		t.Helper()
		t.Logf("stage: %d: step %d: create table", stage, step)

		_, err := conn.Exec("CREATE TABLE test (n INT, UNIQUE(n))", nil)
		return err
	}
	return stageStep{f: f}
}

// Option that lowers SQLite's page cache size to force it to write uncommitted
// dirty pages to the WAL.
func lowerCacheSize() stageStep {
	f := func(t *testing.T, stage int, step int, conn *sqlite3.SQLiteConn) error {
		t.Helper()
		t.Logf("stage: %d: step %d: lower cache size", stage, step)

		_, err := conn.Exec("PRAGMA page_size = 1024", nil)
		require.NoError(t, err) // SQLite should never return an error here
		_, err = conn.Exec("PRAGMA cache_size = 1", nil)
		require.NoError(t, err) // SQLite should never return an error here

		return nil
	}
	return stageStep{f: f}
}

func begin() stageStep {
	f := func(t *testing.T, stage int, step int, conn *sqlite3.SQLiteConn) error {
		t.Helper()
		t.Logf("stage: %d: step %d: begin", stage, step)

		_, err := conn.Exec("BEGIN", nil)
		return err
	}
	return stageStep{f: f}
}

// Inserts the given number of rows in the test table.
func insertN(n int) stageStep {
	f := func(t *testing.T, stage int, step int, conn *sqlite3.SQLiteConn) error {
		t.Helper()
		t.Logf("stage: %d: step %d: insert %d rows", stage, step, n)

		values := ""
		for i := 0; i < n; i++ {
			values += fmt.Sprintf(" (%d),", i+1)
		}
		values = values[:len(values)-1]
		_, err := conn.Exec(fmt.Sprintf("INSERT INTO test(n) VALUES %s", values), nil)
		return err
	}
	return stageStep{f: f}
}

// Inserts a single row into the test table with value 1.
func insertOne() stageStep {
	f := func(t *testing.T, stage int, step int, conn *sqlite3.SQLiteConn) error {
		t.Helper()
		t.Logf("stage: %d: step %d: insert one", stage, step)

		_, err := conn.Exec("INSERT INTO test(n) VALUES (1)", nil)
		return err
	}
	return stageStep{f: f}
}

// Inserts a single row into the test table with value 2.
func insertTwo() stageStep {
	f := func(t *testing.T, stage int, step int, conn *sqlite3.SQLiteConn) error {
		t.Helper()
		t.Logf("stage: %d: step %d: insert two", stage, step)

		_, err := conn.Exec("INSERT INTO test(n) VALUES (2)", nil)
		return err
	}
	return stageStep{f: f}
}

// Inserts the one more number into the test table, after that N have been
// inserted already
func insertOneAfterN(n int) stageStep {
	f := func(t *testing.T, stage int, step int, conn *sqlite3.SQLiteConn) error {
		t.Helper()
		t.Logf("stage: %d: step %d: insert one row after %d rows", stage, step, n)

		_, err := conn.Exec(fmt.Sprintf("INSERT INTO test(n) VALUES (%d)", n+1), nil)
		return err
	}
	return stageStep{f: f}
}

func commit() stageStep {
	f := func(t *testing.T, stage int, step int, conn *sqlite3.SQLiteConn) error {
		t.Helper()
		t.Logf("stage: %d: step %d: commit", stage, step)

		_, err := conn.Exec("COMMIT", nil)
		return err
	}
	return stageStep{f: f}
}

func rollback() stageStep {
	f := func(t *testing.T, stage int, step int, conn *sqlite3.SQLiteConn) error {
		t.Helper()
		t.Logf("stage: %d: step %d: rollback", stage, step)

		_, err := conn.Exec("ROLLBACK", nil)
		return err
	}
	return stageStep{f: f}
}

// Create a new test cluster with 3 nodes, each with its own FSM, Methods and
// two connections opened in leader mode.
func newCluster(t *testing.T, opts ...clusterOption) (clusterConns, *rafttest.Control, func()) {
	t.Helper()

	// Registries and FSMs
	cleanups := []func(){}
	registries := make([]*registry.Registry, 3)
	dirs := make([]string, 3)
	fsms := make([]raft.FSM, 3)
	for i := range fsms {
		dir, cleanup := newDir(t)
		cleanups = append(cleanups, cleanup)

		registries[i] = registry.New(dir)
		registries[i].Testing(t, i)

		dirs[i] = dir
		fsms[i] = replication.NewFSM(registries[i])
	}

	// Use an actual boltdb store, so the Log.Data bytes will be copied
	// when fetching a log from the store. This is necessary for the
	// Registry.HookSync mechanism to work properly.
	stores := make([]raft.LogStore, 3)
	for i := range stores {
		path := filepath.Join(registries[i].Dir(), "bolt.db")
		store, err := raftboltdb.NewBoltStore(path)
		require.NoError(t, err)

		stores[i] = store
	}

	// Raft instances.
	store := rafttest.LogStore(func(i int) raft.LogStore { return stores[i] })
	rafts, control := rafttest.Cluster(t, fsms, store, rafttest.DiscardLogger())

	// Methods and connections.
	methods := make([]*replication.Methods, 3)
	conns := map[raft.ServerID][2]*sqlite3.SQLiteConn{}
	for i := range methods {
		id := raft.ServerID(strconv.Itoa(i))
		methods[i] = replication.NewMethods(registries[i], rafts[id])

		dir := dirs[i]
		timeout := rafttest.Duration(100*time.Millisecond).Nanoseconds() / (1000 * 1000)
		path := filepath.Join(dir, fmt.Sprintf("test.db?_busy_timeout=%d", timeout))

		conn1, err := connection.OpenLeader(path, methods[i])
		require.NoError(t, err)
		methods[i].Registry().ConnLeaderAdd("test.db", conn1)

		conn2, err := connection.OpenLeader(path, methods[i])
		require.NoError(t, err)
		methods[i].Registry().ConnLeaderAdd("test.db", conn2)

		conns[id] = [2]*sqlite3.SQLiteConn{conn1, conn2}
	}

	options := defaultClusterOptions()
	for _, o := range opts {
		o(options)
	}

	args := &clusterTweakArgs{
		Dirs:    dirs,
		FSMs:    fsms,
		Control: control,
		Methods: methods,
		Conns:   conns,
	}

	for _, f := range options.SetupFuncs {
		f(t, args)
	}

	cleanup := func() {
		for i := range conns {
			require.NoError(t, conns[i][0].Close())
			require.NoError(t, conns[i][1].Close())
		}
		control.Close()
		if !t.Failed() {
			for _, f := range options.CleanupFuncs {
				f(t, args)
			}
		}
		for i := range cleanups {
			cleanups[i]()
		}
	}

	return conns, control, cleanup
}

// Leader SQLite connections setup by newCluster. Each server has two of them.
type clusterConns map[raft.ServerID][2]*sqlite3.SQLiteConn

// A function that tweaks the cluster setup or cleanup.
type clusterTweakFunc func(*testing.T, *clusterTweakArgs)

// Hold objects to pass to cluster tweak functions.
type clusterTweakArgs struct {
	Dirs    []string
	FSMs    []raft.FSM
	Control *rafttest.Control
	Methods []*replication.Methods
	Conns   clusterConns
}

// Expose various internal cluster parameters that tests can tweak with
// clusterOption functions.
type clusterOptions struct {
	SetupFuncs   []clusterTweakFunc // Tweaks to run at setup time.
	CleanupFuncs []clusterTweakFunc // Assertions to run at cleanup time.
}

// Default cluster options.
func defaultClusterOptions() *clusterOptions {
	return &clusterOptions{
		SetupFuncs:   make([]clusterTweakFunc, 0),
		CleanupFuncs: make([]clusterTweakFunc, 0),
	}
}

type clusterOption func(*clusterOptions)

// Option to assert that all database files have the exact same content at
// cleanup time.
func assertEqualDatabaseFiles(o *clusterOptions) {
	f := func(t *testing.T, args *clusterTweakArgs) {
		t.Helper()

		// We need to checkpoint the databases before comparing them, because
		// each WAL file has its own magic seed.
		for _, fsm := range args.FSMs {
			checkpointDatabase(t, fsm)
		}

		data1 := readDatabaseFile(t, args.Dirs[0])
		data2 := readDatabaseFile(t, args.Dirs[1])
		data3 := readDatabaseFile(t, args.Dirs[2])

		assert.Equal(t, data1, data2)
		assert.Equal(t, data1, data3)
	}
	o.CleanupFuncs = append(o.CleanupFuncs, f)
}

// Option that disable leadership checks in the methods hooks.
//
// Each time a method hook is invoked it N is decremented by one. When it
// reaches zero leadership checks will run again in follow up method hook
// calls.
func noLeaderCheck(n int) clusterOption {
	return func(o *clusterOptions) {
		f := func(t *testing.T, args *clusterTweakArgs) {
			for _, methods := range args.Methods {
				methods.NoLeaderCheck(n)
			}
		}
		o.SetupFuncs = append(o.SetupFuncs, f)
	}
}

// Apply a checkpoint command against the given fsm.
func checkpointDatabase(t *testing.T, fsm raft.FSM) {
	t.Helper()

	cmd := protocol.NewCheckpoint("test.db")
	data, err := protocol.MarshalCommand(cmd)
	require.NoError(t, err)
	log := &raft.Log{Data: data}
	fsm.Apply(log)
}

// Read the test database file in the given directory.
func readDatabaseFile(t *testing.T, dir string) []byte {
	t.Helper()

	path := filepath.Join(dir, "test.db")
	data, err := ioutil.ReadFile(path)
	require.NoError(t, err)
	return data
}
