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
	"fmt"
	"testing"

	"github.com/CanonicalLtd/dqlite/internal/transaction"
	"github.com/CanonicalLtd/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

func TestRegistry_TxnAddLeader(t *testing.T) {
	registry, cleanup := newRegistry(t)
	defer cleanup()

	conn := &sqlite3.SQLiteConn{}
	registry.ConnLeaderAdd("test.db", conn)
	txn := registry.TxnLeaderAdd(conn, 1)

	if txn.ID() == 0 {
		t.Error("no ID assigned to transaction")
	}
	if txn.Conn() != conn {
		t.Error("transaction associated with wrong connection")
	}
	if !txn.IsLeader() {
		t.Error("transaction reported wrong replication mode")
	}
}

func TestRegistry_TxnLeaderAddPanicsIfPassedSameLeaderConnectionTwice(t *testing.T) {
	registry, cleanup := newRegistry(t)
	defer cleanup()

	conn := &sqlite3.SQLiteConn{}
	registry.ConnLeaderAdd("test.db", conn)
	txn := registry.TxnLeaderAdd(conn, 1)

	want := fmt.Sprintf("a transaction for this connection is already registered with ID %d", txn.ID())
	defer func() {
		got := recover()
		if got != want {
			t.Errorf("expected\n%q\ngot\n%q", want, got)
		}
	}()
	registry.TxnLeaderAdd(conn, 2)
}

func TestRegistry_TxnFollowerAdd(t *testing.T) {
	registry, cleanup := newRegistry(t)
	defer cleanup()

	conn := &sqlite3.SQLiteConn{}
	txn := registry.TxnFollowerAdd(conn, 123)

	if txn.ID() != 123 {
		t.Errorf("expected transaction ID 123, got %d", txn.ID())
	}
	if txn.Conn() != conn {
		t.Error("transaction associated with wrong connection")
	}
	if txn.IsLeader() {
		t.Error("transaction reported wrong replication mode")
	}
}

func TestRegistry_TxnByID(t *testing.T) {
	registry, cleanup := newRegistry(t)
	defer cleanup()

	conn := &sqlite3.SQLiteConn{}
	registry.ConnLeaderAdd("test.db", conn)
	txn := registry.TxnLeaderAdd(conn, 0)
	if registry.TxnByID(txn.ID()) != txn {
		t.Error("transactions instances don't match")
	}
}

func TestRegistry_TxnByIDNotFound(t *testing.T) {
	registry, cleanup := newRegistry(t)
	defer cleanup()

	if registry.TxnByID(123) != nil {
		t.Error("expected no transaction instance for non-existing ID")
	}
}

func TestRegistry_TxnByConn(t *testing.T) {
	registry, cleanup := newRegistry(t)
	defer cleanup()

	conn := &sqlite3.SQLiteConn{}
	registry.ConnLeaderAdd("test.db", conn)
	txn := registry.TxnLeaderAdd(conn, 0)
	if registry.TxnByConn(conn) != txn {
		t.Error("transactions instances don't match")
	}
}

func TestRegistry_TxnByConnFound(t *testing.T) {
	registry, cleanup := newRegistry(t)
	defer cleanup()

	conn := &sqlite3.SQLiteConn{}
	if registry.TxnByConn(conn) != nil {
		t.Error("expected no transaction instance for non-registered conn")
	}
}

func TestRegistry_TxnDel(t *testing.T) {
	registry, cleanup := newRegistry(t)
	defer cleanup()

	conn := &sqlite3.SQLiteConn{}
	registry.ConnLeaderAdd("test.db", conn)
	txn := registry.TxnLeaderAdd(conn, 0)

	registry.TxnDel(txn.ID())
	if registry.TxnByID(txn.ID()) != nil {
		t.Error("expected no transaction instance for unregistered ID")
	}
}

// Add and find IDs of recently committed transactions.
func TestRegistry_TxnCommitted(t *testing.T) {
	registry, cleanup := newRegistry(t)
	defer cleanup()

	for i := 1; i <= 10001; i++ {
		txn := transaction.New(nil, uint64(i))
		registry.TxnCommittedAdd(txn)
	}

	assert.False(t, registry.TxnCommittedFind(1))
	assert.True(t, registry.TxnCommittedFind(10001))
	assert.True(t, registry.TxnCommittedFind(2))
}

func TestRegistry_RemovePanicsIfPassedNonRegisteredID(t *testing.T) {
	registry, cleanup := newRegistry(t)
	defer cleanup()

	const want = "attempt to remove unregistered transaction 123"
	defer func() {
		got := recover()
		if got != want {
			t.Errorf("expected\n%q\ngot\n%q", want, got)
		}
	}()
	registry.TxnDel(123)
}
