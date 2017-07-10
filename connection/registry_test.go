package connection_test

import (
	"database/sql"
	"path/filepath"

	"testing"

	"github.com/dqlite/dqlite/connection"
	"github.com/dqlite/go-sqlite3x"
	_ "github.com/mattn/go-sqlite3"
)

func TestRegistry_Add(t *testing.T) {
	registry := connection.NewTempRegistry()
	defer registry.Purge()

	dsn := connection.NewTestDSN()
	registry.Add("test", dsn)

	if registry.DSN("test") != dsn {
		t.Error("DSN mismatch")
	}
}

func TestRegistry_Duplicate(t *testing.T) {
	registry := connection.NewTempRegistry()
	defer registry.Purge()

	dsn := connection.NewTestDSN()
	registry.Add("test", dsn)

	defer panicCheck(t, "name 'test' is already registered")
	registry.Add("test", dsn)
}

func TestRegistry_OpenAndCloseFollower(t *testing.T) {
	registry := connection.NewTempRegistry()
	defer registry.Purge()

	registry.Add("test", connection.NewTestDSN())

	if err := registry.OpenFollower("test"); err != nil {
		t.Fatal(err)
	}
	if err := registry.CloseFollower("test"); err != nil {
		t.Fatal(err)
	}
}

func TestRegistry_OpenFollowerTwice(t *testing.T) {
	registry := connection.NewTempRegistry()
	defer registry.Purge()

	registry.Add("test", connection.NewTestDSN())

	if err := registry.OpenFollower("test"); err != nil {
		t.Fatal(err)
	}
	defer panicCheck(t, "follower connection for 'test' already open")
	registry.OpenFollower("test")
}

func TestRegistry_OpenFollowerFileError(t *testing.T) {
	registry := connection.NewTempRegistry()
	registry.Add("test", connection.NewTestDSN())
	registry.Purge()

	err := registry.OpenFollower("test")

	if err == nil {
		t.Fatal("expected error after opening leader with purged registry")
	}
	const want = "failed to open follower connection: unable to open database file"
	got := err.Error()
	if got != want {
		t.Errorf("expected\n%q\ngot\n%q", want, got)
	}
}

func TestRegistry_CloseFollowerWithoutOpeningItFirst(t *testing.T) {
	registry := connection.NewTempRegistry()
	defer registry.Purge()

	registry.Add("test", connection.NewTestDSN())

	defer panicCheck(t, "no follower connection for 'test'")
	registry.CloseFollower("test")
}

func TestRegistry_OpenAndCloseLeader(t *testing.T) {
	registry := connection.NewTempRegistryWithDatabase()
	defer registry.Purge()

	methods := sqlite3x.DirectReplicationMethods()
	conn, err := registry.OpenLeader("test", methods)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.CloseLeader(conn); err != nil {
		t.Fatal(err)
	}
}

func TestRegistry_OpenLeaderFileError(t *testing.T) {
	registry := connection.NewTempRegistryWithDatabase()
	registry.Purge()

	methods := sqlite3x.DirectReplicationMethods()
	conn, err := registry.OpenLeader("test", methods)
	if conn != nil {
		t.Fatal("expected nil conn return value after opening leader with purged registry")
	}
	if err == nil {
		t.Fatal("expected error after opening leader with purged registry")
	}
	const want = "failed to open leader connection: unable to open database file"
	got := err.Error()
	if got != want {
		t.Errorf("expected\n%q\ngot\n%q", want, got)
	}
}

func TestRegistry_CloseLeaderTwice(t *testing.T) {
	registry, conn := connection.NewTempRegistryWithLeader()
	defer registry.Purge()

	if err := registry.CloseLeader(conn); err != nil {
		t.Fatal(err)
	}

	const want = "attempt to close a connection that was not registered"
	defer func() {
		got := recover()
		if got != want {
			t.Errorf("expected\n%q\ngot\n%q", want, got)
		}
	}()
	registry.CloseLeader(conn)
}

func TestRegistry_AutoCheckpoint(t *testing.T) {
	registry := connection.NewTempRegistryWithDatabase()
	defer registry.Purge()

	registry.AutoCheckpoint(1)

	methods := sqlite3x.DirectReplicationMethods()
	conn, err := registry.OpenLeader("test", methods)
	if err != nil {
		t.Fatal(err)
	}
	size := sqlite3x.DatabaseSize(conn)
	if !(size > 0) {
		t.Fatal("database file has no bytes")
	}
	if _, err := conn.Exec("CREATE TABLE test (n INT)", nil); err != nil {
		t.Fatal(err)
	}
	if !(sqlite3x.DatabaseSize(conn) > size) {
		t.Fatal("checkpoint did not run, as database size after creating table is not larger than initial one")
	}
}

func TestRegistry_NameByLeader(t *testing.T) {
	registry, conn := connection.NewTempRegistryWithLeader()
	defer registry.Purge()

	if name := registry.NameByLeader(conn); name != "test" {
		t.Fatalf("got database name '%s', want 'test'", name)
	}
}

func TestRegistry_NameByLeaderPanicsIfPassedNonRegisteredConn(t *testing.T) {
	registry, conn := connection.NewTempRegistryWithLeader()
	defer registry.Purge()

	if err := registry.CloseLeader(conn); err != nil {
		t.Fatal(err)
	}

	defer panicCheck(t, "no database for the given connection")
	registry.NameByLeader(conn)
}

func TestRegistry_AllNames(t *testing.T) {
	registry := connection.NewTempRegistryWithDatabase()
	defer registry.Purge()

	names := registry.AllNames()
	if len(names) != 1 {
		t.Fatalf("got %d names, want 1", len(names))
	}
	if names[0] != "test" {
		t.Fatalf("got name '%s', want 'test'", names[0])
	}
}

func TestRegistry_Leaders(t *testing.T) {
	registry, conn := connection.NewTempRegistryWithLeader()
	defer registry.Purge()

	conns := registry.Leaders("test")
	if len(conns) != 1 {
		t.Fatalf("got %d conns, want 1", len(conns))
	}
	if conns[0] != conn {
		t.Fatalf("leader connection mismatch")
	}
}

func TestRegistry_Backup(t *testing.T) {
	registry, conn := connection.NewTempRegistryWithLeader()
	defer registry.Purge()

	if _, err := conn.Exec("BEGIN; CREATE TABLE foo (n INT); INSERT INTO foo VALUES(1); COMMIT", nil); err != nil {
		t.Fatal(err)
	}

	database, wal, err := registry.Backup("test")
	if err != nil {
		t.Fatal(err)
	}

	if err := registry.Restore("test", database, wal); err != nil {
		t.Fatal(err)
	}

	// Check that the data actually matches our source database.
	dsn := registry.DSN("test")
	db, err := sql.Open("sqlite3", filepath.Join(registry.Dir(), dsn.Filename))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query("SELECT * FROM foo", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("query returned empty result set")
	}
	var n int
	if err := rows.Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("got row value of %d instead of 1", n)
	}
}

func panicCheck(t *testing.T, want string) {
	got := recover()
	if got == "" {
		t.Errorf("no panic occurred")
		return
	}
	if got != want {
		t.Errorf("unexpected recover:\n want: %v\n  got: %v", want, got)
	}
}
