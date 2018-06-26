package connection_test

import (
	"testing"

	"github.com/CanonicalLtd/dqlite/internal/bindings"
	"github.com/CanonicalLtd/dqlite/internal/connection"
)

// If the given sqlite DSN string is compatible with SQLite, no error
// will be returned.
func TestParseURI_Valid(t *testing.T) {
	cases := []string{
		"test.db",
		"test.db?_foreign_keys=1&mode=rwc",
		"file:test.db",
		"file:test.db?_foreign_keys=1",
	}
	for _, input := range cases {
		t.Run(input, func(t *testing.T) {
			_, _, err := connection.ParseURI(input)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

// The query string gets converted into flags.
func TestParseURI_Query(t *testing.T) {
	cases := map[string]uint64{
		"test.db":         bindings.OpenReadWrite | bindings.OpenCreate,
		"file:test.db?":   bindings.OpenReadWrite | bindings.OpenCreate,
		"test.db?mode=rw": bindings.OpenReadWrite,
		"test.db?mode=ro": bindings.OpenReadOnly,
	}
	for uri, expected := range cases {
		t.Run(uri, func(t *testing.T) {
			_, flags, err := connection.ParseURI(uri)
			if err != nil {
				t.Fatal(err)
			}
			if flags != expected {
				t.Fatalf("Got flags %d, expected %d", flags, expected)
			}
		})
	}
}

// The filename string gets returned as first return value.
func TestParseURI_Filename(t *testing.T) {
	cases := map[string]string{
		"test.db":               "test.db",
		"file:test.db?":         "test.db",
		"test.db?mode=rwc":      "test.db",
		"file:test.db?mode=rwc": "test.db",
	}
	for uri, expected := range cases {
		t.Run(uri, func(t *testing.T) {
			filename, _, err := connection.ParseURI(uri)
			if err != nil {
				t.Fatal(err)
			}
			if filename != expected {
				t.Fatalf("Got filename %s, expected %s", filename, expected)
			}
		})
	}
}

// If the given sqlite DSN string is not compatible with SQLite, an
// error will be returned.
func TestParseURI_Invalid(t *testing.T) {
	cases := map[string]string{
		":memory:":            "memory database not supported",
		"test.db?mode=memory": "memory database not supported",
		"test.db?%gh&%ij":     "invalid URL escape \"%gh\"",
		"file:///test.db":     "directory segments are invalid",
		"/foo/test.db":        "directory segments are invalid",
		"./bar/test.db":       "directory segments are invalid",
	}
	for uri, error := range cases {
		t.Run(uri, func(t *testing.T) {
			_, _, err := connection.ParseURI(uri)
			if err == nil {
				t.Fatalf("ParseURI didn't fail with %s", uri)
			}
			if err.Error() != error {
				t.Fatalf("Expected ParseURI to fail with %s, got %s instead", error, err.Error())
			}
		})
	}
}
