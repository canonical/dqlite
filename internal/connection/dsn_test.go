package connection_test

import (
	"testing"

	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/mpvl/subtest"
)

// If the given sqlite DSN string is compatible with SQLite, no error
// will be returned.
func TestNewDSN_Valid(t *testing.T) {
	cases := []string{
		"test.db",
		"test.db?_foreign_keys=1&mode=rwc",
		"file:test.db",
		"file:test.db?_foreign_keys=1",
	}
	for _, input := range cases {
		subtest.Run(t, input, func(t *testing.T) {
			_, err := connection.NewDSN(input)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

// The query string gets stored in the Query attribute.
func TestNewDSN_Query(t *testing.T) {
	cases := map[string]string{
		"test.db":                          "",
		"file:test.db?":                    "",
		"test.db?mode=rwc":                 "mode=rwc",
		"test.db?_foreign_keys=1&mode=rwc": "_foreign_keys=1&mode=rwc",
	}
	for input, query := range cases {
		subtest.Run(t, input, func(t *testing.T) {
			dsn, err := connection.NewDSN(input)
			if err != nil {
				t.Fatal(err)
			}
			if dsn.Query != query {
				t.Fatalf("Got query %s, expected %s", dsn.Query, query)
			}
		})
	}
}

// The filename string gets stored in the Filename attribute.
func TestNewDSN_Filename(t *testing.T) {
	cases := map[string]string{
		"test.db":               "test.db",
		"file:test.db?":         "test.db",
		"test.db?mode=rwc":      "test.db",
		"file:test.db?mode=rwc": "test.db",
	}
	for input, filename := range cases {
		subtest.Run(t, input, func(t *testing.T) {
			dsn, err := connection.NewDSN(input)
			if err != nil {
				t.Fatal(err)
			}
			if dsn.Filename != filename {
				t.Fatalf("Got filename %s, expected %s", dsn.Filename, filename)
			}
		})
	}
}

// If the given sqlite DSN string is not compatible with SQLite, an
// error will be returned.
func TestNewDSN_Invalid(t *testing.T) {
	cases := map[string]string{
		":memory:":            "can't replicate a memory database",
		"test.db?mode=memory": "can't replicate a memory database",
		"test.db?%gh&%ij":     "invalid URL escape \"%gh\"",
		"file:///test.db":     "directory segments are invalid",
		"/foo/test.db":        "directory segments are invalid",
		"./bar/test.db":       "directory segments are invalid",
	}
	for input, error := range cases {
		subtest.Run(t, input, func(t *testing.T) {
			_, err := connection.NewDSN(input)
			if err == nil {
				t.Fatalf("NewDSN didn't fail with %s", input)
			}
			if err.Error() != error {
				t.Fatalf("Expected NewDSN to fail with %s, got %s instead", error, err.Error())
			}
		})
	}
}

func TestDSN_Encode(t *testing.T) {
	cases := map[string]string{
		"test.db":        "test.db",
		"test.db?mode=r": "test.db?mode=r",
	}
	for input, output := range cases {
		subtest.Run(t, input, func(t *testing.T) {
			dsn, err := connection.NewDSN(input)
			if err != nil {
				t.Fatal(err)
			}
			got := dsn.Encode()
			if got != output {
				t.Fatalf("String() should have returned %s, got %s instead", output, got)
			}
		})
	}
}
