package connection_test

import (
	"testing"

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

// The query string gets returned as second return value.
func TestParseURI_Query(t *testing.T) {
	cases := map[string]string{
		"test.db":                          "",
		"file:test.db?":                    "",
		"test.db?mode=rwc":                 "mode=rwc",
		"test.db?_foreign_keys=1&mode=rwc": "_foreign_keys=1&mode=rwc",
	}
	for uri, expected := range cases {
		t.Run(uri, func(t *testing.T) {
			_, query, err := connection.ParseURI(uri)
			if err != nil {
				t.Fatal(err)
			}
			if query != expected {
				t.Fatalf("Got query %s, expected %s", query, expected)
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
		":memory:":            "can't replicate a memory database",
		"test.db?mode=memory": "can't replicate a memory database",
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

func TestDSN_Encode(t *testing.T) {
	cases := map[string]string{
		"test.db":        "test.db",
		"test.db?mode=r": "test.db?mode=r",
		"file:test.db":   "test.db",
	}
	for uri, expected := range cases {
		t.Run(uri, func(t *testing.T) {
			filename, query, err := connection.ParseURI(uri)
			if err != nil {
				t.Fatal(err)
			}
			got := connection.EncodeURI(filename, query)
			if got != expected {
				t.Fatalf("EncodeURI() should have returned %s, got %s instead", expected, got)
			}
		})
	}
}
