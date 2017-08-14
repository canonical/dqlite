package connection_test

import (
	"testing"

	"github.com/CanonicalLtd/dqlite/connection"
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
		t.Run(input, func(t *testing.T) {
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
		t.Run(input, func(t *testing.T) {
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
		t.Run(input, func(t *testing.T) {
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
		":memory:":                        "can't replicate a memory database",
		"test.db?mode=memory":             "can't replicate a memory database",
		"test.db?%gh&%ij":                 "invalid URL escape \"%gh\"",
		"file:///test.db":                 "directory segments are invalid",
		"/foo/test.db":                    "directory segments are invalid",
		"./bar/test.db":                   "directory segments are invalid",
		"test.db?_leadership_timeout=abc": "leadership timeout is not a number: 'abc'",
		"test.db?_initialize_timeout=abc": "initialize timeout is not a number: 'abc'",
	}
	for input, error := range cases {
		t.Run(input, func(t *testing.T) {
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

func TestNewDSN_LeadershipTimeout(t *testing.T) {
	dsn, err := connection.NewDSN("test.db?_leadership_timeout=100&mode=rwc")
	if err != nil {
		t.Fatal(err)
	}
	if dsn.LeadershipTimeout.String() != "100ms" {
		t.Errorf("leadership timeout is %s, want 100s", dsn.LeadershipTimeout)
	}
	if dsn.Query != "mode=rwc" {
		t.Errorf("query is '%s', want 'mode=rwc'", dsn.Query)
	}
}

func TestNewDSN_InitializeTimeout(t *testing.T) {
	dsn, err := connection.NewDSN("test.db?_initialize_timeout=100&mode=rwc")
	if err != nil {
		t.Fatal(err)
	}
	if dsn.InitializeTimeout.String() != "100ms" {
		t.Errorf("initialize timeout is %s, want 100s", dsn.InitializeTimeout)
	}
	if dsn.Query != "mode=rwc" {
		t.Errorf("query is '%s', want 'mode=rwc'", dsn.Query)
	}
}

func TestDSN_Encode(t *testing.T) {
	cases := map[string]string{
		"test.db":                                "test.db",
		"test.db?mode=r":                         "test.db?mode=r",
		"test.db?_leadership_timeout=100&mode=r": "test.db?mode=r",
	}
	for input, output := range cases {
		t.Run(input, func(t *testing.T) {
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
