package connection

import (
	"fmt"
	"net/url"
	"strings"
)

// Params captures details of dqlite leader connection parameters. Only pure file
// names without any directory segment are accepted (e.g. "test.db"). Query
// parameters are always valid except for "mode=memory".
type Params struct {
	Filename string // Main database filename
	Query    string // Opaque query string to pass down to go-sqlite3/SQLite
}

// NewDSN parses the given sqlite3 DSN name checking if it's
// compatible with dqlite.
func NewDSN(name string) (*Params, error) {
	filename := name
	query := ""

	pos := strings.IndexRune(name, '?')
	if pos >= 1 {
		params, err := url.ParseQuery(name[pos+1:])
		if err != nil {
			return nil, err
		}
		if params.Get("mode") == "memory" {
			return nil, fmt.Errorf("can't replicate a memory database")
		}
		query = params.Encode()
		filename = filename[:pos]
	}

	if strings.HasPrefix(filename, "file:") {
		filename = filename[len("file:"):]
	}

	if filename == ":memory:" {
		return nil, fmt.Errorf("can't replicate a memory database")
	}

	if strings.IndexRune(filename, '/') >= 0 {
		return nil, fmt.Errorf("directory segments are invalid")
	}

	dsn := &Params{
		Filename: filename,
		Query:    query,
	}
	return dsn, nil
}

// Encode returns the full URI, including filename and query.
func (d *Params) Encode() string {
	query := ""
	if d.Query != "" {
		query = "?" + d.Query
	}
	return d.Filename + query
}
