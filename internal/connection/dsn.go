package connection

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Params captures details of dqlite leader connection parameters. Only pure file
// names without any directory segment are accepted (e.g. "test.db"). Query
// parameters are always valid except for "mode=memory".
type Params struct {
	Filename string // Main database filename
	Query    string // Opaque query string to pass down to go-sqlite3/SQLite

	// Special dqlite parameters
	LeadershipTimeout time.Duration // Maximum time to wait for leadership
	InitializeTimeout time.Duration // Maximum time to wait for pending logs to be applied
}

// NewDSN parses the given sqlite3 DSN name checking if it's
// compatible with dqlite.
func NewDSN(name string) (*Params, error) {
	filename := name
	query := ""

	leadershipTimeout := 10000
	initializeTimeout := 30000

	pos := strings.IndexRune(name, '?')
	if pos >= 1 {
		params, err := url.ParseQuery(name[pos+1:])
		if err != nil {
			return nil, err
		}
		if params.Get("mode") == "memory" {
			return nil, fmt.Errorf("can't replicate a memory database")
		}
		if key := params.Get("_leadership_timeout"); key != "" {
			leadershipTimeout, err = strconv.Atoi(key)
			if err != nil {
				return nil, fmt.Errorf("leadership timeout is not a number: '%s'", key)
			}
			params.Del("_leadership_timeout")
		}
		if key := params.Get("_initialize_timeout"); key != "" {
			initializeTimeout, err = strconv.Atoi(key)
			if err != nil {
				return nil, fmt.Errorf("initialize timeout is not a number: '%s'", key)
			}
			params.Del("_initialize_timeout")
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
		Filename:          filename,
		Query:             query,
		LeadershipTimeout: time.Duration(leadershipTimeout) * time.Millisecond,
		InitializeTimeout: time.Duration(initializeTimeout) * time.Millisecond,
	}
	return dsn, nil
}

// Encode returns the full URI, including filename and query, but excluding any
// dqlite-specific parameters.
func (d *Params) Encode() string {
	query := ""
	if d.Query != "" {
		query = "?" + d.Query
	}
	return d.Filename + query
}
