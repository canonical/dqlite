package connection

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
)

// DSN captures details of a dqlite-compatible sqlite connection DSN. Only
// pure file names without any directory segment are accepted (e.g.
// "test.db"). Query parameters are always valid except for "mode=memory".
type DSN struct {
	Filename string
	Query    string
}

// NewDSN parses the given sqlite3 DSN name checking if it's
// compatible with dqlite.
func NewDSN(name string) (*DSN, error) {
	filename := name
	query := ""
	pos := strings.IndexRune(name, '?')
	if pos >= 1 {
		query = name[pos+1:]
		params, err := url.ParseQuery(query)
		if err != nil {
			return nil, err
		}
		if params.Get("mode") == "memory" {
			return nil, fmt.Errorf("can't replicate a memory database")
		}
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

	dsn := &DSN{
		Filename: filename,
		Query:    query,
	}
	return dsn, nil
}

// String returns the full URI, including filename and query, the
// given dir will be prepended.
func (i *DSN) String(dir string) string {
	query := ""
	if i.Query != "" {
		query = "?" + i.Query
	}
	return fmt.Sprintf("%s%s", filepath.Join(dir, i.Filename), query)
}
