package dqlite_test

import (
	"fmt"
	"io/ioutil"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/dqlite/dqlite"
	"github.com/dqlite/raft-http"
)

func TestNewHTTPConfig(t *testing.T) {
	configs := make([]*dqlite.Config, 2)
	for i := range configs {
		configs[i] = newHTTPConfig()
		defer os.RemoveAll(configs[0].Dir)
	}
	driver1, err := dqlite.NewDriver(configs[0], "")
	if err != nil {
		t.Fatal(err)
	}
	defer driver1.Shutdown()

	driver2, err := dqlite.NewDriver(configs[1], configs[0].Transport.LocalAddr())
	if err != nil {
		t.Fatal(err)
	}
	defer driver2.Shutdown()
}

// Wrapper around NewHTTPConfig that also creates a listener and
// handler.
func newHTTPConfig() *dqlite.Config {
	dir, err := ioutil.TempDir("", "go-dqlite-")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp dir: %v", err))
	}

	handler := rafthttp.NewHandler()
	server := httptest.NewServer(handler)
	addr := server.Listener.Addr()
	logger := dqlite.NewLogger(ioutil.Discard, "", 0)

	return dqlite.NewHTTPConfig(dir, handler, "/", addr, logger)
}
