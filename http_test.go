package dqlite_test

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/CanonicalLtd/dqlite"
	"github.com/CanonicalLtd/raft-http"
)

func TestNewHTTPConfig(t *testing.T) {
	configs := make([]*dqlite.Config, 2)
	for i := range configs {
		configs[i] = newHTTPConfig()
		defer os.RemoveAll(configs[0].Dir)
	}
	configs[0].EnableSingleNode = true
	driver1, err := dqlite.NewDriver(configs[0])
	if err != nil {
		t.Fatal(err)
	}
	defer driver1.Shutdown()

	driver2, err := dqlite.NewDriver(configs[1])
	if err != nil {
		t.Fatal(err)
	}
	defer driver2.Shutdown()

	if err := driver2.Join(configs[0].Transport.LocalAddr(), 500*time.Millisecond); err != nil {
		t.Fatal(err)
	}
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
	logger := log.New(ioutil.Discard, "", 0)

	config := dqlite.NewHTTPConfig(dir, handler, "/", addr, logger)

	// Lower timeouts to speed up text execution.
	config.HeartbeatTimeout = 50 * time.Millisecond
	config.ElectionTimeout = 50 * time.Millisecond
	config.LeaderLeaseTimeout = 50 * time.Millisecond

	return config
}
