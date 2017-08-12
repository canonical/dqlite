PROJECT=github.com/CanonicalLtd/dqlite

SQLITE_TAG=replication-support-3.19.2
SQLITE_TAR=release--enable-debug.tar.gz
SQLITE_URL=https://github.com/CanonicalLtd/sqlite/releases/download/$(SQLITE_TAG)/$(SQLITE_TAR)

export GO_TAGS=libsqlite3
export CGO_CFLAGS=-I$(PWD)/.sqlite/
export CGO_LDFLAGS=-L$(PWD)/.sqlite/
export LD_LIBRARY_PATH=$(PWD)/.sqlite/

dependencies:
	mkdir -p .sqlite && cd .sqlite && rm -f * && wget $(SQLITE_URL) -O - | tar xfz -

build:
	go get -tags "$TAGS" ./...
	go get github.com/CanonicalLtd/raft-test
	go build -tags "$TAGS"

coverage:
	$(GOPATH)/bin/overalls -project $(PROJECT) -covermode=count -- -tags $(GO_TAGS)

.PHONY: dependencies build coverage
