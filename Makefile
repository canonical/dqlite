PROJECT=github.com/CanonicalLtd/dqlite

SQLITE_TAG=replication-support-3.19.2
SQLITE_TAR=release--enable-debug.tar.gz
SQLITE_URL=https://github.com/CanonicalLtd/sqlite/releases/download/$(SQLITE_TAG)/$(SQLITE_TAR)

GO=$(shell pwd)/.go-wrapper
GO_TAGS=libsqlite3

dependencies:
	mkdir -p .sqlite && cd .sqlite && rm -f * && wget $(SQLITE_URL) -O - | tar xfz -

proto:
	protoc --gofast_out=. command/commands.proto

build:
	$(GO) get -t -tags $(GO_TAGS) ./...
	$(GO) build -tags  $(GO_TAGS)

coverage:
	$(GOPATH)/bin/overalls -go-binary $(GO) -project $(PROJECT) -covermode=count -- -tags $(GO_TAGS)

.PHONY: dependencies build coverage
