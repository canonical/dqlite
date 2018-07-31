dqlite [![Build Status](https://travis-ci.org/CanonicalLtd/dqlite.png)](https://travis-ci.org/CanonicalLtd/dqlite) [![codecov](https://codecov.io/gh/CanonicalLtd/dqlite/branch/master/graph/badge.svg)](https://codecov.io/gh/CanonicalLtd/dqlite)
======

This repository provides the `dqlite` C library (libdqlite), which can be used
to expose a SQLite database over the network and replicate it across a cluster
of peers, using the Raft algorithm.

Note that at the moment libdqlite implements only the client/server networking
code, which allows a client to connect to a dqlite node and perform SQL queries
using a dedicated wire protocol. The code that implements Raft-based replication
is currently written in Go and available in the [go-dqlite](/CanonicalLtd/go-dqlite/)
repository.

It should be possible to compile the ``go-dqlite`` Go package as shared library
and hence use ``libdqlite`` with any programming language with C
bindings. However, the current focus of dqlite is to be an embedded distributed
database for Go applications.

See [go-dqlite](/CanonicalLtd/go-dqlite/) for more information.

Design higlights
----------------

* Asynchronous single-threaded server implemented on top of [libuv](http://libuv.org/)
* Custom wire protocol optimized for SQLite primitives and data types
* Raft replication is not built-in, consumers need to provide an implementation

Status
------

This is **beta** software for now, but we'll get to rc/release soon.

Build
-----

To build ``libdqlite`` you need:

* A [patched version of SQLite](https://github.com/CanonicalLtd/sqlite/releases/latest)
  with support for WAL-based replication.
* A reasonably recent version of ``libuv`` (v1.8.0 or beyond).
