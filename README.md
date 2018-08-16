dqlite [![Build Status](https://travis-ci.org/CanonicalLtd/dqlite.png)](https://travis-ci.org/CanonicalLtd/dqlite) [![codecov](https://codecov.io/gh/CanonicalLtd/dqlite/branch/master/graph/badge.svg)](https://codecov.io/gh/CanonicalLtd/dqlite)
======

This repository provides the `dqlite` C library (libdqlite), which can be used
to expose a SQLite database over the network and replicate it across a cluster
of peers, using the Raft algorithm.

Note that at the moment libdqlite implements only the client/server networking
code, which allows a client to connect to a dqlite node and perform SQL queries
using a dedicated wire protocol. The code that implements Raft-based replication
is currently written in Go and available in the [go-dqlite](https://github.com/CanonicalLtd/go-dqlite/)
repository.

It should be possible to compile the ``go-dqlite`` Go package as shared library
and hence use ``libdqlite`` with any programming language with C
bindings. However, the current focus of dqlite is to be an embedded distributed
database for Go applications.

See [go-dqlite](https://github.com/CanonicalLtd/go-dqlite/) for more information.

Design higlights
----------------

* Asynchronous single-threaded server implemented on top of [libuv](http://libuv.org/)
* Custom wire protocol optimized for SQLite primitives and data types
* Raft replication is not built-in, consumers need to provide an implementation

Status
------

This is **beta** software for now, but we'll get to rc/release soon.

Install
-------

If you are on a Debian-based system, you can install daily built packages from a
[Launchpad PPA](https://launchpad.net/~dqlite-maintainers/+archive/ubuntu/master):

```
sudo add-apt-repository ppa:dqlite-maintainers/master
sudo apt-get update
sudo apt-get install libsqlite3-dev libdqlite-dev
```

Build
-----

To build ``libdqlite`` from source you'll need:

* A reasonably recent version of [libuv](http://libuv.org/) (v1.8.0 or beyond).
* A [patched version of SQLite](https://github.com/CanonicalLtd/sqlite/releases/latest)
  with support for WAL-based replication.

Your distribution should already provide you a pre-built libuv shared
library.

As for the patched version of SQLite, the base line is currently version 3.24.0
and the changeset can be viewed [here](https://github.com/mackyle/sqlite/compare/version-3.24.0...CanonicalLtd:replication).

To build it:

```
git clone --depth 100 https://github.com/CanonicalLtd/sqlite.git
git log -1 --format=format:%ci%n | sed -e 's/ [-+].*$//;s/ /T/;s/^/D /' > manifest
git log -1 --format=format:%H > manifest.uuid
./configure --enable-replication
make
sudo make install
```

Once libuv and SQLite are installed, to in order to build the dqlite shared
library itself you can run:

```
autoreconf -i
./configure
make
sudo make install
```
