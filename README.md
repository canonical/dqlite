dqlite [![Build Status](https://travis-ci.org/canonical/dqlite.png)](https://travis-ci.org/canonical/dqlite) [![codecov](https://codecov.io/gh/canonical/dqlite/branch/master/graph/badge.svg)](https://codecov.io/gh/canonical/dqlite)
======

dqlite is a C library that implements an embeddable and replicated SQL database
engine with high-availability and automatic failover.

The acronym "dqlite" stands for "distributed SQLite": it extends
[SQLite](https://sqlite.org/) with a network protocol that can connect together
various instances of your application and have them act as a high-available
cluster, with no dependency on external databases.

Design higlights
----------------

* Asynchronous single-threaded implementation using [libuv](https://libuv.org/)
  as event loop.
* Custom wire protocol optimized for SQLite primitives and data types.
* Data replication based on the [Raft](https://raft.github.io/) algorithm and its
  efficient [C-raft](https://github.com/canonical/raft) implementation.

Install
-------

If you are on a Debian-based system, you can install daily built packages from
dqlite's [PPA](https://launchpad.net/~dqlite-maintainers/+archive/ubuntu/master):

```
sudo add-apt-repository ppa:dqlite-maintainers/master
sudo apt-get update
sudo apt-get install libdqlite-dev
```

Build
-----

To build ``libdqlite`` from source you'll need:

* A reasonably recent version of [libuv](http://libuv.org/) (v1.8.0 or beyond).
* A [patched version of SQLite](https://github.com/canonical/sqlite/releases/latest)
  with support for WAL-based replication.
* A build of the [C-raft](https://github.com/canonical/raft) Raft library.
* A build of the [libco](https://github.com/canonical/libco) coroutine library.

Your distribution should already provide you a pre-built libuv shared
library.

To build the other libraries:

```
git clone --depth 100 https://github.com/canonical/sqlite.git
cd sqlite
./configure --enable-replication
make
sudo make install
cd ..
git clone https://github.com/canonical/libco.git
cd libco
make
sudo make install
cd ..
git clone https://github.com/canonical/raft.git
cd raft
autoreconf -i
./configure
make
sudo make install
cd ..
```

Once all required libraries are installed, to in order to build the dqlite
shared library itself you can run:

```
autoreconf -i
./configure
make
sudo make install
```
