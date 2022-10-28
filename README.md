dqlite [![CI Tests](https://github.com/canonical/dqlite/actions/workflows/build-and-test.yml/badge.svg)](https://github.com/canonical/dqlite/actions/workflows/build-and-test.yml) [![codecov](https://codecov.io/gh/canonical/dqlite/branch/master/graph/badge.svg)](https://codecov.io/gh/canonical/dqlite)
======

[English](./README.md)|[简体中文](./README_CH.md)

[dqlite](https://dqlite.io) is a C library that implements an embeddable and replicated SQL database
engine with high-availability and automatic failover.

The acronym "dqlite" stands for "distributed SQLite", meaning that dqlite extends
[SQLite](https://sqlite.org/) with a network protocol that can connect together
various instances of your application and have them act as a highly-available
cluster, with no dependency on external databases.

Design highlights
----------------

* Asynchronous single-threaded implementation using [libuv](https://libuv.org/)
  as event loop.
* Custom wire protocol optimized for SQLite primitives and data types.
* Data replication based on the [Raft](https://raft.github.io/) algorithm and its
  efficient [C-raft](https://github.com/canonical/raft) implementation.

License
-------

The dqlite library is released under a slightly modified version of LGPLv3, that
includes a copyright exception allowing users to statically link the library code
in their project and release the final work under their own terms. See the full
[license](https://github.com/canonical/dqlite/blob/master/LICENSE) text.

Compatibility
-------------

dqlite runs on Linux and requires a kernel with support for [native async
I/O](https://man7.org/linux/man-pages/man2/io_setup.2.html) (not to be confused
with [POSIX AIO](https://man7.org/linux/man-pages/man7/aio.7.html)), which is
used by the libuv backend of C-raft.

Try it
-------

The simplest way to see dqlite in action is to use the demo program that comes
with the Go dqlite bindings. Please see the [relevant
documentation](https://github.com/canonical/go-dqlite#demo) in that project.

Media
-----

A talk about dqlite was given at FOSDEM 2020, you can watch it
[here](https://fosdem.org/2020/schedule/event/dqlite/).

Wire protocol
-------------

If you wish to write a client, please refer to the [wire protocol](https://dqlite.io/docs/protocol)
documentation.

Install
-------

If you are on a Debian-based system, you can get the latest development release from
dqlite's [dev PPA](https://launchpad.net/~dqlite/+archive/ubuntu/dev):

```
sudo add-apt-repository ppa:dqlite/dev
sudo apt-get update
sudo apt-get install libdqlite-dev
```

Build
-----

To build ``libdqlite`` from source you'll need:

* A reasonably recent version of [libuv](http://libuv.org/) (v1.8.0 or beyond).
* A reasonably recent version of sqlite3-dev
* A build of the [C-raft](https://github.com/canonical/raft) Raft library.

Your distribution should already provide you with a pre-built libuv shared
library and libsqlite3-dev.

To build the raft library:

```
git clone https://github.com/canonical/raft.git
cd raft
autoreconf -i
./configure
make
sudo make install
cd ..
```

Once all the required libraries are installed, in order to build the dqlite
shared library itself, you can run:

```
autoreconf -i
./configure
make
sudo make install
```

Usage Notes
-----------

Detailed tracing will be enabled when the environment variable `LIBDQLITE_TRACE` is set before startup.

