dqlite [![Build Status](https://travis-ci.org/canonical/dqlite.png)](https://travis-ci.org/canonical/dqlite) [![codecov](https://codecov.io/gh/canonical/dqlite/branch/master/graph/badge.svg)](https://codecov.io/gh/canonical/dqlite)
======

dqlite is a C library that implements an embeddable and replicated SQL database
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
[license](https://github.com/canonical/dqlite/blob/LICENSE) text.

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

If you wish to write a client, please refer to the [wire protocol](doc/protocol.md)
documentation.

Install
-------

If you are on a Debian-based system, you can get the latest stable release from
dqlite's [stable PPA](https://launchpad.net/~dqlite/+archive/ubuntu/stable):

```
sudo add-apt-repository ppa:dqlite/stable
sudo apt-get update
sudo apt-get install libdqlite-dev
```

Build
-----

To build ``libdqlite`` from source you'll need:

* A reasonably recent version of [libuv](http://libuv.org/) (v1.8.0 or beyond).
* A build of the [C-raft](https://github.com/canonical/raft) Raft library.

Your distribution should already provide you with a pre-built libuv shared
library.

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
