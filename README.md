dqlite [![CI Tests](https://github.com/canonical/dqlite/actions/workflows/build-and-test.yml/badge.svg)](https://github.com/canonical/dqlite/actions/workflows/build-and-test.yml) [![codecov](https://codecov.io/gh/canonical/dqlite/branch/master/graph/badge.svg)](https://codecov.io/gh/canonical/dqlite)
======

[English](./README.md)|[简体中文](./README_CH.md)

[dqlite](https://dqlite.io) is a C library that implements an embeddable and
replicated SQL database engine with high availability and automatic failover.

The acronym "dqlite" stands for "distributed SQLite", meaning that dqlite
extends [SQLite](https://sqlite.org/) with a network protocol that can connect
together various instances of your application and have them act as a
highly-available cluster, with no dependency on external databases.

Design highlights
----------------

* Asynchronous single-threaded implementation using [libuv](https://libuv.org/)
  as event loop.
* Custom wire protocol optimized for SQLite primitives and data types.
* Data replication based on the [Raft](https://raft.github.io/) algorithm.

License
-------

The dqlite library is released under a slightly modified version of LGPLv3,
that includes a copyright exception allowing users to statically link the
library code in their project and release the final work under their own terms.
See the full [license](https://github.com/canonical/dqlite/blob/master/LICENSE)
text.

Compatibility
-------------

dqlite runs on Linux and requires a kernel with support for [native async
I/O](https://man7.org/linux/man-pages/man2/io_setup.2.html) (not to be confused
with [POSIX AIO](https://man7.org/linux/man-pages/man7/aio.7.html)).

Try it
-------

The simplest way to see dqlite in action is to use the demo program that comes
with the Go dqlite bindings. Please see the [relevant
documentation](https://github.com/canonical/go-dqlite#demo) in that project.

Media
-----

A talk about dqlite was given at FOSDEM 2020, you can watch it
[here](https://fosdem.org/2020/schedule/event/dqlite/).

[Here](https://gcore.com/blog/comparing-litestream-rqlite-dqlite/) is a blog
post from 2022 comparing dqlite with rqlite and Litestream, other replication
software for SQLite.

Wire protocol
-------------

If you wish to write a client, please refer to the [wire
protocol](https://dqlite.io/docs/protocol) documentation.

Install
-------

If you are on a Debian-based system, you can get the latest development release
from dqlite's [dev PPA](https://launchpad.net/~dqlite/+archive/ubuntu/dev):

```
sudo add-apt-repository ppa:dqlite/dev
sudo apt update
sudo apt install libdqlite-dev
```

Contributing
------------

See [CONTRIBUTING.md](./CONTRIBUTING.md).

Build
-----

To build libdqlite from source you'll need:

* Build dependencies: pkg-config and GNU Autoconf, Automake, libtool, and make
* A reasonably recent version of [libuv](https://libuv.org/) (v1.8.0 or later), with headers.
* A reasonably recent version of [SQLite](https://sqlite.org/) (v3.22.0 or later), with headers.
* Optionally, a reasonably recent version of [LZ4](https://lz4.org/) (v1.7.1 or later), with headers.

Your distribution should already provide you with these dependencies. For
example, on Debian-based distros:

```
sudo apt install pkg-config autoconf automake libtool make libuv1-dev libsqlite3-dev liblz4-dev
```

With these dependencies installed, you can build and install the dqlite shared
library and headers as follows:

```
$ autoreconf -i
$ ./configure
$ make
$ sudo make install
```

The default installation prefix is `/usr/local`; you may need to run

```
$ sudo ldconfig
```

to enable the linker to find `libdqlite.so`. To install to a different prefix,
replace the configure step with something like

```
$ ./configure --prefix=/usr
```

Building for static linking
---------------------------

If you're building dqlite for eventual use in a statically-linked
binary, there are some additional considerations. You should pass
`--with-static-deps` to the configure script; this disables code that
relies on dependencies being dynamically linked. (Currently it only
affects the test suite, but you should use it even when building
`libdqlite.a` only for future compatibility.)

When linking libdqlite with musl libc, it's recommended to increase
the default stack size, which is otherwise too low for dqlite's
needs:

```
LDFLAGS="-Wl,-z,stack-size=1048576"
```

The `contrib/build-static.sh` script demonstrates building and
testing dqlite with all dependencies (including libc) statically
linked.

Usage notes
-----------

Detailed tracing will be enabled when the environment variable
`LIBDQLITE_TRACE` is set before startup.  The value of it can be in `[0..5]`
range and represents a tracing level, where `0` means "no traces" emitted, `5`
enables minimum (FATAL records only), and `1` enables maximum verbosity (all:
DEBUG, INFO, WARN, ERROR, FATAL records).
