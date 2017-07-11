dqlite [![Build Status](https://travis-ci.org/dqlite/dqlite.png)](https://travis-ci.org/dqlite/dqlite) [![Coverage Status](https://coveralls.io/repos/github/dqlite/dqlite/badge.svg?branch=master)](https://coveralls.io/github/dqlite/dqlite?branch=master) [![Go Report Card](https://goreportcard.com/badge/github.com/dqlite/dqlite)](https://goreportcard.com/report/github.com/dqlite/dqlite)
======

This repository provides the `dqlite` Go package, which can be used to
replicate a SQLite database across a cluster, using the Raft
algorithm.

Design higlights
----------------

* No external processes needed: dqlite is just a Go library, you link it
  it to your application exactly like you would with SQLite.
* Replication needs a [SQLite patch](https://github.com/dqlite/sqlite/commit/2a9aa8b056f37ae05f38835182a2856ffc95aee4)
  which is not yet included upstream.
* The Go [Raft package](https://github.com/hashicorp/raft) from Hashicorp
  is used internally for replicating the write-ahead log frames of SQLite
  across all nodes.

How does it compare to rqlite?
------------------------------

The main differences from [rqlite](https://github.com/rqlite/rqlite) are:

* Full support for transactions
* No need for statements to be deterministic (e.g. you can use ```time()```)
* Frame-based replication instead of statement-based replication, this
  means in dqlite there's more data flowing between nodes, so expect
  lower performance. Should not really matter for most use cases.

Status
------

This is '''alpha''' software for now, but we'll get to beta/rc soon.

Demo
----

To see dqlite in action, make sure you have the following dependencies
installed:

* Go (tested on 1.8)
* gcc
* any dependency/header that SQLite needs to build from source
* Python 3

Then run:

```
go get -d github.com/dqlite/dqlite
cd $GOPATH/src/github.com/dqlite/dqlite
./run-demo
```

This should spawn three dqlite-based nodes, each of one running the
code in the [demo Go source](testdata/demo.go).

Each node inserts data in a test table and then dies abruptly after a
random timeout. Leftover transactions and failover to other nodes
should be handled gracefully.

While the demo is running, to get more details about what's going on
behind the scenes you can also open another terminal and run a command
like:

```
watch ls -l /tmp/dqlite-demo-*/ /tmp/dqlite-demo-*/snapshots/
```

and see how the data directories of the three nodes evolve in terms
SQLite databases (```test.db```), write-ahead log files (```test.db-wal```),
raft logs store (```raft.db```), and raft snapshots.


Documentation
-------------

The documentation for this package can be found on [Godoc](http://godoc.org/github.com/dqlite/dqlite).
