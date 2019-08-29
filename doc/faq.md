How does dqlite behave during conflict situations?
--------------------------------------------------

Does Raft select a winning WAL write and any others in flight writes are
aborted?

There can't be a conflict situation. Raft's model is that only the leader can
append new log entries, which translated to dqlite means that only the leader
can write new WAL frames. So this means that any attempt to perform a write
transaction on a non-leader node will fail with a ErrNotLeader error (and in
this case clients are supposed to retry against whoever is the new leader).

When not enough nodes are available, are writes hung until consensus?
---------------------------------------------------------------------

Yes, however there's a (configurable) timeout. This is a consequence of Raft
sitting in the CP spectrum of the CAP theorem: in case of a network partition it
chooses consistency and sacrifices availability.


How does dqlite compare to rqlite?
----------------------------------

The main differences from [rqlite](https://github.com/rqlite/rqlite) are:

* Embeddable in any language that can interoperate with C
* Full support for transactions
* No need for statements to be deterministic (e.g. you can use ```time()```)
* Frame-based replication instead of statement-based replication

Why C?
------

The first prototype implementation of dqlite was in Go, leveraging the
[hashicorp/raft](https://github.com/hashicorp/raft/) implementation of the Raft
algorithm. The project was later rewritten entirely in C because of performance
problems due to the way Go interoperates with C: Go considers a function call
into C that lasts more than ~20 microseconds as a blocking system call, in that
case it will put the goroutine running that C call in waiting queue and resuming
it will effectively cause a context switch, degrading performance (since there
were a lot of them happening). See also [this
issue](https://github.com/golang/go/issues/19574) in the Go bug tracker.

The added benefit of the rewrite in C is that it's now easy to embed dqlite into
project written in effectively any language, since all major languages have
provisions to create C bindings.

Who's using dqlite?
-------------------

At the moment the biggest user of dqlite is the
[LXD](https://linuxcontainers.org/lxd/introduction/) system containers manager,
which uses dqlite to implement high-availability when run in cluster mode. See
the relevant
[documentation](https://github.com/lxc/lxd/blob/master/doc/clustering.md).
