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
