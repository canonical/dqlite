How does it compare to rqlite?
------------------------------

The main differences from [rqlite](https://github.com/rqlite/rqlite) are:

* Embeddable in any language that can interoperate with C
* Full support for transactions
* No need for statements to be deterministic (e.g. you can use ```time()```)
* Frame-based replication instead of statement-based replication
