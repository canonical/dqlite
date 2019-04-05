/**
 *
 * Raft-based implementation of the SQLite replication interface.
 *
 */

#ifndef DQLITE_REPLICATION_H_
#define DQLITE_REPLICATION_H_

#include <raft.h>
#include <sqlite3.h>

#include "logger.h"

/**
 * Initialize the given SQLite replication interface with dqlite's raft based
 * implementation.
 *
 * This function also automatically register the implementation in the global
 * SQLite registry, using the given @name.
 */
int replication__init(struct sqlite3_wal_replication *replication,
		      const char *name,
		      struct logger *logger,
		      struct raft *raft);

/**
 * Release all memory associated with the given dqlite raft's based replication
 * implementation.
 *
 * This function also automatically unregister the implementation from the
 * SQLite global registry.
 */
void replication__close(struct sqlite3_wal_replication *replication);

#endif /* DQLITE_REPLICATION_H_ */
