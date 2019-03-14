/******************************************************************************
 *
 * Raft-based implementation of the SQLite replication interface.
 *
 *****************************************************************************/

#ifndef DQLITE_REPLICATION_H_
#define DQLITE_REPLICATION_H_

#include <raft.h>
#include <sqlite3.h>

#include "../include/dqlite.h"

/**
 * Initialize the given SQLite replication interface with dqlite's raft based
 * implementation.
 */
int replication__init(struct sqlite3_wal_replication *replication,
		      struct dqlite_logger *logger,
		      struct raft *raft);

void replication__close(struct sqlite3_wal_replication *replication);

#endif /* DQLITE_REPLICATION_H_ */
