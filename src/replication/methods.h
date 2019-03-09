/******************************************************************************
 *
 * Raft-based implementation of the SQLite replication interface.
 *
 *****************************************************************************/

#ifndef DQLITE_REPLICATION_METHODS_H
#define DQLITE_REPLICATION_METHODS_H

#include <libco.h>
#include <sqlite3.h>

#include "../../include/dqlite.h"

/**
 * Initialize the given SQLite replication interface with dqlite's raft based
 * implementation.
 */
int replication__init(struct sqlite3_wal_replication *replication,
		      struct dqlite_logger *logger);

void replication__close(struct sqlite3_wal_replication *replication);

#endif /* DQLITE_REPLICATION_METHODS_H_ */
