/******************************************************************************
 *
 * Dqlite Raft FSM
 *
 *****************************************************************************/

#ifndef DQLITE_FSM_H_
#define DQLITE_FSM_H_

#include <raft.h>

#include "../include/dqlite.h"

#include "registry.h"

/**
 * Initialize the given SQLite replication interface with dqlite's raft based
 * implementation.
 */
int fsm__init(struct raft_fsm *fsm,
	      struct dqlite_logger *logger,
	      struct registry *registry);

void fsm__close(struct raft_fsm *fsm);

#endif /* DQLITE_REPLICATION_METHODS_H_ */
