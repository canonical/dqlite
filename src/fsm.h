/**
 * Dqlite Raft FSM
 */

#ifndef DQLITE_FSM_H_
#define DQLITE_FSM_H_

#include "config.h"
#include "raft.h"
#include "registry.h"

/**
 * Initialize the given SQLite replication interface with dqlite's raft based
 * implementation.
 */
int fsm__init(struct raft_fsm *fsm,
	      struct config *config,
	      struct registry *registry);

void fsm__close(struct raft_fsm *fsm);

#endif /* DQLITE_REPLICATION_METHODS_H_ */
