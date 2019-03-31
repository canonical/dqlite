/**
 * Core dqlite server engine, calling out SQLite for serving client requests.
 */

#ifndef DQLITE_GATEWAY_H_
#define DQLITE_GATEWAY_H_

#include <raft.h>

#include "../include/dqlite.h"

#include "leader.h"
#include "options.h"
#include "registry.h"

/**
 * Handle requests from a single connected client and forward them to
 * SQLite.
 */
struct gateway
{
	struct dqlite_logger *logger; /* Logger to use */
	struct options *options;      /* Configuration options */
	struct registry *registry;    /* Register of existing databases */
	struct raft *raft;	    /* Raft instance */
	struct leader *leader;
};

void gateway__init(struct gateway *g,
		   struct dqlite_logger *logger,
		   struct options *options,
		   struct registry *registry,
		   struct raft *raft);

void gateway__close(struct gateway *g);

#endif /* DQLITE_GATEWAY_H_ */
