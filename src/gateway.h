/**
 * Core dqlite server engine, calling out SQLite for serving client requests.
 */

#ifndef DQLITE_GATEWAY_H_
#define DQLITE_GATEWAY_H_

#include "../include/dqlite.h"

#include "db_.h"
#include "error.h"
#include "leader.h"
#include "options.h"
#include "registry.h"
#include "request.h"
#include "response.h"

/**
 * Handle requests from a single connected client and forward them to
 * SQLite.
 */
struct gateway
{
	struct dqlite_logger *logger; /* Logger to use */
	struct options *options;      /* Configuration options */
	struct registry *registry;
	struct leader *leader;
};

void gateway__init(struct gateway *g,
		   struct dqlite_logger *logger,
		   struct options *options,
		   struct registry *registry);

void gateway__close(struct gateway *g);

#endif /* DQLITE_GATEWAY_H_ */
