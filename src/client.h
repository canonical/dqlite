#ifndef DQLITE_CLIENT_H_
#define DQLITE_CLIENT_H_

#include "client/protocol.h"
#include "server.h"

struct dqlite {
	struct dqlite_server *server;
	char *name; /* owned */
};

struct dqlite_stmt {
	uint32_t stmt_id;
	uint64_t n_params;
	struct client_proto proto;

	uint64_t offset;
};

#endif /* DQLITE_CLIENT_H_ */
