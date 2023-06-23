#ifndef DQLITE_CLIENT_H_
#define DQLITE_CLIENT_H_

#include "client_protocol.h"

struct dqlite
{
	struct dqlite_server *server;
	char *name; /* owned */
};

struct owned_value
{
	struct value inner;
	void (*dealloc)(void *);
};

struct dqlite_stmt
{
	struct dqlite *db;
	struct client_proto proto;
	uint32_t id;
	unsigned n_params;
	struct owned_value *params;
	int state;
	struct rows rows;
	struct row *next_row;
	void **converted;
};

#endif /* DQLITE_CLIENT_H_ */
