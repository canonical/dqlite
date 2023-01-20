/* Core dqlite client logic for encoding requests and decoding responses. */

#ifndef DQLITE_CLIENT_PROTOCOL_H_
#define DQLITE_CLIENT_PROTOCOL_H_

#include "../lib/buffer.h"

#include "../tuple.h"

struct client_proto
{
	int fd;		     /* Connected socket */
	unsigned db_id;      /* Database ID provided by the server */
	char *db_name;
	bool db_is_init;
	struct buffer read;  /* Read buffer */
	struct buffer write; /* Write buffer */
	uint64_t errcode;
	char *errmsg;
};

struct row
{
	struct value *values;
	struct row *next;
};

struct rows
{
	unsigned column_count;
	const char **column_names;
	struct row *next;
};

/* Initialize a new client, writing requests to fd. */
int clientInit(struct client_proto *c, int fd);

/* Release all memory used by the client, and close the client socket. */
void clientClose(struct client_proto *c);

/* Initialize the connection by writing the protocol version. This must be
 * called before using any other API. */
int clientSendHandshake(struct client_proto *c);

/* Send a request to open a database */
int clientSendOpen(struct client_proto *c, const char *name);

/* Receive the response to an open request. */
int clientRecvDb(struct client_proto *c);

/* Send a request to prepare a statement. */
int clientSendPrepare(struct client_proto *c, const char *sql);

/* Receive the response to a prepare request. */
int clientRecvStmt(struct client_proto *c, unsigned *stmt_id);

/* Send a request to execute a statement. */
int clientSendExec(struct client_proto *c, unsigned stmt_id,
			struct value *params, size_t n_params);

/* Send a request to execute a non-prepared statement. */
int clientSendExecSQL(struct client_proto *c, const char *sql,
			struct value *params, size_t n_params);

/* Receive the response to an exec request. */
int clientRecvResult(struct client_proto *c,
			unsigned *last_insert_id,
			unsigned *rows_affected);

/* Send a request to perform a query. */
int clientSendQuery(struct client_proto *c, unsigned stmt_id,
			struct value *params, size_t n_params);

/* Send a request to perform a non-prepared query. */
int clientSendQuerySQL(struct client_proto *c, const char *sql,
			struct value *params, size_t n_params);

/* Receive the response of a query request. */
int clientRecvRows(struct client_proto *c, struct rows *rows);

/* Release all memory used in the given rows object. */
void clientCloseRows(struct rows *rows);

/* Send a request to add a dqlite node. */
int clientSendAdd(struct client_proto *c, unsigned id, const char *address);

/* Send a request to assign a role to a node. */
int clientSendAssign(struct client_proto *c, unsigned id, int role);

/* Send a request to remove a server from the cluster. */
int clientSendRemove(struct client_proto *c, unsigned id);

/* Send a request to transfer leadership to node with id `id`. */
int clientSendTransfer(struct client_proto *c, unsigned id);

/* Receive an empty response. */
int clientRecvEmpty(struct client_proto *c);

/* Receive a failure response. */
int clientRecvFailure(struct client_proto *c, uint64_t *code, const char **msg);

#endif /* DQLITE_CLIENT_PROTOCOL_H_ */
