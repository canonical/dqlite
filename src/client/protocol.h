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

struct client_node_info
{
	uint64_t id;
	char *addr;
	int role;
};

struct client_file
{
	char *name;
	uint64_t size;
	void *blob;
};

/* Initialize a new client, writing requests to fd. */
int clientInit(struct client_proto *c, int fd);

/* Release all memory used by the client, and close the client socket. */
void clientClose(struct client_proto *c);

/* Initialize the connection by writing the protocol version. This must be
 * called before using any other API. */
int clientSendHandshake(struct client_proto *c);

/* Send a request to get the current leader. */
int clientSendLeader(struct client_proto *c);

/* Send a request identifying this client to the attached server. */
int clientSendClient(struct client_proto *c, uint64_t id);

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

/* Send a request to interrupt a server that's sending rows. */
int clientSendInterrupt(struct client_proto *c);

/* Send a request to finalize a prepared statement. */
int clientSendFinalize(struct client_proto *c, unsigned stmt_id);

/* Send a request to add a dqlite node. */
int clientSendAdd(struct client_proto *c, unsigned id, const char *address);

/* Send a request to assign a role to a node. */
int clientSendAssign(struct client_proto *c, unsigned id, int role);

/* Send a request to remove a server from the cluster. */
int clientSendRemove(struct client_proto *c, unsigned id);

/* Send a request to dump the contents of the attached database. */
int clientSendDump(struct client_proto *c);

/* Send a request to list the nodes of the cluster with their addresses and roles. */
int clientSendCluster(struct client_proto *c);

/* Send a request to transfer leadership to node with id `id`. */
int clientSendTransfer(struct client_proto *c, unsigned id);

/* Send a request to retrieve metadata about the attached server. */
int clientSendDescribe(struct client_proto *c);

/* Send a request to set the weight metadata for the attached server. */
int clientSendWeight(struct client_proto *c, unsigned weight);

/* Receive a response with the ID and address of a single node. */
int clientRecvServer(struct client_proto *c, uint64_t *id, char **address);

/* Receive a "welcome" handshake response. */
int clientRecvWelcome(struct client_proto *c);

/* Receive an empty response. */
int clientRecvEmpty(struct client_proto *c);

/* Receive a failure response. */
int clientRecvFailure(struct client_proto *c, uint64_t *code, const char **msg);

/* Receive a list of nodes in the cluster. */
int clientRecvServers(struct client_proto *c, struct client_node_info **servers, size_t *n_servers);

/* Receive a list of files that make up a database. */
int clientRecvFiles(struct client_proto *c, struct client_file **files, size_t *n_files);

/* Receive metadata for a single server. */
int clientRecvMetadata(struct client_proto *c, unsigned *failure_domain, unsigned *weight);

#endif /* DQLITE_CLIENT_PROTOCOL_H_ */
