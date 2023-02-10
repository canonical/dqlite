/* Core dqlite client logic for encoding requests and decoding responses. */

#ifndef DQLITE_CLIENT_PROTOCOL_H_
#define DQLITE_CLIENT_PROTOCOL_H_

#include "../lib/buffer.h"

#include "../tuple.h"

/* All functions declared in this header file return 0 for success or one
 * of the follow error codes on failure. */
enum {
	/* We received a FAILURE response when we expected another response.
	 *
	 * The data carried by the FAILURE response can be retrieved from the
	 * errcode and errmsg fields of struct client_proto.
	 *
	 * It's safe to continue using the client_proto object after receiving
	 * this error code. */
	DQLITE_CLIENT_PROTO_RECEIVED_FAILURE = 1,
	/* We timed out while reading from or writing to our fd, or a read/write
	 * returned EOF before the expected number of bytes were read/written.
	 *
	 * It is not generally safe to continue using the client_proto object
	 * after receiving this error code. */
	DQLITE_CLIENT_PROTO_SHORT,
	/* Another kind of error occurred, like a syscall or malloc failure.
	 *
	 * It is not generally safe to continue using the client_proto object
	 * after receiving this error code. */
	DQLITE_CLIENT_PROTO_ERROR
};

struct client_proto
{
	int fd;		     /* Connected socket */
	uint32_t db_id;      /* Database ID provided by the server */
	char *db_name;       /* Database filename (owned) */
	bool db_is_init;     /* Whether the database ID has been initialized */
	struct buffer read;  /* Read buffer */
	struct buffer write; /* Write buffer */
	uint64_t errcode;    /* Last error code returned by the server (owned) */
	char *errmsg;        /* Last error string returned by the server */
};

/* All of the Send and Recv functions take an `struct client_context *context`
 * argument, which controls timeouts for read and write operations (and possibly
 * other knobs in the future).
 *
 * Passing NULL for the context argument is permitted and disables all timeouts. */
struct client_context
{
	/* If budget_millis is negative when a Send or Recv function is called, reading
	 * or writing may block indefinitely and the value is not modified. Otherwise,
	 * the initial value caps the number of milliseconds that will be spent attempting
	 * the send or receive operation (potentially split between multiple read/write syscalls).
	 * If it's not negative initially, budget_millis is modified by subtracting the
	 * number of milliseconds actually spent. If the time budget runs out without
	 * completing the operation, DQLITE_CLIENT_PROTO_SHORT is returned and the values
	 * of any output parameters are undefined. Because this implies that we failed to
	 * read/write a complete message from/to the fd, it's important to call clientClose
	 * immediately and not keep using the client_proto object. */
	int budget_millis;
};

struct row
{
	struct value *values;
	struct row *next;
};

struct rows
{
	unsigned column_count;
	char **column_names;
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
int clientSendHandshake(struct client_proto *c, struct client_context *context);

/* Send a request to get the current leader. */
int clientSendLeader(struct client_proto *c, struct client_context *context);

/* Send a request identifying this client to the attached server. */
int clientSendClient(struct client_proto *c, uint64_t id, struct client_context *context);

/* Send a request to open a database */
int clientSendOpen(struct client_proto *c, const char *name, struct client_context *context);

/* Receive the response to an open request. */
int clientRecvDb(struct client_proto *c, struct client_context *context);

/* Send a request to prepare a statement. */
int clientSendPrepare(struct client_proto *c, const char *sql, struct client_context *context);

/* Receive the response to a prepare request. */
int clientRecvStmt(struct client_proto *c,
			uint32_t *stmt_id,
			uint64_t *offset,
			struct client_context *context);

/* Send a request to execute a statement. */
int clientSendExec(struct client_proto *c, uint32_t stmt_id,
			struct value *params, size_t n_params,
			struct client_context *context);

/* Send a request to execute a non-prepared statement. */
int clientSendExecSQL(struct client_proto *c, const char *sql,
			struct value *params, size_t n_params,
			struct client_context *context);

/* Receive the response to an exec request. */
int clientRecvResult(struct client_proto *c,
			uint64_t *last_insert_id,
			uint64_t *rows_affected,
			struct client_context *context);

/* Send a request to perform a query. */
int clientSendQuery(struct client_proto *c, uint32_t stmt_id,
			struct value *params, size_t n_params,
			struct client_context *context);

/* Send a request to perform a non-prepared query. */
int clientSendQuerySQL(struct client_proto *c, const char *sql,
			struct value *params, size_t n_params,
			struct client_context *context);

/* Receive the response of a query request. */
int clientRecvRows(struct client_proto *c, struct rows *rows, struct client_context *context);

/* Release all memory used in the given rows object. */
void clientCloseRows(struct rows *rows);

/* Send a request to interrupt a server that's sending rows. */
int clientSendInterrupt(struct client_proto *c, struct client_context *context);

/* Send a request to finalize a prepared statement. */
int clientSendFinalize(struct client_proto *c, uint32_t stmt_id, struct client_context *context);

/* Send a request to add a dqlite node. */
int clientSendAdd(struct client_proto *c,
			uint64_t id,
			const char *address,
			struct client_context *context);

/* Send a request to assign a role to a node. */
int clientSendAssign(struct client_proto *c,
			uint64_t id,
			int role,
			struct client_context *context);

/* Send a request to remove a server from the cluster. */
int clientSendRemove(struct client_proto *c, uint64_t id, struct client_context *context);

/* Send a request to dump the contents of the attached database. */
int clientSendDump(struct client_proto *c, struct client_context *context);

/* Send a request to list the nodes of the cluster with their addresses and roles. */
int clientSendCluster(struct client_proto *c, struct client_context *context);

/* Send a request to transfer leadership to node with id `id`. */
int clientSendTransfer(struct client_proto *c, uint64_t id, struct client_context *context);

/* Send a request to retrieve metadata about the attached server. */
int clientSendDescribe(struct client_proto *c, struct client_context *context);

/* Send a request to set the weight metadata for the attached server. */
int clientSendWeight(struct client_proto *c, uint64_t weight, struct client_context *context);

/* Receive a response with the ID and address of a single node. */
int clientRecvServer(struct client_proto *c,
			uint64_t *id,
			char **address,
			struct client_context *context);

/* Receive a "welcome" handshake response. */
int clientRecvWelcome(struct client_proto *c, struct client_context *context);

/* Receive an empty response. */
int clientRecvEmpty(struct client_proto *c, struct client_context *context);

/* Receive a failure response. */
int clientRecvFailure(struct client_proto *c,
			uint64_t *code,
			const char **msg,
			struct client_context *context);

/* Receive a list of nodes in the cluster. */
int clientRecvServers(struct client_proto *c,
			struct client_node_info **servers,
			size_t *n_servers,
			struct client_context *context);

/* Receive a list of files that make up a database. */
int clientRecvFiles(struct client_proto *c,
			struct client_file **files,
			size_t *n_files,
			struct client_context *context);

/* Receive metadata for a single server. */
int clientRecvMetadata(struct client_proto *c,
			uint64_t *failure_domain,
			uint64_t *weight,
			struct client_context *context);

#endif /* DQLITE_CLIENT_PROTOCOL_H_ */
