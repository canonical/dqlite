/* Core dqlite client logic for encoding requests and decoding responses. */

#ifndef DQLITE_CLIENT_PROTOCOL_H_
#define DQLITE_CLIENT_PROTOCOL_H_

#include "../../include/dqlite.h"

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
	/* Another kind of error occurred, like a syscall failure.
	 *
	 * It is not generally safe to continue using the client_proto object
	 * after receiving this error code. */
	DQLITE_CLIENT_PROTO_ERROR
};

struct client_proto
{
	/* TODO find a better approach to initializing these fields? */
	int (*connect)(void *, const char *, int *);
	void *connect_arg;
	int fd;          /* Connected socket */
	uint32_t db_id;  /* Database ID provided by the server */
	char *db_name;   /* Database filename (owned) */
	bool db_is_init; /* Whether the database ID has been initialized */
	uint64_t server_id;
	struct buffer read;  /* Read buffer */
	struct buffer write; /* Write buffer */
	uint64_t errcode; /* Last error code returned by the server (owned) */
	char *errmsg;     /* Last error string returned by the server */
};

/* All of the Send and Recv functions take an `struct client_context *context`
 * argument, which controls timeouts for read and write operations (and possibly
 * other knobs in the future).
 *
 * Passing NULL for the context argument is permitted and disables all timeouts.
 */
struct client_context
{
	/* An absolute CLOCK_REALTIME timestamp that limits how long will be
	 * spent trying to complete the requested send or receive operation.
	 * Whenever we are about to make a blocking syscall (read or write), we
	 * first poll(2) using a timeout computed based on how much time remains
	 * before the deadline. If the poll times out, we return early instead
	 * of completing the operation. */
	struct timespec deadline;
};

/* TODO Consider using a dynamic array instead of a linked list here? */
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

/* Checked allocation functions that abort the process on allocation failure. */

void *mallocChecked(size_t n);
void *callocChecked(size_t nmemb, size_t size);
char *strdupChecked(const char *s);
char *strndupCheck(const char *s, size_t n);

/* Initialize a context whose deadline will fall after the given duration
 * in milliseconds. */
DQLITE_VISIBLE_TO_TESTS void clientContextMillis(struct client_context *context,
						 long millis);

/* Initialize a new client. */
DQLITE_VISIBLE_TO_TESTS int clientOpen(struct client_proto *c,
				       const char *addr,
				       uint64_t server_id);

/* Release all memory used by the client, and close the client socket. */
DQLITE_VISIBLE_TO_TESTS void clientClose(struct client_proto *c);

/* Initialize the connection by writing the protocol version. This must be
 * called before using any other API. */
DQLITE_VISIBLE_TO_TESTS int clientSendHandshake(struct client_proto *c,
						struct client_context *context);

/* Send a request to get the current leader. */
DQLITE_VISIBLE_TO_TESTS int clientSendLeader(struct client_proto *c,
					     struct client_context *context);

/* Send a request identifying this client to the attached server. */
DQLITE_VISIBLE_TO_TESTS int clientSendClient(struct client_proto *c,
					     uint64_t id,
					     struct client_context *context);

/* Send a request to open a database */
DQLITE_VISIBLE_TO_TESTS int clientSendOpen(struct client_proto *c,
					   const char *name,
					   struct client_context *context);

/* Receive the response to an open request. */
DQLITE_VISIBLE_TO_TESTS int clientRecvDb(struct client_proto *c,
					 struct client_context *context);

/* Send a request to prepare a statement. */
DQLITE_VISIBLE_TO_TESTS int clientSendPrepare(struct client_proto *c,
					      const char *sql,
					      struct client_context *context);

/* Receive the response to a prepare request. */
DQLITE_VISIBLE_TO_TESTS int clientRecvStmt(struct client_proto *c,
					   uint32_t *stmt_id,
					   uint64_t *n_params,
					   uint64_t *offset,
					   struct client_context *context);

/* Send a request to execute a statement. */
DQLITE_VISIBLE_TO_TESTS int clientSendExec(struct client_proto *c,
					   uint32_t stmt_id,
					   struct value *params,
					   unsigned n_params,
					   struct client_context *context);

/* Send a request to execute a non-prepared statement. */
DQLITE_VISIBLE_TO_TESTS int clientSendExecSQL(struct client_proto *c,
					      const char *sql,
					      struct value *params,
					      unsigned n_params,
					      struct client_context *context);

/* Receive the response to an exec request. */
DQLITE_VISIBLE_TO_TESTS int clientRecvResult(struct client_proto *c,
					     uint64_t *last_insert_id,
					     uint64_t *rows_affected,
					     struct client_context *context);

/* Send a request to perform a query. */
DQLITE_VISIBLE_TO_TESTS int clientSendQuery(struct client_proto *c,
					    uint32_t stmt_id,
					    struct value *params,
					    unsigned n_params,
					    struct client_context *context);

/* Send a request to perform a non-prepared query. */
DQLITE_VISIBLE_TO_TESTS int clientSendQuerySQL(struct client_proto *c,
					       const char *sql,
					       struct value *params,
					       unsigned n_params,
					       struct client_context *context);

/* Receive the response of a query request. */
DQLITE_VISIBLE_TO_TESTS int clientRecvRows(struct client_proto *c,
					   struct rows *rows,
					   bool *done,
					   struct client_context *context);

/* Release all memory used in the given rows object. */
DQLITE_VISIBLE_TO_TESTS void clientCloseRows(struct rows *rows);

/* Send a request to interrupt a server that's sending rows. */
DQLITE_VISIBLE_TO_TESTS int clientSendInterrupt(struct client_proto *c,
						struct client_context *context);

/* Send a request to finalize a prepared statement. */
DQLITE_VISIBLE_TO_TESTS int clientSendFinalize(struct client_proto *c,
					       uint32_t stmt_id,
					       struct client_context *context);

/* Send a request to add a dqlite node. */
DQLITE_VISIBLE_TO_TESTS int clientSendAdd(struct client_proto *c,
					  uint64_t id,
					  const char *address,
					  struct client_context *context);

/* Send a request to assign a role to a node. */
DQLITE_VISIBLE_TO_TESTS int clientSendAssign(struct client_proto *c,
					     uint64_t id,
					     int role,
					     struct client_context *context);

/* Send a request to remove a server from the cluster. */
DQLITE_VISIBLE_TO_TESTS int clientSendRemove(struct client_proto *c,
					     uint64_t id,
					     struct client_context *context);

/* Send a request to dump the contents of the attached database. */
DQLITE_VISIBLE_TO_TESTS int clientSendDump(struct client_proto *c,
					   struct client_context *context);

/* Send a request to list the nodes of the cluster with their addresses and
 * roles. */
DQLITE_VISIBLE_TO_TESTS int clientSendCluster(struct client_proto *c,
					      struct client_context *context);

/* Send a request to transfer leadership to node with id `id`. */
DQLITE_VISIBLE_TO_TESTS int clientSendTransfer(struct client_proto *c,
					       uint64_t id,
					       struct client_context *context);

/* Send a request to retrieve metadata about the attached server. */
DQLITE_VISIBLE_TO_TESTS int clientSendDescribe(struct client_proto *c,
					       struct client_context *context);

/* Send a request to set the weight metadata for the attached server. */
DQLITE_VISIBLE_TO_TESTS int clientSendWeight(struct client_proto *c,
					     uint64_t weight,
					     struct client_context *context);

/* Receive a response with the ID and address of a single node. */
DQLITE_VISIBLE_TO_TESTS int clientRecvServer(struct client_proto *c,
					     uint64_t *id,
					     char **address,
					     struct client_context *context);

/* Receive a "welcome" handshake response. */
DQLITE_VISIBLE_TO_TESTS int clientRecvWelcome(struct client_proto *c,
					      struct client_context *context);

/* Receive an empty response. */
DQLITE_VISIBLE_TO_TESTS int clientRecvEmpty(struct client_proto *c,
					    struct client_context *context);

/* Receive a failure response. */
DQLITE_VISIBLE_TO_TESTS int clientRecvFailure(struct client_proto *c,
					      uint64_t *code,
					      char **msg,
					      struct client_context *context);

/* Receive a list of nodes in the cluster. */
DQLITE_VISIBLE_TO_TESTS int clientRecvServers(struct client_proto *c,
					      struct client_node_info **servers,
					      uint64_t *n_servers,
					      struct client_context *context);

/* Receive a list of files that make up a database. */
DQLITE_VISIBLE_TO_TESTS int clientRecvFiles(struct client_proto *c,
					    struct client_file **files,
					    size_t *n_files,
					    struct client_context *context);

/* Receive metadata for a single server. */
DQLITE_VISIBLE_TO_TESTS int clientRecvMetadata(struct client_proto *c,
					       uint64_t *failure_domain,
					       uint64_t *weight,
					       struct client_context *context);

#endif /* DQLITE_CLIENT_PROTOCOL_H_ */
