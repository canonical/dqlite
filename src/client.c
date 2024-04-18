#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>

#include "client.h"
#include "client/protocol.h"
#include "src/tracing.h"

static int client_connect_to_some_server(struct client_proto *proto,
					 struct node_store_cache *cache,
					 struct client_context *context)
{
	int rv;

	for (unsigned int i = 0; i < cache->len; i++) {
		struct client_node_info node = cache->nodes[i];
		if (clientOpen(proto, node.addr, node.id) != 0) {
			continue;
		}
		rv = clientSendHandshake(proto, context);
		if (rv != SQLITE_OK) {
			clientClose(proto);
			continue;
		} else {
			return SQLITE_OK;
		}
	}

	return SQLITE_ERROR;
}

static int client_get_leader_and_open(struct client_proto *proto,
				      char *db_name,
				      struct client_context *context)
{
	int rv;
	// Get the leader from the server we connected to.
	rv = clientSendLeader(proto, context);
	if (rv != SQLITE_OK) {
		return rv;
	}
	uint64_t server_id;
	char *address;
	rv = clientRecvServer(proto, &server_id, &address, context);
	if (rv != SQLITE_OK) {
		return rv;
	}
	clientClose(proto);

	// Connect to the leader and open the db.
	rv = clientOpen(proto, address, server_id);
	if (rv != SQLITE_OK) {
		free(address);
		return rv;
	}
	free(address);
	address = NULL;
	rv = clientSendHandshake(proto, context);
	if (rv != SQLITE_OK) {
		return rv;
	}
	rv = clientSendOpen(proto, db_name, context);
	if (rv != SQLITE_OK) {
		return rv;
	}
	rv = clientRecvDb(proto, context);
	if (rv != SQLITE_OK) {
		return rv;
	}

	return SQLITE_OK;
}

// TODO remove DQLITE_VISIBLE_TO_TESTS from the client* functions.
// TODO it acceps the dqlite_server in order to tie the lifetime of the client
// and server. Why though? If we are not freeing any of them when we finish.
// TODO why have here flags if they are not used?
int dqlite_open(dqlite_server *server, const char *name, dqlite **db, int flags)
{
	(void)flags;
	*db = callocChecked(1, sizeof(dqlite));
	(*db)->name = strdupChecked(name);
	(*db)->server = server;
	return SQLITE_OK;
}

int dqlite_close(dqlite *db)
{
	free(db->name);
	free(db);
	return SQLITE_OK;
}

// TODO what happens if the leader changes, do we have a retry strategy?
int dqlite_prepare(dqlite *db,
		   const char *sql,
		   int sql_len,
		   dqlite_stmt **stmt,
		   const char **tail)
{
	int rv;
	struct client_proto proto = { 0 };
	// TODO update client_proto in db->server?
	proto.connect = db->server->connect;
	proto.connect_arg = db->server->connect_arg;
	struct client_context context = { 0 };
	// TODO Why 5000? Eventually add function to configure it. Maybe add a
	// dqlite_options argument.
	// TODO CLOCK_MONOTONIC
	clientContextMillis(&context, 500000);

	bool connected = false;
	while (!connected) {
		struct timespec now = { 0 };
		rv = clock_gettime(CLOCK_REALTIME, &now);
		assert(rv == 0);
		long long millis =
		    (context.deadline.tv_sec - now.tv_sec) * 1000 +
		    (context.deadline.tv_nsec - now.tv_nsec) / 1000000;
		if (millis <= 0) {
			break;
		}
		rv = pthread_mutex_lock(&db->server->mutex);
		assert(rv == 0);
		// Connect to any server.
		connected =
		    client_connect_to_some_server(&proto, &db->server->cache,
						  &context) == SQLITE_OK;
		rv = pthread_mutex_unlock(&db->server->mutex);
		assert(rv == 0);
		if (client_get_leader_and_open(&proto, db->name, &context) !=
		    SQLITE_OK) {
			clientClose(&proto);
			return SQLITE_ERROR;
		}
	}

	// Run the statement in the leader node.
	// TODO check zero length.
	size_t sql_owned_len = sql_len >= 0 ? (size_t)sql_len : strlen(sql);
	const char *sql_owned = strndupChecked(sql, sql_owned_len);
	if (tail != NULL) {
		*tail = sql + sql_owned_len;
	}
	rv = clientSendPrepare(&proto, sql_owned, &context);
	free((void *)sql_owned);
	if (rv != SQLITE_OK) {
		clientClose(&proto);
		return SQLITE_ERROR;
	}
	*stmt = callocChecked(1, sizeof(**stmt));
	rv = clientRecvStmt(&proto, &(*stmt)->stmt_id, &(*stmt)->n_params,
			    &(*stmt)->offset, &context);
	if (rv != SQLITE_OK) {
		free(*stmt);
		clientClose(&proto);
		return SQLITE_ERROR;
	}
	(*stmt)->proto = proto;

	return SQLITE_OK;
}

int dqlite_finalize(dqlite_stmt *stmt)
{
	struct client_context context;

	if (stmt == NULL) {
		return SQLITE_OK;
	}
	// TODO add options.
	clientContextMillis(&context, 500000);
	if (clientSendFinalize(&stmt->proto, stmt->stmt_id, &context) !=
	    SQLITE_OK) {
		return SQLITE_ERROR;
	}
	if (clientRecvEmpty(&stmt->proto, &context) != SQLITE_OK) {
		return SQLITE_ERROR;
	}
	clientClose(&stmt->proto);
	free(stmt);
	return SQLITE_OK;
}
