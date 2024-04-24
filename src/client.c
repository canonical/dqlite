#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>

#include "client.h"
#include "client/protocol.h"
#include "include/dqlite.h"
#include "src/tracing.h"

static int client_connect_to_some_server(struct client_proto *proto,
					 struct node_store_cache *cache,
					 struct client_context *context)
{
	int rv;

	for (unsigned i = 0; i < cache->len; i++) {
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
	/* Get the leader from the server we connected to. */
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

	/* Connect to the leader and open the db. */
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

// TODO Move flags into dqlite_options?
// TODO Should all functions receive dqlite_options for future compatibility?
//
// TODO it acceps the dqlite_server in order to tie the lifetime of the client
// and server. Why though? If we are not freeing any of them when we finish.
int dqlite_open(dqlite_server *server,
		const char *name,
		dqlite **db,
		int flags,
		dqlite_options *options)
{
	(void)flags;
	(void)options;
	*db = callocChecked(1, sizeof(**db));
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

int dqlite_prepare(dqlite *db,
		   const char *sql,
		   int sql_len,
		   dqlite_stmt **stmt,
		   const char **tail,
		   dqlite_options *options)
{
	int rv;
	struct client_proto proto = { 0 };
	// TODO update db->server->proto?
	proto.connect = db->server->connect;
	proto.connect_arg = db->server->connect_arg;
	// TODO CLOCK_MONOTONIC

	/* Retry until success or context expires. */
	while (true) {
		/* Check if context has expired. */
		struct timespec now = { 0 };
		rv = clock_gettime(CLOCK_REALTIME, &now);
		assert(rv == 0);
		long long millis =
		    (options->context->deadline.tv_sec - now.tv_sec) * 1000 +
		    (options->context->deadline.tv_nsec - now.tv_nsec) /
			1000000;
		if (millis <= 0) {
			return SQLITE_ERROR;
		}
		rv = pthread_mutex_lock(&db->server->mutex);
		assert(rv == 0);

		/* Connect to any server. */
		bool connected = SQLITE_OK == client_connect_to_some_server(
						  &proto, &db->server->cache,
						  options->context);
		rv = pthread_mutex_unlock(&db->server->mutex);
		assert(rv == 0);
		if (!connected) {
			clientClose(&proto);
			continue;
		}
		if (client_get_leader_and_open(&proto, db->name,
					       options->context) != SQLITE_OK) {
			clientClose(&proto);
			continue;
		}

		/* Run the statement in the leader node. */
		size_t sql_owned_len =
		    sql_len >= 0 ? (size_t)sql_len : strlen(sql);
		const char *sql_owned = strndupChecked(sql, sql_owned_len);
		rv = clientSendPrepare(&proto, sql_owned, options->context);
		free((void *)sql_owned);
		if (rv != SQLITE_OK) {
			clientClose(&proto);
			continue;
		}
		*stmt = callocChecked(1, sizeof(**stmt));
		rv = clientRecvStmt(&proto, &(*stmt)->stmt_id,
				    &(*stmt)->n_params, &(*stmt)->offset,
				    options->context);
		if (rv != SQLITE_OK) {
			free(*stmt);
			clientClose(&proto);
			continue;
		}
		if (tail != NULL) {
			*tail = sql + (*stmt)->offset;
		}
		(*stmt)->proto = proto;

		return SQLITE_OK;
	}
}

// TODO should the options be a ptr type? What is the convention?
int dqlite_finalize(dqlite_stmt *stmt, dqlite_options *options)
{
	if (stmt == NULL) {
		return SQLITE_OK;
	}
	if (clientSendFinalize(&stmt->proto, stmt->stmt_id, options->context) !=
	    SQLITE_OK) {
		return SQLITE_ERROR;
	}
	if (clientRecvEmpty(&stmt->proto, options->context) != SQLITE_OK) {
		return SQLITE_ERROR;
	}
	clientClose(&stmt->proto);
	free(stmt);
	return SQLITE_OK;
}
