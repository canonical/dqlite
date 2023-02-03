#include "../../include/dqlite/client.h"
#include "../leader.h"
#include "../lib/addr.h"
#include "../server.h"
#include "protocol.h"

#include <stdlib.h>
#include <stdio.h>

#define MAGIC_BOOTSTRAP_ID 1

enum {
	DQLITE_STMT_START, /* request not yet send */
	DQLITE_STMT_PARTIAL, /* request sent, EOF not received */
	DQLITE_STMT_DONE /* EOF received */
};

struct dqlite
{
	struct client_proto leader;
	struct client_node_info *cluster;
	size_t n_cluster;
	unsigned local_index;
	unsigned leader_index;
	dqlite_node *local;
};

struct dqlite_stmt
{
	uint32_t id;
	uint64_t n_params;
	struct value *params;
	struct rows rows;
	struct row *next_row;
	dqlite *db;
	struct client_proto leader;
	int state;
};

static int connectAndHandshake(struct client_proto *proto, const char *addr, struct client_context *context)
{
	struct sockaddr sockaddr;
	socklen_t sockaddr_len = sizeof sockaddr;
	int sock;
	int rv;

	rv = AddrParse(addr, &sockaddr, &sockaddr_len, NULL, 0);
	if (rv != 0) {
		abort(); /* TODO */
	}

	sock = socket(sockaddr.sa_family, SOCK_STREAM, 0);
	if (sock < 0) {
		abort(); /* TODO */
	}
	rv = connect(sock, &sockaddr, sockaddr_len);
	if (rv != 0) {
		abort(); /* TODO */
	}

	clientInit(proto, sock);
	rv = clientSendHandshake(proto, context);
	if (rv != 0) {
		abort(); /* TODO */
	}
	return 0;
}

static int connectHandshakeAndOpen(struct client_proto *proto, const char *addr, const char *name, struct client_context *context)
{
	int rv;

	rv = connectAndHandshake(proto, addr, context);
	if (rv != 0) {
		abort(); /* TODO */
	}
	rv = clientSendOpen(proto, name, context);
	if (rv != 0) {
		abort(); /* TODO */
	}
	rv = clientRecvDb(proto, context);
	if (rv != 0) {
		abort(); /* TODO */
	}
	return 0;
}

int dqlite_open(const char *dir, const char *name, dqlite **db, const char *const *addrs, unsigned n_addrs, unsigned me)
{
	unsigned i;
	dqlite_node *node;
	struct dqlite *database;
	struct client_context context = {5000};
	struct client_proto local;
	bool joining;
	int rv;

	assert(n_addrs > 0);

	struct client_node_info *cluster = callocChecked(n_addrs, sizeof *cluster);
	for (i = 0; i < n_addrs; ++i) {
		cluster[i].addr = strdupChecked(addrs[i]);
		cluster[i].id = (i == 0) ? MAGIC_BOOTSTRAP_ID : dqlite_generate_node_id(addrs[i]);
	}

	rv = dqlite_node_create(cluster[me].id, cluster[me].addr, dir, &node);
	if (rv != 0) {
		abort(); /* TODO */
	}
	rv = dqlite_node_set_bind_address(node, cluster[me].addr);
	if (rv != 0) {
		abort(); /* TODO */
	}
	rv = dqlite_node_start(node);
	if (rv != 0) {
		abort(); /* TODO */
	}

	rv = connectAndHandshake(&local, cluster[me].addr, &context);
	if (rv != 0) {
		abort(); /* TODO */
	}
	joining = node->raft.current_term == 0;
	clientClose(&local);

	database = mallocChecked(sizeof *database);
	rv = connectHandshakeAndOpen(&database->leader, cluster[0].addr, name, &context);
	if (rv != 0) {
		abort(); /* TODO */
	}
	if (joining) {
		rv = clientSendAdd(&database->leader, cluster[me].id, cluster[me].addr, &context);
		if (rv != 0) {
			abort(); /* TODO */
		}
		rv = clientRecvEmpty(&database->leader, &context);
		if (rv != 0) {
			abort(); /* TODO */
		}
		rv = clientSendAssign(&database->leader, cluster[me].id, DQLITE_VOTER, &context);
		if (rv != 0) {
			abort(); /* TODO */
		}
		rv = clientRecvEmpty(&database->leader, &context);
		if (rv != 0) {
			abort(); /* TODO */
		}
	}

	database->cluster = cluster;
	database->n_cluster = n_addrs;
	database->local_index = me;
	database->local = node;
	database->leader_index = 0;
	*db = database;
	return 0;
}

static int askForLeader(struct client_proto *proto, uint64_t *id, char **addr, struct client_context *context)
{
	int rv;

	rv = clientSendLeader(proto, context);
	if (rv != 0) {
		abort(); /* TODO */
	}
	rv = clientRecvServer(proto, id, addr, context);
	if (rv != 0) {
		abort(); /* TODO */
	}
	return 0;
}

static int refreshLeader(dqlite *db, struct client_context *context)
{
	uint64_t id;
	char *addr;
	unsigned i;
	struct client_proto proto;
	char *name;
	int rv;

	/* First, try the last known leader */
	rv = askForLeader(&db->leader, &id, &addr, context);
	if (rv != 0) {
		/* Failing that, poll the rest of the cluster */
		for (i = 0; i < db->n_cluster; ++i) {
			if (i == db->leader_index) {
				/* Don't poll the last known leader again */
				continue;
			}

			rv = connectAndHandshake(&proto, db->cluster[i].addr, context);
			if (rv != 0) {
				/* Try the next server */
				continue;
			}

			rv = askForLeader(&proto, &id, &addr, context);
			if (rv != 0) {
				/* Try the next server */
				clientClose(&proto);
				continue;
			}

			clientClose(&proto);
			break;
		}
		if (i == db->n_cluster) {
			/* All requests failed! */
			abort(); /* TODO */
		}
	}


	/* Save the DB name so we can re-open it on the new leader */
	name = db->leader.db_name;
	db->leader.db_name = NULL;
	clientClose(&db->leader);
	
	/* Connect to the new leader */
	rv = connectHandshakeAndOpen(&db->leader, addr, name, context);
	if (rv != 0) {
		abort(); /* TODO */
	}
	for (i = 0; i < db->n_cluster; ++i) {
		if (id == db->cluster[i].id) {
			break;
		}
	}
	assert(i < db->n_cluster);
	db->leader_index = i;

	free(addr);
	free(name);
	return 0;
}

int dqlite_prepare(dqlite *db, const char *sql, int sql_len, dqlite_stmt **stmt, const char **tail)
{
	struct client_context context = {5000};
	struct dqlite_stmt *statement;
	char *owned_sql;
	uint32_t id;
	uint64_t n_params;
	uint64_t offset;
	int rv;

	if (sql_len < 0) {
		sql_len = (int)strlen(sql);
		assert(sql_len >= 0);
		assert((size_t)sql_len == strlen(sql));
	} else  {
		sql_len = (int)strnlen(sql, (size_t)sql_len);
	}
	owned_sql = strndupChecked(sql, (size_t)sql_len);

	rv = clientSendPrepare(&db->leader, owned_sql, &context);
	free(owned_sql);
	if (rv != 0) {
		abort(); /* TODO */
	}

	rv = clientRecvStmt(&db->leader, &id, &n_params, &offset, &context);
	if (rv == DQLITE_CLIENT_PROTO_RECEIVED_FAILURE &&
			db->leader.errcode == SQLITE_IOERR_NOT_LEADER) {
		rv = refreshLeader(db, &context);
		if (rv != 0) {
			abort(); /* TODO */
		}
		rv = clientSendPrepare(&db->leader, sql, &context);
		if (rv != 0) {
			abort(); /* TODO */
		}
		rv = clientRecvStmt(&db->leader, &id, &n_params, &offset, &context);
	}
	if (rv != 0) {
		abort(); /* TODO */
	}

	statement = mallocChecked(sizeof *statement);

	statement->leader = db->leader;
	memset(&db->leader, 0, sizeof db->leader);
	rv = connectHandshakeAndOpen(&db->leader, db->cluster[db->leader_index].addr, statement->leader.db_name, &context);
	if (rv != 0) {
		abort(); /* TODO */
	}

	statement->id = id;
	statement->n_params = n_params;
	statement->params = callocChecked(n_params, sizeof *statement->params);
	memset(&statement->rows, 0, sizeof statement->rows);
	statement->next_row = NULL;
	statement->db = db;
	statement->state = DQLITE_STMT_START;
	*stmt = statement;
	if (tail != NULL) {
		*tail = sql + offset;
	}
	return 0;
}

int dqlite_bind_blob64(dqlite_stmt *stmt, int index, const void *blob, uint64_t blob_len, void (*dealloc)(void *))
{
	struct value value;
	void *p;

	if (index <= 0 || (uint64_t)index > stmt->n_params) {
		abort(); /* TODO */
	}
	if (stmt->params[index].type != 0) {
		abort(); /* TODO */
	}
	if (dealloc != SQLITE_TRANSIENT) {
		abort(); /* TODO */
	}
	p = mallocChecked(blob_len);
	memcpy(p, blob, blob_len);
	value.type = SQLITE_BLOB;
	value.blob.base = p;
	value.blob.len = blob_len;
	stmt->params[index] = value;
	return 0;
}

int dqlite_bind_double(dqlite_stmt *stmt, int index, double val)
{
	struct value value;

	if (index <= 0 || (uint64_t)index > stmt->n_params) {
		abort(); /* TODO */
	}
	if (stmt->params[index].type != 0) {
		abort(); /* TODO */
	}
	value.type = SQLITE_FLOAT;
	value.float_ = val;
	stmt->params[index] = value;
	return 0;
}

int dqlite_bind_int64(dqlite_stmt *stmt, int index, int64_t val)
{
	struct value value;

	if (index <= 0 || (uint64_t)index > stmt->n_params) {
		abort(); /* TODO */
	}
	if (stmt->params[index].type != 0) {
		abort(); /* TODO */
	}
	value.type = SQLITE_INTEGER;
	value.integer = val;
	stmt->params[index] = value;
	return 0;
}

int dqlite_bind_null(dqlite_stmt *stmt, int index)
{
	struct value value;

	value.type = SQLITE_NULL;
	value.null = 0;
	stmt->params[index] = value;
	return 0;
}

int dqlite_bind_text64(dqlite_stmt *stmt, int index, const char *text, uint64_t text_len, void (*dealloc)(void *), unsigned char encoding)
{
	struct value value;
	char *p;

	if (index <= 0 || (uint64_t)index > stmt->n_params) {
		abort(); /* TODO */
	}
	if (stmt->params[index].type != 0) {
		abort(); /* TODO */
	}
	if (dealloc != SQLITE_TRANSIENT) {
		abort(); /* TODO */
	}
	if (encoding != SQLITE_UTF8) {
		abort(); /* TODO */
	}
	p = strndupChecked(text, text_len);
	value.type = SQLITE_TEXT;
	value.text = p;
	stmt->params[index] = value;
	return 0;
}

static int maybeFetchRows(dqlite_stmt *stmt, struct client_context *context)
{
	bool done;
	int rv;

	if (stmt->next_row == NULL) {
		if (stmt->state == DQLITE_STMT_DONE) {
			return SQLITE_DONE;
		}
		rv = clientRecvRows(&stmt->leader, &stmt->rows, &done, context);
		if (rv != 0) {
			abort(); /* TODO */
		}
		stmt->state = done ? DQLITE_STMT_DONE : DQLITE_STMT_PARTIAL;
		stmt->next_row = stmt->rows.next;
	}
	if (stmt->next_row == NULL) {
		assert(stmt->state == DQLITE_STMT_DONE);
		return SQLITE_DONE;
	}
	return SQLITE_ROW;
}

int dqlite_step(dqlite_stmt *stmt)
{
	struct client_context context = {5000};
	uint64_t i;
	int rv;

	if (stmt->state == DQLITE_STMT_START) {
		for (i = 0; i < stmt->n_params; ++i) {
			if (stmt->params[i].type == 0) {
				abort(); /* TODO */
			}
		}
		rv = clientSendQuery(&stmt->leader, stmt->id, stmt->params, stmt->n_params, &context);
		if (rv != 0) {
			abort(); /* TODO */
		}
		stmt->state = DQLITE_STMT_PARTIAL;
		rv = maybeFetchRows(stmt, &context);
		if (rv != SQLITE_ROW && rv != SQLITE_DONE) {
			abort(); /* TODO */
		}
		return rv;
	}

	if (stmt->next_row == NULL) {
		assert(stmt->state == DQLITE_STMT_DONE);
		return SQLITE_DONE;
	}
	stmt->next_row = stmt->next_row->next;
	rv = maybeFetchRows(stmt, &context);
	if (rv != SQLITE_ROW && rv != SQLITE_DONE) {
		abort(); /* TODO */
	}
	return rv;
}

int dqlite_reset(dqlite_stmt *stmt)
{
	struct client_context context = {5000};
	int rv;

	if (stmt->state == DQLITE_STMT_PARTIAL) {
		rv = clientSendInterrupt(&stmt->leader, &context);
		if (rv != 0) {
			abort(); /* TODO */
		}
		rv = clientRecvEmpty(&stmt->leader, &context);
		if (rv != 0) {
			abort(); /* TODO */
		}
	}

	memset(&stmt->rows, 0, sizeof stmt->rows);
	stmt->next_row = NULL;
	stmt->state = DQLITE_STMT_START;
	return 0;
}

int dqlite_finalize(dqlite_stmt *stmt)
{
	struct client_context context = {5000};
	uint64_t i;
	int rv = 0;

	rv = clientSendFinalize(&stmt->leader, stmt->id, &context);
	if (rv == 0) {
		rv = clientRecvEmpty(&stmt->leader, &context);
	}

	for (i = 0; i < stmt->n_params; ++i) {
		freeOwnedValue(stmt->params[i]);
	}
	free(stmt->params);
	clientCloseRows(&stmt->rows);
	clientClose(&stmt->leader);
	free(stmt);
	return rv;
}

int dqlite_exec(dqlite *db, const char *sql, int (*cb)(void *, int, char **, char **), void *cb_data, char **errmsg)
{
	dqlite_stmt *stmt;
	int rv;

	if (cb != NULL || cb_data != NULL || errmsg != NULL) {
		abort(); /* TODO */
	}

	rv = dqlite_prepare(db, sql, -1, &stmt, NULL);
	if (rv != 0) {
		abort(); /* TODO */
	}
	do {
		rv = dqlite_step(stmt);
	} while (rv == SQLITE_ROW);
	if (rv != SQLITE_DONE) {
		abort(); /* TODO */
	}
	rv = dqlite_finalize(stmt);
	if (rv != 0) {
		abort(); /* TODO */
	}
	return 0;
}

void dqlite_close(dqlite *db)
{
	unsigned i;

	for (i = 0; i < db->n_cluster; ++i) {
		free(db->cluster[i].addr);
	}
	free(db->cluster);
	dqlite_node_destroy(db->local);
	free(db);
}
