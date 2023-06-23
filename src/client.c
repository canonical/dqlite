#include <stdlib.h>

#include "client.h"
#include "lib/alloc.h"
#include "server.h"

int dqlite_open(dqlite_server *server, const char *name, dqlite **db, int flags)
{
	(void)flags;
	*db = callocChecked(1, sizeof **db);
	(*db)->server = server;
	(*db)->name = strdupChecked(name);
	return SQLITE_OK;
}

enum { DQLITE_STEP_START, DQLITE_STEP_SENT_QUERY, DQLITE_STEP_GOT_EOF };

int dqlite_prepare(dqlite *db,
		   const char *sql,
		   int sql_len,
		   dqlite_stmt **stmt,
		   const char **tail)
{
	struct client_proto proto;
	struct client_context context;
	size_t sql_strlen;
	char *owned_sql;
	uint32_t stmt_id;
	uint64_t n_params;
	uint64_t prepared_len;
	unsigned i;
	int rv;

	clientContextMillis(&context, 5000);

	rv = pthread_mutex_lock(&db->server->mutex);
	assert(rv == 0);
	proto.connect = db->server->connect;
	proto.connect_arg = db->server->connect_arg;
	rv = clientConnectToSomeServer(&proto, db->server->cache.nodes,
				       db->server->cache.len, &context);
	pthread_mutex_unlock(&db->server->mutex);
	if (rv != 0) {
		goto err;
	}
	rv = clientTryReconnectToLeader(&proto, &context);
	if (rv != 0) {
		goto err_after_open_client;
	}

	rv = clientSendOpen(&proto, db->name, &context);
	if (rv != 0) {
		goto err_after_open_client;
	}
	rv = clientRecvDb(&proto, &context);
	if (rv != 0) {
		goto err_after_open_client;
	}

	if (sql_len < 0) {
		sql_strlen = SIZE_MAX;
	} else if (sql_len > 0) {
		sql_strlen = (size_t)sql_len - 1;
	} else {
		sql_strlen = 0;
	}
	owned_sql = strndupChecked(sql, sql_strlen);
	rv = clientSendPrepare(&proto, owned_sql, &context);
	free(owned_sql);
	if (rv != 0) {
		goto err_after_open_client;
	}
	rv = clientRecvStmt(&proto, &stmt_id, &n_params, &prepared_len,
			    &context);
	if (rv != 0) {
		goto err_after_open_client;
	}

	*stmt = callocChecked(1, sizeof **stmt);
	(*stmt)->db = db;
	(*stmt)->proto = proto;
	(*stmt)->id = stmt_id;
	(*stmt)->n_params = (unsigned)n_params;
	(*stmt)->params = callocChecked(n_params, sizeof *(*stmt)->params);
	for (i = 0; i < n_params; i += 1) {
		(*stmt)->params[i].inner.type = SQLITE_NULL;
	}
	(*stmt)->state = DQLITE_STEP_START;
	if (tail != NULL) {
		*tail = sql + prepared_len;
	}
	return SQLITE_OK;

err_after_open_client:
	clientClose(&proto);
err:
	return SQLITE_ERROR;
}

static int getMoreRows(struct dqlite_stmt *stmt, struct client_context *context)
{
	bool done;
	int rv;

	rv = clientRecvRows(&stmt->proto, &stmt->rows, &done, context);
	if (rv != 0) {
		return SQLITE_ERROR;
	}
	stmt->next_row = stmt->rows.next;
	stmt->state = done ? DQLITE_STEP_GOT_EOF : DQLITE_STEP_SENT_QUERY;
	return SQLITE_OK;
}

static void clearConvertedVals(struct dqlite_stmt *stmt)
{
	unsigned i;

	if (stmt->converted == NULL) {
		return;
	}

	for (i = 0; i < stmt->rows.column_count; i += 1) {
		sqlite3_free(stmt->converted[i]);
		stmt->converted[i] = NULL;
	}
}

int dqlite_step(dqlite_stmt *stmt)
{
	struct client_context context;
	int rv;

	clientContextMillis(&context, 5000);

	if (stmt->state == DQLITE_STEP_START) {
		rv = clientSendQueryGeneric(&stmt->proto, stmt->id,
					    stmt->params, stmt->n_params,
					    sizeof *stmt->params, &context);
		if (rv != 0) {
			return SQLITE_ERROR;
		}
		rv = getMoreRows(stmt, &context);
		if (rv != SQLITE_OK) {
			return rv;
		}
	} else if (stmt->next_row == NULL) {
		assert(stmt->state == DQLITE_STEP_GOT_EOF);
		return SQLITE_DONE;
	} else {
		clearConvertedVals(stmt);
		stmt->next_row = stmt->next_row->next;
	}

	if (stmt->next_row == NULL) {
		clientCloseRows(&stmt->rows);
		if (stmt->state == DQLITE_STEP_GOT_EOF) {
			return SQLITE_DONE;
		}
		rv = getMoreRows(stmt, &context);
		if (rv != SQLITE_OK) {
			return rv;
		}
	}

	return (stmt->next_row == NULL) ? SQLITE_DONE : SQLITE_ROW;
}

static void noopDealloc(void *p)
{
	(void)p;
}

static void freeOwnedValue(struct owned_value *val)
{
	if (val->dealloc == SQLITE_STATIC) {
		val->dealloc = noopDealloc;
	} else if (val->dealloc == SQLITE_TRANSIENT) {
		val->dealloc = free;
	}
	switch (val->inner.type) {
		case SQLITE_BLOB:
			val->dealloc(val->inner.blob.base);
			break;
		case SQLITE_TEXT:
			val->dealloc((char *)val->inner.text);
			break;
		case DQLITE_ISO8601:
			val->dealloc((char *)val->inner.iso8601);
			break;
		default:;
	}
	memset(val, 0, sizeof *val);
	val->inner.type = SQLITE_NULL;
}

int dqlite_bind_blob(dqlite_stmt *stmt,
		     int index,
		     const void *blob,
		     int blob_len,
		     void (*dealloc)(void *))
{
	void *owned_blob;

	if (blob == NULL) {
		return dqlite_bind_null(stmt, index);
	}
	if (blob_len < 0) {
		return SQLITE_ERROR;
	}

	index -= 1;
	if (index < 0 || (unsigned)index >= stmt->n_params) {
		if (dealloc != SQLITE_STATIC && dealloc != SQLITE_TRANSIENT &&
		    blob != NULL) {
			dealloc((void *)blob);
		}
		return SQLITE_RANGE;
	}
	if (dealloc == SQLITE_TRANSIENT) {
		owned_blob = mallocChecked((size_t)blob_len);
		memcpy(owned_blob, blob, (size_t)blob_len);
	} else {
		owned_blob = (void *)blob;
	}
	freeOwnedValue(&stmt->params[index]);
	stmt->params[index].inner.type = SQLITE_BLOB;
	stmt->params[index].inner.blob.base = owned_blob;
	stmt->params[index].inner.blob.len = (size_t)blob_len;
	stmt->params[index].dealloc = dealloc;
	return SQLITE_OK;
}

int dqlite_bind_double(dqlite_stmt *stmt, int index, double val)
{
	index -= 1;
	if (index < 0 || (unsigned)index >= stmt->n_params) {
		return SQLITE_RANGE;
	}
	freeOwnedValue(&stmt->params[index]);
	stmt->params[index].inner.type = SQLITE_FLOAT;
	stmt->params[index].inner.float_ = val;
	return SQLITE_OK;
}

int dqlite_bind_int64(dqlite_stmt *stmt, int index, int64_t val)
{
	index -= 1;
	if (index < 0 || (unsigned)index >= stmt->n_params) {
		return SQLITE_RANGE;
	}
	freeOwnedValue(&stmt->params[index]);
	stmt->params[index].inner.type = SQLITE_INTEGER;
	stmt->params[index].inner.integer = val;
	return SQLITE_OK;
}

int dqlite_bind_null(dqlite_stmt *stmt, int index)
{
	index -= 1;
	if (index < 0 || (unsigned)index >= stmt->n_params) {
		return SQLITE_RANGE;
	}
	freeOwnedValue(&stmt->params[index]);
	/* just for clarity */
	stmt->params[index].inner.type = SQLITE_NULL;
	return SQLITE_OK;
}

int dqlite_bind_text(dqlite_stmt *stmt,
		     int index,
		     const char *text,
		     int text_len,
		     void (*dealloc)(void *))
{
	char *owned_text;
	int rv;

	if (text == NULL) {
		return dqlite_bind_null(stmt, index);
	}
	if (text_len < 0) {
		text_len = INT_MAX;
	}

	index -= 1;
	if (index < 0 || (unsigned)index >= stmt->n_params) {
		rv = SQLITE_RANGE;
		goto err;
	}
	if (dealloc == SQLITE_TRANSIENT) {
		owned_text = strndupChecked(text, (size_t)text_len);
	} else {
		owned_text = (char *)text;
	}
	freeOwnedValue(&stmt->params[index]);
	stmt->params[index].inner.type = SQLITE_TEXT;
	stmt->params[index].inner.text = owned_text;
	stmt->params[index].dealloc = dealloc;
	return SQLITE_OK;

err:
	if (dealloc != SQLITE_STATIC && dealloc != SQLITE_TRANSIENT &&
	    text != NULL) {
		dealloc((char *)text);
	}
	return rv;
}

int dqlite_clear_bindings(dqlite_stmt *stmt)
{
	unsigned i;

	for (i = 0; i < stmt->n_params; i += 1) {
		freeOwnedValue(&stmt->params[i]);
	}
	return SQLITE_OK;
}

static int drainRows(struct client_proto *proto, struct client_context *context)
{
	/* TODO use the interrupt request */
	struct rows rows;
	bool done = false;
	int rv;

	while (!done) {
		rv = clientRecvRows(proto, &rows, &done, context);
		if (rv != 0) {
			return SQLITE_ERROR;
		}
		clientCloseRows(&rows);
	}
	return SQLITE_OK;
}

int dqlite_reset(dqlite_stmt *stmt)
{
	struct client_context context;
	int rv;

	clientContextMillis(&context, 5000);
	clearConvertedVals(stmt);
	free(stmt->converted);
	stmt->converted = NULL;
	switch (stmt->state) {
		case DQLITE_STEP_START:
			break;
		case DQLITE_STEP_SENT_QUERY:
			rv = drainRows(&stmt->proto, &context);
			if (rv != 0) {
				return rv;
			}
			__attribute__((fallthrough));
		case DQLITE_STEP_GOT_EOF:
			clientCloseRows(&stmt->rows);
			stmt->next_row = NULL;
			stmt->state = DQLITE_STEP_START;
			break;
		default:
			assert(0);
	}
	return SQLITE_OK;
}

static const void *convertIntegerToText(struct dqlite_stmt *stmt, int index)
{
	struct value *val = &stmt->next_row->values[index];

	assert(val->type == SQLITE_INTEGER);
	if (stmt->converted == NULL) {
		stmt->converted = callocChecked(stmt->rows.column_count,
						sizeof *stmt->converted);
	}
	if (stmt->converted[index] == NULL) {
		stmt->converted[index] =
		    sqlite3_mprintf("%lld", (long long)val->integer);
	}
	return stmt->converted[index];
}

static const void *convertFloatToText(struct dqlite_stmt *stmt, int index)
{
	struct value *val = &stmt->next_row->values[index];

	assert(val->type == SQLITE_FLOAT);
	if (stmt->converted == NULL) {
		stmt->converted = callocChecked(stmt->rows.column_count,
						sizeof *stmt->converted);
	}
	if (stmt->converted[index] == NULL) {
		stmt->converted[index] = sqlite3_mprintf("%!.15g", val->float_);
	}
	return stmt->converted[index];
}

static const void *convertBlobToText(struct dqlite_stmt *stmt, int index)
{
	struct value *val = &stmt->next_row->values[index];
	char *text;

	assert(val->type == SQLITE_BLOB);
	if (stmt->converted == NULL) {
		stmt->converted = callocChecked(stmt->rows.column_count,
						sizeof *stmt->converted);
	}
	if (stmt->converted[index] == NULL) {
		stmt->converted[index] = sqlite3_malloc((int)val->blob.len + 1);
		text = stmt->converted[index];
		memcpy(text, val->blob.base, val->blob.len);
		text[val->blob.len] = '\0';
	}
	return stmt->converted[index];
}

const void *dqlite_column_blob(dqlite_stmt *stmt, int index)
{
	struct value *val;

	if (stmt->next_row == NULL) {
		return NULL;
	}
	if (index < 0 || (unsigned)index >= stmt->rows.column_count) {
		return NULL;
	}
	val = &stmt->next_row->values[index];
	switch (val->type) {
		case SQLITE_BLOB:
			return val->blob.base;
		case SQLITE_TEXT:
			return val->text;
		case DQLITE_ISO8601:
			return val->iso8601;
		case SQLITE_NULL:
			return NULL;
		case DQLITE_BOOLEAN:
			return val->boolean ? "1" : "0";
		case SQLITE_INTEGER:
			return convertIntegerToText(stmt, index);
		case SQLITE_FLOAT:
			return convertFloatToText(stmt, index);
		default:
			assert(0);
	}
}

double dqlite_column_double(dqlite_stmt *stmt, int index)
{
	struct value *val;

	if (stmt->next_row == NULL) {
		return 0.0;
	}
	if (index < 0 || (unsigned)index >= stmt->rows.column_count) {
		return 0.0;
	}
	val = &stmt->next_row->values[index];
	switch (val->type) {
		case SQLITE_FLOAT:
			return val->float_;
		case SQLITE_INTEGER:
			return (double)val->integer;
		case DQLITE_BOOLEAN:
			return val->boolean ? 1.0 : 0.0;
		case SQLITE_NULL:
			return 0.0;
		case SQLITE_BLOB:
		case SQLITE_TEXT:
		case DQLITE_ISO8601:
			/* TODO */
			return 0.0;
		default:
			assert(0);
	}
}

int64_t dqlite_column_int64(dqlite_stmt *stmt, int index)
{
	struct value *val;
	double d;

	if (stmt->next_row == NULL) {
		return 0;
	}
	if (index < 0 || (unsigned)index >= stmt->rows.column_count) {
		return 0;
	}
	val = &stmt->next_row->values[index];
	switch (val->type) {
		case SQLITE_INTEGER:
			return val->integer;
		case DQLITE_BOOLEAN:
			return (int64_t)val->boolean;
		case SQLITE_NULL:
			return 0;
		case SQLITE_FLOAT:
			d = val->float_;
			if (d > (double)INT64_MAX) {
				return INT64_MAX;
			} else if (d < (double)INT64_MIN) {
				return INT64_MIN;
			} else {
				return (int64_t)d;
			}
		case SQLITE_BLOB:
		case SQLITE_TEXT:
		case DQLITE_ISO8601:
			/* TODO */
			return 0;
		default:
			assert(0);
	}
}

const unsigned char *dqlite_column_text(dqlite_stmt *stmt, int index)
{
	struct value *val;
	const char *text;

	if (stmt->next_row == NULL) {
		return NULL;
	}
	if (index < 0 || (unsigned)index >= stmt->rows.column_count) {
		return NULL;
	}
	val = &stmt->next_row->values[index];
	switch (val->type) {
		case SQLITE_TEXT:
			return (const unsigned char *)val->text;
		case DQLITE_ISO8601:
			return (const unsigned char *)val->iso8601;
		case DQLITE_BOOLEAN:
			return (const unsigned char *)(val->boolean ? "1"
								    : "0");
		case SQLITE_BLOB:
			text = convertBlobToText(stmt, index);
			return (const unsigned char *)text;
		case SQLITE_NULL:
			return NULL;
		case SQLITE_INTEGER:
			return convertIntegerToText(stmt, index);
		case SQLITE_FLOAT:
			return convertFloatToText(stmt, index);
		default:
			assert(0);
	}
}

int dqlite_column_bytes(dqlite_stmt *stmt, int index)
{
	struct value *val;
	const char *text;

	if (stmt->next_row == NULL) {
		return 0;
	}
	if (index < 0 || (unsigned)index >= stmt->rows.column_count) {
		return 0;
	}
	val = &stmt->next_row->values[index];
	switch (val->type) {
		case SQLITE_BLOB:
			return (int)val->blob.len;
		case SQLITE_TEXT:
			return (int)strlen(val->text);
		case DQLITE_ISO8601:
			return (int)strlen(val->iso8601);
		case SQLITE_NULL:
			return 0;
		case SQLITE_INTEGER:
			text = convertIntegerToText(stmt, index);
			return (int)strlen(text) + 1;
		case SQLITE_FLOAT:
			text = convertFloatToText(stmt, index);
			return (int)strlen(text) + 1;
		case DQLITE_BOOLEAN:
			return 2;
		default:
			assert(0);
	}
}

int dqlite_column_type(dqlite_stmt *stmt, int index)
{
	if (stmt->next_row == NULL) {
		return 0;
	}
	if (index < 0 || (unsigned)index >= stmt->rows.column_count) {
		return 0;
	}
	return stmt->next_row->values[index].type;
}

int dqlite_finalize(dqlite_stmt *stmt)
{
	unsigned i;

	clientClose(&stmt->proto);
	for (i = 0; i < stmt->n_params; i += 1) {
		freeOwnedValue(&stmt->params[i]);
	}
	free(stmt->params);
	if (stmt->state != DQLITE_STEP_START) {
		clientCloseRows(&stmt->rows);
	}
	clearConvertedVals(stmt);
	free(stmt->converted);
	free(stmt);
	return SQLITE_OK;
}

int dqlite_close(dqlite *db)
{
	free(db->name);
	free(db);
	return SQLITE_OK;
}
