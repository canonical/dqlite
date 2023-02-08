#include <inttypes.h>
#include <poll.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>

#include "../lib/assert.h"

#include "protocol.h"
#include "../message.h"
#include "../protocol.h"
#include "../request.h"
#include "../response.h"
#include "../tracing.h"
#include "../tuple.h"

static void oom(void) {
	abort();
}

static void *mallocChecked(size_t n)
{
	void *p = malloc(n);
	if (p == NULL) {
		oom();
	}
	return p;
}

static void *callocChecked(size_t count, size_t n)
{
	void *p = calloc(count, n);
	if (p == NULL) {
		oom();
	}
	return p;
}

/* Convert a value that potentially borrows data from the client_proto read buffer
 * into one that owns its data. The owned data must be free with freeOwnedValue. */
static void makeValueOwned(struct value *val)
{
	char *p;
	switch (val->type) {
		case SQLITE_TEXT:
			p = strdup(val->text);
			if (p == NULL) {
				oom();
			}
			val->text = p;
			break;
		case DQLITE_ISO8601:
			p = strdup(val->iso8601);
			if (p == NULL) {
				oom();
			}
			val->iso8601 = p;
			break;
		case SQLITE_BLOB:
			p = mallocChecked(val->blob.len);
			memcpy(p, val->blob.base, val->blob.len);
			val->blob.base = p;
			break;
		default:
			;
	}
}

/* Free the owned data of a value, which must have had makeValueOwned called
 * on it previously. */
static void freeOwnedValue(struct value val)
{
	switch (val.type) {
		case SQLITE_TEXT:
			free((char *)val.text);
			break;
		case DQLITE_ISO8601:
			free((char *)val.iso8601);
			break;
		case SQLITE_BLOB:
			free(val.blob.base);
			break;
		default:
			;
	}
}

static int peekUint64(struct cursor cursor, uint64_t *val)
{
	if (cursor.cap < 8) {
		return DQLITE_CLIENT_PROTO_ERROR;
	}
	*val = ByteFlipLe64(*(uint64_t *)cursor.p);
	return 0;
}

/* Read data from fd into buf until one of the following occurs:
 *
 * - The full count n of bytes is read.
 * - A read returns 0 (EOF).
 * - The time budget is exhausted.
 * - An error occurs.
 *
 * On error, -1 is returned. Otherwise the return value is the count
 * of bytes read. This may be less than n if either EOF happened or
 * the time budget was exhausted. */
static ssize_t doRead(int fd, void *buf, size_t n, struct client_context *context)
{
	bool have_budget = context != NULL && context->budget_millis >= 0;
	size_t got = 0;
	struct pollfd pfd;
	struct timespec prev;
	struct timespec cur;
	int diff_millis;
	ssize_t rv;

	if (have_budget) {
		rv = clock_gettime(CLOCK_MONOTONIC, &prev);
		if (rv != 0) {
			return -1;
		}
	}

	pfd.fd = fd;
	pfd.events = POLLIN;
	pfd.revents = 0;

	while (got < n) {
		rv = poll(&pfd, 1, (context == NULL) ? -1 : context->budget_millis);
		if (rv < 0) {
			if (errno == EINTR) {
				continue;
			} else {
				return rv;
			}
		} else if (rv == 0) {
			/* Timeout */
			break;
		}
		assert(rv == 1);
		if (pfd.revents != POLLIN) {
			return -1;
		}

		/* Update the time budget */
		if (have_budget) {
			rv = clock_gettime(CLOCK_MONOTONIC, &cur);
			if (rv != 0) {
				return -1;
			}
			diff_millis = (int)(cur.tv_sec - prev.tv_sec) * 1000 +
				(int)((cur.tv_nsec - prev.tv_nsec) / 1000000);
			assert(diff_millis >= 0);
			context->budget_millis -= diff_millis;
			if (context->budget_millis <= 0) {
				/* Timeout */
				break;
			}
			prev = cur;
		}

		rv = read(fd, (char *)buf + got, n - got);
		if (rv < 0) {
			if (errno == EINTR) {
				continue;
			} else {
				return rv;
			}
		} else if (rv == 0) {
			/* EOF */
			break;
		}
		got += (size_t)rv;
	}
	return (ssize_t)got;
}

/* Write data into fd from buf until one of the following occurs:
 *
 * - The full count n of bytes is written.
 * - A write returns 0 (EOF).
 * - The time budget is exhausted.
 * - An error occurs.
 *
 * On error, -1 is returned. Otherwise the return value is the count
 * of bytes written. This may be less than n if either EOF happened or
 * the time budget was exhausted. */
static ssize_t doWrite(int fd, void *buf, size_t n, struct client_context *context)
{
	bool have_budget = context != NULL && context->budget_millis >= 0;
	size_t wrote = 0;
	struct pollfd pfd;
	struct timespec prev;
	struct timespec cur;
	int diff_millis;
	ssize_t rv;

	if (have_budget) {
		rv = clock_gettime(CLOCK_MONOTONIC, &prev);
		if (rv != 0) {
			return -1;
		}
	}

	pfd.fd = fd;
	pfd.events = POLLOUT;
	pfd.revents = 0;

	while (wrote < n) {
		rv = poll(&pfd, 1, (context == NULL) ? -1 : context->budget_millis);
		if (rv < 0) {
			if (errno == EINTR) {
				continue;
			} else {
				return rv;
			}
		} else if (rv == 0) {
			/* Timeout */
			break;
		}
		assert(rv == 1);
		if (pfd.revents != POLLOUT) {
			return -1;
		}

		/* Update the time budget */
		if (have_budget) {
			rv = clock_gettime(CLOCK_MONOTONIC, &cur);
			if (rv != 0) {
				return -1;
			}
			diff_millis = (int)(cur.tv_sec - prev.tv_sec) * 1000 +
				(int)((cur.tv_nsec - prev.tv_nsec) / 1000000);
			assert(diff_millis >= 0);
			context->budget_millis -= diff_millis;
			if (context->budget_millis <= 0) {
				/* Timeout */
				break;
			}
			prev = cur;
		}

		rv = write(fd, (char *)buf + wrote, n - wrote);
		if (rv < 0) {
			if (errno == EINTR) {
				continue;
			} else {
				return rv;
			}
		} else if (rv == 0) {
			/* EOF */
			break;
		}
		wrote += (size_t)rv;
	}
	return (ssize_t)wrote;
}

static int handleFailure(struct client_proto *c)
{
	struct response_failure failure;
	struct cursor cursor;
	int rv;

	cursor.p = buffer__cursor(&c->read, 0);
	cursor.cap = buffer__offset(&c->read);
	rv = response_failure__decode(&cursor, &failure);
	if (rv != 0) {
		tracef("decode as failure failed rv:%d", rv);
		return DQLITE_CLIENT_PROTO_ERROR;
	}
	c->errcode = failure.code;
	if (c->errmsg != NULL) {
		free(c->errmsg);
	}
	c->errmsg = strdup(failure.message);
	if (c->errmsg == NULL) {
		oom();
	}
	return DQLITE_CLIENT_PROTO_RECEIVED_FAILURE;
}

int clientInit(struct client_proto *c, int fd)
{
	tracef("init client");
	int rv;
	c->fd = fd;
	c->db_name = NULL;
	c->db_is_init = false;

	rv = buffer__init(&c->read);
	if (rv != 0) {
		oom();
	}
	rv = buffer__init(&c->write);
	if (rv != 0) {
		oom();
	}

	c->errcode = 0;
	c->errmsg = NULL;

	return 0;
}

void clientClose(struct client_proto *c)
{
	tracef("client close");
	close(c->fd);
	buffer__close(&c->write);
	buffer__close(&c->read);
	free(c->db_name);
	free(c->errmsg);
}

int clientSendHandshake(struct client_proto *c, struct client_context *context)
{
	uint64_t protocol;
	ssize_t rv;

	tracef("client send handshake");
	protocol = ByteFlipLe64(DQLITE_PROTOCOL_VERSION);

	rv = doWrite(c->fd, &protocol, sizeof protocol, context);
	if (rv < 0) {
		tracef("client send handshake failed %zd", rv);
		return DQLITE_CLIENT_PROTO_ERROR;
	} else if ((size_t)rv < sizeof protocol) {
		return DQLITE_CLIENT_PROTO_SHORT;
	}

	return 0;
}

static int writeMessage(struct client_proto *c, uint8_t type, uint8_t schema, struct client_context *context)
{
	struct message message = {0};
	size_t n;
	size_t words;
	void *cursor;
	ssize_t rv;
	n = buffer__offset(&c->write);
	words = (n - message__sizeof(&message)) / 8;
	message.words = (uint32_t)words;
	message.type = type;
	message.schema = schema;
	cursor = buffer__cursor(&c->write, 0);
	message__encode(&message, &cursor);
	rv = doWrite(c->fd, buffer__cursor(&c->write, 0), n, context);
	if (rv < 0) {
		tracef("request write failed rv:%zd", rv);
		return DQLITE_CLIENT_PROTO_ERROR;
	} else if ((size_t)rv < n) {
		return DQLITE_CLIENT_PROTO_SHORT;
	}
	return 0;
}

#define BUFFER_REQUEST(LOWER, UPPER)                             \
	{                                                        \
		struct message _message = {0};                   \
		size_t _n1;                                      \
		size_t _n2;                                      \
		void *_cursor;                                   \
		_n1 = message__sizeof(&_message);                \
		_n2 = request_##LOWER##__sizeof(&request);       \
		buffer__reset(&c->write);                        \
		_cursor = buffer__advance(&c->write, _n1 + _n2); \
		if (_cursor == NULL) {                           \
			oom();                                   \
		}                                                \
		assert(_n2 % 8 == 0);                            \
		message__encode(&_message, &_cursor);            \
		request_##LOWER##__encode(&request, &_cursor);   \
	}

/* Write out a request. */
#define REQUEST(LOWER, UPPER, SCHEMA)                                                 \
	{                                                                             \
		int _rv;                                                              \
		BUFFER_REQUEST(LOWER, UPPER);                                         \
		_rv = writeMessage(c, DQLITE_REQUEST_##UPPER, SCHEMA, context); \
		if (_rv != 0) {                                                       \
			return _rv;                                                   \
		}                                                                     \
	}

static int readMessage(struct client_proto *c, uint8_t *type, struct client_context *context)
{
	struct message message = {0};
	struct cursor cursor;
	void *p;
	size_t n;
	ssize_t rv;

	buffer__reset(&c->read);
	n = message__sizeof(&message);
	p = buffer__advance(&c->read, n);
	if (p == NULL) {
		oom();
	}
	rv = doRead(c->fd, p, n, context);
	if (rv < 0) {
		return DQLITE_CLIENT_PROTO_ERROR;
	} else if (rv < (ssize_t)n) {
		return DQLITE_CLIENT_PROTO_SHORT;
	}

	cursor.p = p;
	cursor.cap = n;
	rv = message__decode(&cursor, &message);
	if (rv != 0) {
		tracef("message decode failed rv:%zd", rv);
		return DQLITE_CLIENT_PROTO_ERROR;
	}

	buffer__reset(&c->read);
	n = message.words * 8;
	p = buffer__advance(&c->read, n);
	if (p == NULL) {
		oom();
	}
	rv = doRead(c->fd, p, n, context);
	if (rv < 0) {
		return DQLITE_ERROR;
	} else if (rv < (ssize_t)n) {
		return DQLITE_CLIENT_PROTO_SHORT;
	}

	*type = message.type;
	return 0;
}

/* Read and decode a response. */
#define RESPONSE(LOWER, UPPER)                                        \
	{                                                             \
		uint8_t _type;                                        \
		int _rv;                                              \
		_rv = readMessage(c, &_type, context);                \
		if (_rv != 0) {                                       \
			return _rv;                                   \
		}                                                     \
		if (_type == DQLITE_RESPONSE_FAILURE) {               \
			handleFailure(c);                             \
		} else if (_type != DQLITE_RESPONSE_##UPPER) {        \
			return DQLITE_CLIENT_PROTO_ERROR;             \
		}                                                     \
		cursor.p = buffer__cursor(&c->read, 0);               \
		cursor.cap = buffer__offset(&c->read);                \
		_rv = response_##LOWER##__decode(&cursor, &response); \
		if (_rv != 0) {                                       \
			return DQLITE_CLIENT_PROTO_ERROR;             \
		}                                                     \
	}

int clientSendLeader(struct client_proto *c, struct client_context *context)
{
	tracef("client send leader");
	struct request_leader request = {0};
	REQUEST(leader, LEADER, 0);
	return 0;
}

int clientSendClient(struct client_proto *c, uint64_t id, struct client_context *context)
{
	tracef("client send client");
	struct request_client request;
	request.id = id;
	REQUEST(client, CLIENT, 0);
	return 0;
}

int clientSendOpen(struct client_proto *c, const char *name, struct client_context *context)
{
	tracef("client send open name %s", name);
	struct request_open request;
	c->db_name = strdup(name);
	if (c->db_name == NULL) {
		oom();
	}
	request.filename = name;
	request.flags = 0; /* unused */
	request.vfs = "test"; /* unused */
	REQUEST(open, OPEN, 0);
	return 0;
}

int clientRecvDb(struct client_proto *c, struct client_context *context)
{
	tracef("client recvdb");
	struct cursor cursor;
	struct response_db response;
	RESPONSE(db, DB);
	c->db_id = response.id;
	c->db_is_init = true;
	return 0;
}

int clientSendPrepare(struct client_proto *c, const char *sql, struct client_context *context)
{
	tracef("client send prepare");
	struct request_prepare request;
	request.db_id = c->db_id;
	request.sql = sql;
	REQUEST(prepare, PREPARE, DQLITE_PREPARE_STMT_SCHEMA_V1);
	return 0;
}

int clientRecvStmt(struct client_proto *c,
			uint32_t *stmt_id,
			uint64_t *offset,
			struct client_context *context)
{
	struct cursor cursor;
	struct response_stmt_with_offset response;
	RESPONSE(stmt_with_offset, STMT_WITH_OFFSET);
	tracef("client recv stmt stmt_id:%" PRIu32 " offset:%" PRIu64, response.id, response.offset);
	if (stmt_id != NULL) {
		*stmt_id = response.id;
	}
	if (offset != NULL) {
		*offset = response.offset;
	}
	return 0;
}

static int bufferParams(struct client_proto *c,
			struct value *params,
			size_t n_params)
{
	struct tuple_encoder tup;
	size_t i;
	int rv;

	if (n_params == 0) {
		return 0;
	}
	rv = tuple_encoder__init(&tup, n_params, TUPLE__PARAMS32, &c->write);
	if (rv != 0) {
		return DQLITE_CLIENT_PROTO_ERROR;
	}
	for (i = 0; i < n_params; ++i) {
		rv = tuple_encoder__next(&tup, &params[i]);
		if (rv != 0) {
			return DQLITE_CLIENT_PROTO_ERROR;
		}
	}
	return 0;
}

int clientSendExec(struct client_proto *c,
			uint32_t stmt_id,
			struct value *params,
			size_t n_params,
			struct client_context *context)
{
	tracef("client send exec id %" PRIu32, stmt_id);
	struct request_exec request;
	int rv;

	request.db_id = c->db_id;
	request.stmt_id = stmt_id;
	BUFFER_REQUEST(exec, EXEC);

	rv = bufferParams(c, params, n_params);
	if (rv != 0) {
		return rv;
	}
	rv = writeMessage(c, DQLITE_REQUEST_EXEC, 1, context);
	return rv;
}

int clientSendExecSQL(struct client_proto *c,
			const char *sql,
			struct value *params,
			size_t n_params,
			struct client_context *context)
{
	tracef("client send exec sql");
	struct request_exec_sql request;
	int rv;

	request.db_id = c->db_id;
	request.sql = sql;
	BUFFER_REQUEST(exec_sql, EXEC_SQL);

	rv = bufferParams(c, params, n_params);
	if (rv != 0) {
		return rv;
	}
	rv = writeMessage(c, DQLITE_REQUEST_EXEC_SQL, 1, context);
	return rv;
}

int clientRecvResult(struct client_proto *c,
			uint64_t *last_insert_id,
			uint64_t *rows_affected,
			struct client_context *context)
{
	struct cursor cursor;
	struct response_result response;
	RESPONSE(result, RESULT);
	*last_insert_id = response.last_insert_id;
	*rows_affected = response.rows_affected;
	tracef("client recv result last_insert_id %" PRIu64 "rows_affected %" PRIu64,
			*last_insert_id, *rows_affected);
	return 0;
}

int clientSendQuery(struct client_proto *c,
			uint32_t stmt_id,
			struct value *params,
			size_t n_params,
			struct client_context *context)
{
	tracef("client send query stmt_id %" PRIu32, stmt_id);
	struct request_query request;
	int rv;

	request.db_id = c->db_id;
	request.stmt_id = stmt_id;
	BUFFER_REQUEST(query, QUERY);

	rv = bufferParams(c, params, n_params);
	if (rv != 0) {
		return rv;
	}
	rv = writeMessage(c, DQLITE_REQUEST_QUERY, 1, context);
	return rv;
}

int clientSendQuerySQL(struct client_proto *c,
			const char *sql,
			struct value *params,
			size_t n_params,
			struct client_context *context)
{
	tracef("client send query sql sql %s", sql);
	struct request_query_sql request;
	int rv;

	request.db_id = c->db_id;
	request.sql = sql;
	BUFFER_REQUEST(query_sql, QUERY_SQL);

	rv = bufferParams(c, params, n_params);
	if (rv != 0) {
		return rv;
	}
	rv = writeMessage(c, DQLITE_REQUEST_QUERY_SQL, 1, context);
	return rv;
}

int clientRecvRows(struct client_proto *c, struct rows *rows, struct client_context *context)
{
	tracef("client recv rows");
	struct cursor cursor;
	uint8_t type;
	uint64_t column_count;
	unsigned i;
	unsigned j;
	const char *raw;
	struct row *row;
	struct row *last;
	uint64_t eof;
	struct tuple_decoder tup;
	int rv;

	rv = readMessage(c, &type, context);
	if (rv != 0) {
		return rv;
	}
	if (type == DQLITE_RESPONSE_FAILURE) {
		rv = handleFailure(c);
		return rv;
	} else if (type != DQLITE_RESPONSE_ROWS) {
		return DQLITE_CLIENT_PROTO_ERROR;
	}

	cursor.p = buffer__cursor(&c->read, 0);
	cursor.cap = buffer__offset(&c->read);
	rv = uint64__decode(&cursor, &column_count);
	if (rv != 0) {
		return DQLITE_CLIENT_PROTO_ERROR;
	}
	rows->column_count = (unsigned)column_count;
	assert((uint64_t)rows->column_count == column_count);

	rows->column_names = callocChecked(rows->column_count, sizeof *rows->column_names);
	for (i = 0; i < rows->column_count; ++i) {
		rv = text__decode(&cursor, &raw);
		if (rv != 0) {
			rv = DQLITE_CLIENT_PROTO_ERROR;
			goto err_after_alloc_column_names;
		}
		rows->column_names[i] = strdup(raw);
		if (rows->column_names[i] == NULL) {
			oom();
		}
	}

	rows->next = NULL;
	last = NULL;
	while (1) {
		rv = peekUint64(cursor, &eof);
		if (rv != 0) {
			goto err_after_alloc_column_names;
		}
		if (eof == DQLITE_RESPONSE_ROWS_DONE ||
		    eof == DQLITE_RESPONSE_ROWS_PART) {
			break;
		}

		row = mallocChecked(sizeof *row);
		row->values = callocChecked(rows->column_count, sizeof *row->values);
		row->next = NULL;

		/* Make sure that `goto err_after_alloc_row_values` will do the
		 * right thing even before we enter the for loop. */
		i = 0;
		rv = tuple_decoder__init(&tup, rows->column_count, TUPLE__ROW, &cursor);
		if (rv != 0) {
			rv = DQLITE_CLIENT_PROTO_ERROR;
			goto err_after_alloc_row_values;
		}
		for (; i < rows->column_count; ++i) {
			rv = tuple_decoder__next(&tup, &row->values[i]);
			if (rv != 0) {
				rv = DQLITE_CLIENT_PROTO_ERROR;
				goto err_after_alloc_row_values;
			}
			makeValueOwned(&row->values[i]);
		}

		if (last == NULL) {
			rows->next = row;
		} else {
			last->next = row;
		}
		last = row;
	}

	return 0;

err_after_alloc_row_values:
	for (j = 0; j < i; ++j) {
		freeOwnedValue(row->values[j]);
	}
	free(row->values);
	free(row);

err_after_alloc_column_names:
	clientCloseRows(rows);
	return rv;
}

void clientCloseRows(struct rows *rows)
{
	uint64_t i;
	struct row *row = rows->next;
	struct row *next;

	/* Note that we take care to still do the right thing if this was
	 * called before clientRecvRows completed. */
	for (row = rows->next; row != NULL; row = next) {
		next = row->next;
		row->next = NULL;
		for (i = 0; i < rows->column_count; ++i) {
			freeOwnedValue(row->values[i]);
		}
		free(row->values);
		row->values = NULL;
		free(row);
	}
	rows->next = NULL;
	if (rows->column_names != NULL) {
		for (i = 0; i < rows->column_count; ++i) {
			free(rows->column_names[i]);
			rows->column_names[i] = NULL;
		}
	}
	free(rows->column_names);
}

int clientSendInterrupt(struct client_proto *c, struct client_context *context)
{
	tracef("client send interrupt");
	struct request_interrupt request;
	request.db_id = c->db_id;
	REQUEST(interrupt, INTERRUPT, 0);
	return 0;
}

int clientSendFinalize(struct client_proto *c, uint32_t stmt_id, struct client_context *context)
{
	tracef("client send finalize %u", stmt_id);
	struct request_finalize request;
	request.db_id = c->db_id;
	request.stmt_id = stmt_id;
	REQUEST(finalize, FINALIZE, 0);
	return 0;
}

int clientSendAdd(struct client_proto *c, uint64_t id, const char *address, struct client_context *context)
{
	tracef("client send add id %" PRIu64 " address %s", id, address);
	struct request_add request;
	request.id = id;
	request.address = address;
	REQUEST(add, ADD, 0);
	return 0;
}

int clientSendAssign(struct client_proto *c, uint64_t id, int role, struct client_context *context)
{
	tracef("client send assign id %" PRIu64 " role %d", id, role);
	assert(role == DQLITE_VOTER || role == DQLITE_STANDBY || role == DQLITE_SPARE);
	struct request_assign request;
	request.id = id;
	request.role = (uint64_t)role;
	REQUEST(assign, ASSIGN, 0);
	return 0;
}

int clientSendRemove(struct client_proto *c, uint64_t id, struct client_context *context)
{
	tracef("client send remove id %" PRIu64, id);
	struct request_remove request;
	request.id = id;
	REQUEST(remove, REMOVE, 0);
	return 0;
}

int clientSendDump(struct client_proto *c, struct client_context *context)
{
	tracef("client send dump");
	struct request_dump request;
	assert(c->db_is_init);
	assert(c->db_name != NULL);
	request.filename = c->db_name;
	REQUEST(dump, DUMP, 0);
	return 0;
}

int clientSendCluster(struct client_proto *c, struct client_context *context)
{
	tracef("client send cluster");
	struct request_cluster request;
	request.format = DQLITE_REQUEST_CLUSTER_FORMAT_V1;
	REQUEST(cluster, CLUSTER, 0);
	return 0;
}

int clientSendTransfer(struct client_proto *c, uint64_t id, struct client_context *context)
{
	tracef("client send transfer id %" PRIu64, id);
	struct request_transfer request;
	request.id = id;
	REQUEST(transfer, TRANSFER, 0);
	return 0;
}

int clientSendDescribe(struct client_proto *c, struct client_context *context)
{
	tracef("client send describe");
	struct request_describe request;
	request.format = DQLITE_REQUEST_DESCRIBE_FORMAT_V0;
	REQUEST(describe, DESCRIBE, 0);
	return 0;
}

int clientSendWeight(struct client_proto *c, uint64_t weight, struct client_context *context)
{
	tracef("client send weight %" PRIu64, weight);
	struct request_weight request;
	request.weight = weight;
	REQUEST(weight, WEIGHT, 0);
	return 0;
}

int clientRecvServer(struct client_proto *c,
			uint64_t *id,
			char **address,
			struct client_context *context)
{
	tracef("client recv server");
	struct cursor cursor;
	struct response_server response;
	*id = 0;
	*address = NULL;
	RESPONSE(server, SERVER);
	*address = strdup(response.address);
	if (*address == NULL) {
		oom();
	}
	*id = response.id;
	return 0;
}

int clientRecvWelcome(struct client_proto *c, struct client_context *context)
{
	tracef("client recv welcome");
	struct cursor cursor;
	struct response_welcome response;
	RESPONSE(welcome, WELCOME);
	return 0;
}

int clientRecvEmpty(struct client_proto *c, struct client_context *context)
{
	tracef("client recv empty");
	struct cursor cursor;
	struct response_empty response;
	RESPONSE(empty, EMPTY);
	return 0;
}

int clientRecvFailure(struct client_proto *c,
			uint64_t *code,
			const char **msg,
			struct client_context *context)
{
	tracef("client recv failure");
	struct cursor cursor;
	struct response_failure response;
	RESPONSE(failure, FAILURE);
	*code = response.code;
	*msg = response.message;
	return 0;
}

int clientRecvServers(struct client_proto *c,
			struct client_node_info **servers,
			size_t *n_servers,
			struct client_context *context)
{
	tracef("client recv servers");
	struct cursor cursor;
	size_t n;
	uint64_t i = 0;
	uint64_t j;
	uint64_t raw_role;
	const char *raw_addr;
	struct response_servers response;
	int rv;

	*servers = NULL;
	*n_servers = 0;

	RESPONSE(servers, SERVERS);

	n = (size_t)response.n;
	assert((uint64_t)n == response.n);
	struct client_node_info *srvs = callocChecked(n, sizeof *srvs);
	for (; i < response.n; ++i) {
		rv = uint64__decode(&cursor, &srvs[i].id);
		if (rv != 0) {
			goto err_after_alloc_srvs;
		}
		rv = text__decode(&cursor, &raw_addr);
		if (rv != 0) {
			goto err_after_alloc_srvs;
		}
		srvs[i].addr = strdup(raw_addr);
		if (srvs[i].addr == NULL) {
			oom();
		}
		rv = uint64__decode(&cursor, &raw_role);
		if (rv != 0) {
			free(srvs[i].addr);
			goto err_after_alloc_srvs;
		}
		srvs[i].role = (int)raw_role;
	}

	*n_servers = n;
	*servers = srvs;
	return 0;

err_after_alloc_srvs:
	for (j = 0; j < i; ++j) {
		free(srvs[i].addr);
	}
	free(srvs);
	return rv;
}

int clientRecvFiles(struct client_proto *c,
			struct client_file **files,
			size_t *n_files,
			struct client_context *context)
{
	tracef("client recv files");
	struct cursor cursor;
	struct response_files response;
	struct client_file *fs;
	size_t n;
	size_t z;
	size_t i = 0;
	size_t j;
	const char *raw_name;
	int rv;
	*files = NULL;
	*n_files = 0;
	RESPONSE(files, FILES);

	n = (size_t)response.n;
	assert((uint64_t)n == response.n);
	fs = callocChecked(n, sizeof *fs);
	for (; i < response.n; ++i) {
		rv = text__decode(&cursor, &raw_name);
		if (rv != 0) {
			goto err_after_alloc_fs;
		}
		fs[i].name = strdup(raw_name);
		if (fs[i].name == NULL) {
			oom();
		}
		rv = uint64__decode(&cursor, &fs[i].size);
		if (rv != 0) {
			free(fs[i].name);
			goto err_after_alloc_fs;
		}
		if (cursor.cap != fs[i].size) {
			free(fs[i].name);
			rv = DQLITE_PARSE;
			goto err_after_alloc_fs;
		}
		z = (size_t)fs[i].size;
		assert((uint64_t)z == fs[i].size);
		fs[i].blob = mallocChecked(z);
		memcpy(fs[i].blob, cursor.p, z);
	}

	*files = fs;
	*n_files = n;
	return 0;

err_after_alloc_fs:
	for (j = 0; j < i; ++j) {
		free(fs[i].name);
		free(fs[i].blob);
	}
	free(fs);
	return rv;
}

int clientRecvMetadata(struct client_proto *c,
			uint64_t *failure_domain,
			uint64_t *weight,
			struct client_context *context)
{
	tracef("client recv metadata");
	struct cursor cursor;
	struct response_metadata response;
	RESPONSE(metadata, METADATA);
	*failure_domain = response.failure_domain;
	*weight = response.weight;
	return 0;
}
