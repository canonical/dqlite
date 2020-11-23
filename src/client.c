#include <sqlite3.h>
#include <unistd.h>
#include <stdint.h>

#include "lib/assert.h"

#include "client.h"
#include "message.h"
#include "protocol.h"
#include "request.h"
#include "response.h"
#include "tuple.h"

int clientInit(struct client *c, int fd)
{
	int rv;
	c->fd = fd;

	rv = buffer__init(&c->read);
	if (rv != 0) {
		goto err;
	}
	rv = buffer__init(&c->write);
	if (rv != 0) {
		goto err_after_read_buffer_init;
	}

	return 0;

err_after_read_buffer_init:
	buffer__close(&c->read);
err:
	return rv;
}

void clientClose(struct client *c)
{
	buffer__close(&c->write);
	buffer__close(&c->read);
}

int clientSendHandshake(struct client *c)
{
	uint64_t protocol;
	ssize_t rv;

	/* TODO: update to version 1 */
	protocol = byte__flip64(DQLITE_PROTOCOL_VERSION_LEGACY);

	rv = write(c->fd, &protocol, sizeof protocol);
	if (rv < 0) {
		return DQLITE_ERROR;
	}

	return 0;
}

/* Write out a request. */
#define REQUEST(LOWER, UPPER)                                       \
	{                                                           \
		struct message message;                             \
		size_t n;                                           \
		size_t n1;                                          \
		size_t n2;                                          \
		void *cursor;                                       \
		ssize_t rv;                                         \
		n1 = message__sizeof(&message);                     \
		n2 = request_##LOWER##__sizeof(&request);           \
		n = n1 + n2;                                        \
		buffer__reset(&c->write);                           \
		cursor = bufferAdvance(&c->write, n);               \
		if (cursor == NULL) {                               \
			return DQLITE_NOMEM;                        \
		}                                                   \
		assert(n2 % 8 == 0);                                \
		message.type = DQLITE_REQUEST_##UPPER;              \
		message.words = (uint32_t)(n2 / 8);                 \
		message__encode(&message, &cursor);                 \
		request_##LOWER##__encode(&request, &cursor);       \
		rv = write(c->fd, buffer__cursor(&c->write, 0), n); \
		if (rv != (int)n) {                                 \
			return DQLITE_ERROR;                        \
		}                                                   \
	}

/* Read a response without decoding it. */
#define READ(LOWER, UPPER)                                      \
	{                                                       \
		struct message _message;                        \
		struct cursor _cursor;                          \
		size_t _n;                                      \
		void *_p;                                       \
		ssize_t _rv;                                    \
		_n = message__sizeof(&_message);                \
		buffer__reset(&c->read);                        \
		_p = bufferAdvance(&c->read, _n);               \
		assert(_p != NULL);                             \
		_rv = read(c->fd, _p, _n);                      \
		if (_rv != (int)_n) {                           \
			return DQLITE_ERROR;                    \
		}                                               \
		_cursor.p = _p;                                 \
		_cursor.cap = _n;                               \
		_rv = message__decode(&_cursor, &_message);     \
		assert(_rv == 0);                               \
		if (_message.type != DQLITE_RESPONSE_##UPPER) { \
			return DQLITE_ERROR;                    \
		}                                               \
		buffer__reset(&c->read);                        \
		_n = _message.words * 8;                        \
		_p = bufferAdvance(&c->read, _n);               \
		if (_p == NULL) {                               \
			return DQLITE_ERROR;                    \
		}                                               \
		_rv = read(c->fd, _p, _n);                      \
		if (_rv != (int)_n) {                           \
			return DQLITE_ERROR;                    \
		}                                               \
	}

/* Decode a response. */
#define DECODE(LOWER)                                                \
	{                                                            \
		int rv;                                              \
		struct cursor cursor;                                \
		cursor.p = buffer__cursor(&c->read, 0);              \
		cursor.cap = buffer__offset(&c->read);               \
		rv = response_##LOWER##__decode(&cursor, &response); \
		if (rv != 0) {                                       \
			return DQLITE_ERROR;                         \
		}                                                    \
	}

/* Read and decode a response. */
#define RESPONSE(LOWER, UPPER) \
	READ(LOWER, UPPER);    \
	DECODE(LOWER)

int clientSendOpen(struct client *c, const char *name)
{
	struct request_open request;
	request.filename = name;
	request.flags = 0;    /* TODO: this is unused, should we drop it? */
	request.vfs = "test"; /* TODO: this is unused, should we drop it? */
	REQUEST(open, OPEN);
	return 0;
}

int clientRecvDb(struct client *c)
{
	struct response_db response;
	RESPONSE(db, DB);
	c->db_id = response.id;
	return 0;
}

int clientSendPrepare(struct client *c, const char *sql)
{
	struct request_prepare request;
	request.db_id = c->db_id;
	request.sql = sql;
	REQUEST(prepare, PREPARE);
	return 0;
}

int clientRecvStmt(struct client *c, unsigned *stmt_id)
{
	struct response_stmt response;
	RESPONSE(stmt, STMT);
	*stmt_id = response.id;
	return 0;
}

int clientSendExec(struct client *c, unsigned stmt_id)
{
	struct request_exec request;
	request.db_id = c->db_id;
	request.stmt_id = stmt_id;
	REQUEST(exec, EXEC);
	return 0;
}

int clientSendExecSQL(struct client *c, const char *sql)
{
	struct request_exec_sql request;
	request.db_id = c->db_id;
	request.sql = sql;
	REQUEST(exec_sql, EXEC_SQL);
	return 0;
}

int clientRecvResult(struct client *c,
		     unsigned *last_insert_id,
		     unsigned *rows_affected)
{
	struct response_result response;
	RESPONSE(result, RESULT);
	*last_insert_id = (unsigned)response.last_insert_id;
	*rows_affected = (unsigned)response.rows_affected;
	return 0;
}

int clientSendQuery(struct client *c, unsigned stmt_id)
{
	struct request_query request;
	request.db_id = c->db_id;
	request.stmt_id = stmt_id;
	REQUEST(query, QUERY);
	return 0;
}

int clientRecvRows(struct client *c, struct rows *rows)
{
	struct cursor cursor;
	struct tuple_decoder decoder;
	uint64_t column_count;
	struct row *last;
	unsigned i;
	int rv;
	READ(rows, ROWS);
	cursor.p = buffer__cursor(&c->read, 0);
	cursor.cap = buffer__offset(&c->read);
	rv = uint64__decode(&cursor, &column_count);
	if (rv != 0) {
		return DQLITE_ERROR;
	}
	rows->column_count = (unsigned)column_count;
	for (i = 0; i < rows->column_count; i++) {
		rows->column_names = sqlite3_malloc(
		    (int)(column_count * sizeof *rows->column_names));
		if (rows->column_names == NULL) {
			return DQLITE_ERROR;
		}
		rv = text__decode(&cursor, &rows->column_names[i]);
		if (rv != 0) {
			return DQLITE_ERROR;
		}
	}
	last = NULL;
	while (1) {
		uint64_t eof;
		struct row *row;
		if (cursor.cap < 8) {
			/* No EOF marker fond */
			return DQLITE_ERROR;
		}
		eof = byte__flip64(*(uint64_t *)cursor.p);
		if (eof == DQLITE_RESPONSE_ROWS_DONE ||
		    eof == DQLITE_RESPONSE_ROWS_PART) {
			break;
		}
		row = sqlite3_malloc(sizeof *row);
		if (row == NULL) {
			return DQLITE_NOMEM;
		}
		row->values =
		    sqlite3_malloc((int)(column_count * sizeof *row->values));
		if (row->values == NULL) {
			return DQLITE_NOMEM;
		}
		row->next = NULL;
		rv = tuple_decoder__init(&decoder, (unsigned)column_count,
					 &cursor);
		if (rv != 0) {
			return DQLITE_ERROR;
		}
		for (i = 0; i < rows->column_count; i++) {
			rv = tuple_decoder__next(&decoder, &row->values[i]);
			if (rv != 0) {
				return DQLITE_ERROR;
			}
		}
		if (last == NULL) {
			rows->next = row;
		} else {
			last->next = row;
		}
		last = row;
	}
	return 0;
}

void clientCloseRows(struct rows *rows)
{
	struct row *row = rows->next;
	while (row != NULL) {
		struct row *next;
		next = row->next;
		sqlite3_free(row->values);
		sqlite3_free(row);
		row = next;
	}
	sqlite3_free(rows->column_names);
}

int clientSendAdd(struct client *c, unsigned id, const char *address)
{
	struct request_add request;
	request.id = id;
	request.address = address;
	REQUEST(add, ADD);
	return 0;
}

int clientSendAssign(struct client *c, unsigned id, int role)
{
	struct request_assign request;
	(void)role;
	/* TODO: actually send an assign request, not a legacy promote one. */
	request.id = id;
	REQUEST(assign, ASSIGN);
	return 0;
}

int clientSendRemove(struct client *c, unsigned id)
{
	struct request_remove request;
	request.id = id;
	REQUEST(remove, REMOVE);
	return 0;
}

int clientRecvEmpty(struct client *c)
{
	struct response_empty response;
	RESPONSE(empty, EMPTY);
	return 0;
}
