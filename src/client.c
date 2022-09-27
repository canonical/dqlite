#include <sqlite3.h>
#include <unistd.h>
#include <stdint.h>

#include "lib/assert.h"

#include "client.h"
#include "message.h"
#include "protocol.h"
#include "request.h"
#include "response.h"
#include "tracing.h"
#include "tuple.h"

int clientInit(struct client *c, int fd)
{
	tracef("init client fd %d", fd);
	int rv;
	c->fd = fd;

	rv = buffer__init(&c->read);
	if (rv != 0) {
		tracef("init client read buffer init failed");
		goto err;
	}
	rv = buffer__init(&c->write);
	if (rv != 0) {
		tracef("init client write buffer init failed");
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
	tracef("client close fd %d", c->fd);
	buffer__close(&c->write);
	buffer__close(&c->read);
}

int clientSendHandshake(struct client *c)
{
	uint64_t protocol;
	ssize_t rv;

	tracef("client send handshake fd %d", c->fd);
	/* TODO: update to version 1 */
	protocol = ByteFlipLe64(DQLITE_PROTOCOL_VERSION_LEGACY);

	rv = write(c->fd, &protocol, sizeof protocol);
	if (rv < 0) {
		tracef("client send handshake failed %zd", rv);
		return DQLITE_ERROR;
	}

	return 0;
}

/* Write out a request. */
#define REQUEST(LOWER, UPPER)                                       \
	{                                                           \
		struct message message = {0};                       \
		size_t n;                                           \
		size_t n1;                                          \
		size_t n2;                                          \
		void *cursor;                                       \
		ssize_t rv;                                         \
		n1 = message__sizeof(&message);                     \
		n2 = request_##LOWER##__sizeof(&request);           \
		n = n1 + n2;                                        \
		buffer__reset(&c->write);                           \
		cursor = buffer__advance(&c->write, n);             \
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
			tracef("request write failed rv %zd", rv);  \
			return DQLITE_ERROR;                        \
		}                                                   \
	}

/* Read a response without decoding it. */
#define READ(LOWER, UPPER)                                          \
	{                                                           \
		struct message _message = {0};                      \
		struct cursor _cursor;                              \
		size_t _n;                                          \
		void *_p;                                           \
		ssize_t _rv;                                        \
		_n = message__sizeof(&_message);                    \
		buffer__reset(&c->read);                            \
		_p = buffer__advance(&c->read, _n);                 \
		assert(_p != NULL);                                 \
		_rv = read(c->fd, _p, _n);                          \
		if (_rv != (int)_n) {                               \
			tracef("read head failed rv %zd", _rv);     \
			return DQLITE_ERROR;                        \
		}                                                   \
		_cursor.p = _p;                                     \
		_cursor.cap = _n;                                   \
		_rv = message__decode(&_cursor, &_message);         \
		assert(_rv == 0);                                   \
		if (_message.type != DQLITE_RESPONSE_##UPPER) {     \
			tracef("read decode failed rv %zd)", _rv);  \
			return DQLITE_ERROR;                        \
		}                                                   \
		buffer__reset(&c->read);                            \
		_n = _message.words * 8;                            \
		_p = buffer__advance(&c->read, _n);                 \
		if (_p == NULL) {                                   \
			tracef("read buf adv failed rv %zd", _rv);  \
			return DQLITE_ERROR;                        \
		}                                                   \
		_rv = read(c->fd, _p, _n);                          \
		if (_rv != (int)_n) {                               \
			tracef("read body failed rv %zd", _rv);     \
			return DQLITE_ERROR;                        \
		}                                                   \
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
			tracef("decode failed rv %d)", rv);          \
			return DQLITE_ERROR;                         \
		}                                                    \
	}

/* Read and decode a response. */
#define RESPONSE(LOWER, UPPER) \
	READ(LOWER, UPPER);    \
	DECODE(LOWER)

int clientSendOpen(struct client *c, const char *name)
{
	tracef("client send open fd %d name %s", c->fd, name);
	struct request_open request;
	request.filename = name;
	request.flags = 0;    /* TODO: this is unused, should we drop it? */
	request.vfs = "test"; /* TODO: this is unused, should we drop it? */
	REQUEST(open, OPEN);
	return 0;
}

int clientRecvDb(struct client *c)
{
	tracef("client recvdb fd %d", c->fd);
	struct response_db response;
	RESPONSE(db, DB);
	c->db_id = response.id;
	return 0;
}

int clientSendPrepare(struct client *c, const char *sql)
{
	tracef("client send prepare fd %d", c->fd);
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
	tracef("client recv stmt fd %d stmt_id %u", c->fd, *stmt_id);
	return 0;
}

int clientSendExec(struct client *c, unsigned stmt_id)
{
	tracef("client send exec fd %d id %u", c->fd, stmt_id);
	struct request_exec request;
	request.db_id = c->db_id;
	request.stmt_id = stmt_id;
	REQUEST(exec, EXEC);
	return 0;
}

int clientSendExecSQL(struct client *c, const char *sql)
{
	tracef("client send exec sql fd %d", c->fd);
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
	tracef("client recv result fd %d last_insert_id %u rows_affected %u", c->fd, *last_insert_id, *rows_affected);
	return 0;
}

int clientSendQuery(struct client *c, unsigned stmt_id)
{
	tracef("client send query fd %d stmt_id %u", c->fd, stmt_id);
	struct request_query request;
	request.db_id = c->db_id;
	request.stmt_id = stmt_id;
	REQUEST(query, QUERY);
	return 0;
}

int clientSendQuerySql(struct client *c, const char *sql)
{
	tracef("client send query sql fd %d sql %s", c->fd, sql);
	struct request_query_sql request;
	request.db_id = c->db_id;
	request.sql = sql;
	REQUEST(query_sql, QUERY_SQL);
	return 0;
}

int clientRecvRows(struct client *c, struct rows *rows)
{
	tracef("client recv rows fd %d", c->fd);
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
		tracef("client recv rows fd %d decode failed %d", c->fd, rv);
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
		eof = ByteFlipLe64(*(uint64_t *)cursor.p);
		if (eof == DQLITE_RESPONSE_ROWS_DONE ||
		    eof == DQLITE_RESPONSE_ROWS_PART) {
			break;
		}
		row = sqlite3_malloc(sizeof *row);
		if (row == NULL) {
			tracef("malloc");
			return DQLITE_NOMEM;
		}
		row->values =
		    sqlite3_malloc((int)(column_count * sizeof *row->values));
		if (row->values == NULL) {
			tracef("malloc");
			sqlite3_free(row);
			return DQLITE_NOMEM;
		}
		row->next = NULL;
		rv = tuple_decoder__init(&decoder, (unsigned)column_count,
					 TUPLE__ROW, &cursor);
		if (rv != 0) {
			tracef("decode init error %d", rv);
			sqlite3_free(row->values);
			sqlite3_free(row);
			return DQLITE_ERROR;
		}
		for (i = 0; i < rows->column_count; i++) {
			rv = tuple_decoder__next(&decoder, &row->values[i]);
			if (rv != 0) {
				tracef("decode error %d", rv);
				sqlite3_free(row->values);
				sqlite3_free(row);
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
	tracef("client send add fd %d id %u address %s", c->fd, id, address);
	struct request_add request;
	request.id = id;
	request.address = address;
	REQUEST(add, ADD);
	return 0;
}

int clientSendAssign(struct client *c, unsigned id, int role)
{
	tracef("client send assign fd %d id %u role %d", c->fd, id, role);
	struct request_assign request;
	(void)role;
	/* TODO: actually send an assign request, not a legacy promote one. */
	request.id = id;
	REQUEST(assign, ASSIGN);
	return 0;
}

int clientSendRemove(struct client *c, unsigned id)
{
	tracef("client send remove fd %d id %u", c->fd, id);
	struct request_remove request;
	request.id = id;
	REQUEST(remove, REMOVE);
	return 0;
}

int clientSendTransfer(struct client *c, unsigned id)
{
	tracef("client send transfer fd %d id %u", c->fd, id);
	struct request_transfer request;
	request.id = id;
	REQUEST(transfer, TRANSFER);
	return 0;
}

int clientRecvEmpty(struct client *c)
{
	tracef("client recv empty fd %d", c->fd);
	struct response_empty response;
	RESPONSE(empty, EMPTY);
	return 0;
}

int clientRecvFailure(struct client *c, uint64_t *code, const char **msg)
{
	tracef("client recv failure fd %d", c->fd);
	struct response_failure response;
	RESPONSE(failure, FAILURE);
	*code = response.code;
	*msg = response.message;
	return 0;
}
