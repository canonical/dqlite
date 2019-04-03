#include <unistd.h>

#include "lib/assert.h"

#include "client.h"
#include "message.h"
#include "request.h"
#include "response.h"

int client__init(struct client *c, int fd)
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

void client__close(struct client *c)
{
	close(c->fd);
	buffer__close(&c->write);
	buffer__close(&c->read);
}

int client__send_handshake(struct client *c)
{
	uint64_t protocol;
	int rv;

	protocol = byte__flip64(DQLITE_PROTOCOL_VERSION);

	rv = write(c->fd, &protocol, sizeof(protocol));
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
		int rv;                                             \
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
		message.words = n2 / 8;                             \
		message__encode(&message, &cursor);                 \
		request_##LOWER##__encode(&request, &cursor);       \
		rv = write(c->fd, buffer__cursor(&c->write, 0), n); \
		if (rv != (int)n) {                                 \
			return DQLITE_ERROR;                        \
		}                                                   \
	}

/* Read a response. */
#define RESPONSE(LOWER, UPPER)                                       \
	{                                                            \
		struct message message;                              \
		struct cursor cursor;                                \
		size_t n;                                            \
		void *p;                                             \
		int rv;                                              \
		n = message__sizeof(&message);                       \
		buffer__reset(&c->read);                             \
		p = buffer__advance(&c->read, n);                    \
		assert(p != NULL);                                   \
		rv = read(c->fd, p, n);                              \
		if (rv != (int)n) {                                  \
			return DQLITE_ERROR;                         \
		}                                                    \
		cursor.p = p;                                        \
		cursor.cap = n;                                      \
		rv = message__decode(&cursor, &message);             \
		assert(rv == 0);                                     \
		if (message.type != DQLITE_RESPONSE_##UPPER) {       \
			return DQLITE_ERROR;                         \
		}                                                    \
		n = message.words * 8;                               \
		p = buffer__advance(&c->read, n);                    \
		if (p == NULL) {                                     \
			return DQLITE_ERROR;                         \
		}                                                    \
		rv = read(c->fd, p, n);                              \
		if (rv != (int)n) {                                  \
			return DQLITE_ERROR;                         \
		}                                                    \
		cursor.p = p;                                        \
		cursor.cap = n;                                      \
		rv = response_##LOWER##__decode(&cursor, &response); \
		if (rv != 0) {                                       \
			return DQLITE_ERROR;                         \
		}                                                    \
	}

int client__send_open(struct client *c, const char *name)
{
	struct request_open request;
	request.filename = name;
	request.flags = 0;    /* TODO: this is unused, should we drop it? */
	request.vfs = "test"; /* TODO: this is unused, should we drop it? */
	REQUEST(open, OPEN);
	return 0;
}

int client__recv_db(struct client *c)
{
	struct response_db response;
	RESPONSE(db, DB);
	c->db_id = response.id;
	return 0;
}

int client__send_prepare(struct client *c, const char *sql)
{
	struct request_prepare request;
	request.db_id = c->db_id;
	request.sql = sql;
	REQUEST(prepare, PREPARE);
	return 0;
}

int client__recv_stmt(struct client *c, unsigned *stmt_id)
{
	struct response_stmt response;
	RESPONSE(stmt, STMT);
	*stmt_id = response.id;
	return 0;
}

int client__send_exec(struct client *c, unsigned stmt_id)
{
	struct request_exec request;
	request.db_id = c->db_id;
	request.stmt_id = stmt_id;
	REQUEST(exec, EXEC);
	return 0;
}

int client__recv_result(struct client *c,
			unsigned *last_insert_id,
			unsigned *rows_affected)
{
	struct response_result response;
	RESPONSE(result, RESULT);
	*last_insert_id = response.last_insert_id;
	*rows_affected = response.rows_affected;
	return 0;
}
