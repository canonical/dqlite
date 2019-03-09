#include <stddef.h>
#include <stdint.h>
#include <unistd.h>

#include <sqlite3.h>

#include "../include/dqlite.h"

#include "../src/lib/byte.h"

#include "../src/message.h"
#include "../src/request.h"
#include "../src/response.h"

#include "client.h"
#include "munit.h"

void test_client_init(struct test_client *c, int fd)
{
	munit_assert_ptr_not_null(c);

	c->fd = fd;
	request_init(&c->request);
	response_init(&c->response);
}

void test_client_handshake(struct test_client *c)
{
	int err;
	uint64_t protocol;

	protocol = byte__flip64(DQLITE_PROTOCOL_VERSION);

	err = write(c->fd, &protocol, sizeof(protocol));
	if (err < 0) {
		munit_errorf("failed to write to client socket: %s",
			     strerror(errno));
	}
}

static void test_client__write(struct test_client *c)
{
	int err;

	/* Encode the request. */
	err = request_encode(&c->request);
	if (err != 0) {
		munit_errorf("failed to encode request: %s", c->request.error);
	}

	/* Write out the request data. */
	message__send_start(&c->request.message, c->bufs);
	err = write(c->fd, c->bufs[0].base, c->bufs[0].len);
	if (err < 0) {
		munit_errorf("failed to write request header: %s",
			     strerror(errno));
		request_close(&c->request);
	}
	err = write(c->fd, c->bufs[1].base, c->bufs[1].len);
	if (err < 0) {
		munit_errorf("failed to write request body: %s",
			     strerror(errno));
		request_close(&c->request);
	}

	/* Reset the request message. */
	message__send_reset(&c->request.message);
}

static void test_client__read(struct test_client *c)
{
	int n;
	int err;
	message__header_recv_start(&c->response.message, &c->bufs[0]);

	err = read(c->fd, c->bufs[0].base, c->bufs[0].len);
	if (err < 0) {
		munit_errorf("failed to read response header: %s",
			     strerror(errno));
	}

	err = message__header_recv_done(&c->response.message);
	if (err != 0) {
		munit_errorf("failed to handle response header: %s",
			     c->response.message.error);
	}

	err = message__body_recv_start(&c->response.message, &c->bufs[0]);
	if (err != 0) {
		munit_errorf("failed to start receiving body: %s",
			     c->response.message.error);
	}

	n = read(c->fd, c->bufs[0].base, c->bufs[0].len);
	if (n < 0) {
		munit_errorf("failed to read response body: %s", strerror(n));
	}
	if (n != (int)c->bufs[0].len) {
		munit_error("short read of response body");
	}

	err = response_decode(&c->response);
	if (err != 0) {
		munit_errorf("failed to decode response: %s",
			     c->response.error);
	}

	if (c->response.type == DQLITE_RESPONSE_FAILURE) {
		munit_errorf("request failed: %s (%lu)",
			     c->response.failure.message,
			     c->response.failure.code);
	}

	/* Reset the message in all cases except for rows responses, which need
	 * manual decoding. */
	if (c->response.type != DQLITE_RESPONSE_ROWS) {
		message__recv_reset(&c->response.message);
	}
}

void test_client_leader(struct test_client *c, char **leader)
{
	(void)leader;

	c->request.type = DQLITE_REQUEST_LEADER;

	test_client__write(c);
	test_client__read(c);
}

void test_client_client(struct test_client *c, uint64_t *heartbeat)
{
	(void)heartbeat;

	c->request.type = DQLITE_REQUEST_CLIENT;
	c->request.client.id = 123;

	test_client__write(c);
	test_client__read(c);
}

void test_client_open(struct test_client *c, const char *name, uint32_t *db_id)
{
	(void)name;

	c->request.type = DQLITE_REQUEST_OPEN;
	c->request.open.name = "test.db";
	c->request.open.flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	c->request.open.vfs = "test";

	test_client__write(c);
	test_client__read(c);

	*db_id = c->response.db.id;
}

void test_client_prepare(struct test_client *c,
			 uint32_t db_id,
			 const char *sql,
			 uint32_t *stmt_id)
{
	c->request.type = DQLITE_REQUEST_PREPARE;
	c->request.prepare.db_id = db_id;
	c->request.prepare.sql = sql;

	test_client__write(c);
	test_client__read(c);

	*stmt_id = c->response.stmt.id;
}

void test_client_exec(struct test_client *c,
		      uint32_t db_id,
		      uint32_t stmt_id,
		      struct test_client_result *result)
{
	c->request.type = DQLITE_REQUEST_EXEC;
	c->request.exec.db_id = db_id;
	c->request.exec.stmt_id = stmt_id;

	test_client__write(c);
	test_client__read(c);

	result->last_insert_id = c->response.result.last_insert_id;
	result->rows_affected = c->response.result.rows_affected;
}

static void test_client_get_row(struct message *m,
				uint64_t column_count,
				struct test_client_row **row,
				int *done)
{
	uint8_t *types = munit_malloc(column_count * sizeof *types);
	void **values = munit_malloc(column_count * sizeof *values);
	int header_bits = column_count * 4;
	int pad_bits = 0;
	int trailing_bits = header_bits % MESSAGE__WORD_BITS;
	int header_size;
	int i;
	int err;

	/* Each column needs a 4 byte slot to store the column type. The row
	 * header must be padded to reach word boundary. */
	if (trailing_bits != 0) {
		pad_bits = MESSAGE__WORD_BITS - trailing_bits;
	}

	header_size =
	    (header_bits + pad_bits) / MESSAGE__WORD_BITS * MESSAGE__WORD_SIZE;

	for (i = 0; i < header_size; i++) {
		uint8_t slot;
		int err;
		int index = i * 2;

		err = message__body_get_uint8(m, &slot);
		munit_assert_int(err, ==, 0);

		/* Rows PART marker */
		if (slot == 0xee) {
			*row = NULL;
			*done = 0;
			free(types);
			free(values);
			return;
		}

		/* Rows DONE marker */
		if (slot == 0xff) {
			*row = NULL;
			*done = 1;
			free(types);
			free(values);
			return;
		}

		if (index >= (int)column_count) {
			continue; /* This is padding. */
		}

		types[index] = slot & 0x0f;

		index++;

		if (index >= (int)column_count) {
			continue; /* This is padding. */
		}

		types[index] = slot >> 4;
	}

	for (i = 0; i < (int)column_count; i++) {
		switch (types[i]) {
			case SQLITE_INTEGER:
				values[i] = munit_malloc(sizeof(int64_t));
				err = message__body_get_int64(
				    m, (int64_t *)values[i]);
				munit_assert_int(err, ==, 0);
				break;
			case SQLITE_TEXT:
				break;
			default:
				munit_errorf("unknown data type: %d", types[i]);
		}
	}

	*row = munit_malloc(sizeof **row);

	(*row)->types = types;
	(*row)->values = values;
	(*row)->next = NULL;
}

static int test_client_query_batch(struct test_client *c,
				   struct test_client_rows *rows,
				   struct test_client_row *prev)
{
	int i;
	int err;
	int done;
	struct test_client_row *next;

	test_client__read(c);

	/* TODO: the request object decodes the eof field as first one, but the
	 * rows are actually written first, by the gateway. */
	rows->column_count = c->response.rows.eof;

	if (prev != NULL) {
		free(rows->column_names);
	}
	rows->column_names =
	    munit_malloc(rows->column_count * sizeof *rows->column_names);

	for (i = 0; i < (int)rows->column_count; i++) {
		err = message__body_get_text(&c->response.message,
					     &rows->column_names[i]);
		munit_assert_int(err, ==, 0);
	}

	do {
		test_client_get_row(&c->response.message, rows->column_count,
				    &next, &done);
		if (prev == NULL) {
			rows->next = next;
		} else {
			struct test_client_row *prev_tail;
			struct test_client_row *tail;
			prev_tail = NULL;
			tail = prev;
			while (tail != NULL) {
				prev_tail = tail;
				tail = tail->next;
			}
			prev_tail->next = next;
		}
		prev = next;
	} while (next != NULL);

	return done;
}

void test_client_query(struct test_client *c,
		       uint32_t db_id,
		       uint32_t stmt_id,
		       struct test_client_rows *rows)
{
	int done;

	c->request.type = DQLITE_REQUEST_QUERY;
	c->request.exec.db_id = db_id;
	c->request.exec.stmt_id = stmt_id;

	test_client__write(c);

	rows->message = &c->response.message;
	rows->next = NULL;

	do {
		done = test_client_query_batch(c, rows, rows->next);
		if (done) {
			break;
		}
		message__recv_reset(rows->message);
	} while (1);
}

void test_client_rows_close(struct test_client_rows *rows)
{
	struct test_client_row *prev;
	struct test_client_row *next;
	unsigned i;

	message__recv_reset(rows->message);

	prev = NULL;
	next = rows->next;
	while (next != NULL) {
		prev = next;
		next = next->next;
		for (i = 0; i < rows->column_count; i++) {
			free(prev->values[i]);
		}
		free(prev->values);
		free(prev->types);
		free(prev);
	}
	free(rows->column_names);
}

void test_client_finalize(struct test_client *c,
			  uint32_t db_id,
			  uint32_t stmt_id)
{
	c->request.type = DQLITE_REQUEST_FINALIZE;
	c->request.finalize.db_id = db_id;
	c->request.finalize.stmt_id = stmt_id;

	test_client__write(c);
	test_client__read(c);
}

void test_client_close(struct test_client *c)
{
	response_close(&c->response);
	request_close(&c->request);
}
