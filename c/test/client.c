#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <unistd.h>

#include <sqlite3.h>

#include "../src/binary.h"
#include "../src/request.h"
#include "../src/response.h"
#include "../include/dqlite.h"

#include "client.h"
#include "suite.h"

void test_client_init(struct test_client *c, int fd)
{
	assert(c != NULL);

	c->fd = fd;
}

int test_client_handshake(struct test_client *c){
	int err;
	uint64_t protocol;

	protocol = dqlite__flip64(DQLITE_PROTOCOL_VERSION);

	err = write(c->fd, &protocol, sizeof(protocol));
	if( err<0 ){
		test_suite_printf("failed to write to client socket: %s", strerror(errno));
		return 1;
	}

	return 0;
}

#define TEST_CLIENT__INIT			\
	int err;				\
	struct dqlite__request request;		\
	struct dqlite__response response;	\
	uv_buf_t bufs[3];			\
	dqlite__request_init(&request)

#define TEST_CLIENT__WRITE						\
	err = dqlite__request_encode(&request);				\
	if (err != 0) {							\
		test_suite_printf("failed to encode request: %s", &request.error); \
	}								\
									\
	dqlite__message_send_start(&request.message, bufs);		\
	err = write(c->fd, bufs[0].base, bufs[0].len);			\
	if( err<0 ){							\
		test_suite_printf("failed to write request header: %s", strerror(errno)); \
		dqlite__request_close(&request);			\
		return 1;						\
	}								\
	err = write(c->fd, bufs[1].base, bufs[1].len);			\
	if( err<0 ){							\
		test_suite_printf("failed to write request body: %s", strerror(errno)); \
		dqlite__request_close(&request);			\
		return 1;						\
	}								\
	dqlite__message_send_reset(&request.message);			\
	dqlite__request_close(&request);				\

#define TEST_CLIENT__READ						\
	dqlite__response_init(&response);				\
	dqlite__message_header_recv_start(&response.message, &bufs[0]);	\
									\
	err = read(c->fd, bufs[0].base, bufs[0].len);			\
	if( err<0 ){							\
		test_suite_printf("failed to read response header: %s", strerror(errno)); \
		dqlite__response_close(&response);			\
		return 1;						\
	}								\
									\
	err = dqlite__message_header_recv_done(&response.message);	\
	if (err != 0) {							\
		test_suite_printf("failed to handle response header: %s", response.message.error); \
		dqlite__response_close(&response);			\
		return 1;						\
	}								\
									\
	err = dqlite__message_body_recv_start(&response.message, &bufs[0]); \
	if (err != 0) {							\
		test_suite_printf("failed to start receiving body: %s", response.message.error); \
		dqlite__response_close(&response);			\
		return 1;						\
	}								\
									\
	err = read(c->fd, bufs[0].base, bufs[0].len);			\
	if( err<0 ){							\
		test_suite_printf("failed to read response body: %s", strerror(errno)); \
		dqlite__response_close(&response);			\
		return 1;						\
	}								\
									\
	err = dqlite__response_decode(&response);			\
	if (err != 0) {							\
		test_suite_printf("failed to decode response: %s", &response.error); \
	}

#define TEST_CLIENT__CLOSE						\
	dqlite__response_close(&response);				\
									\
	return 0

int test_client_leader(struct test_client *c, char **leader)
{
	TEST_CLIENT__INIT;

	request.type = DQLITE_REQUEST_LEADER;

	TEST_CLIENT__WRITE;
	TEST_CLIENT__READ;

	TEST_CLIENT__CLOSE;
}

int test_client_client(struct test_client *c, uint64_t *heartbeat)
{
	TEST_CLIENT__INIT;

	request.type = DQLITE_REQUEST_CLIENT;
	request.client.id = 123;

	TEST_CLIENT__WRITE;
	TEST_CLIENT__READ;

	TEST_CLIENT__CLOSE;
}

int test_client_open(struct test_client *c, const char *name, uint32_t *db_id)
{
	TEST_CLIENT__INIT;

	request.type = DQLITE_REQUEST_OPEN;
	request.open.name = "test.db";
	request.open.flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	request.open.vfs = "test";

	TEST_CLIENT__WRITE;
	TEST_CLIENT__READ;

	*db_id = response.db.id;

	TEST_CLIENT__CLOSE;
}

int test_client_prepare(struct test_client *c, uint32_t db_id, const char *sql, uint32_t *stmt_id)
{
	TEST_CLIENT__INIT;

	request.type = DQLITE_REQUEST_PREPARE;
	request.prepare.db_id = db_id;
	request.prepare.sql = sql;

	TEST_CLIENT__WRITE;
	TEST_CLIENT__READ;

	*stmt_id = response.stmt.id;

	TEST_CLIENT__CLOSE;
}

int test_client_exec(struct test_client *c, uint32_t db_id, uint32_t stmt_id)
{
	TEST_CLIENT__INIT;

	request.type = DQLITE_REQUEST_EXEC;
	request.exec.db_id = db_id;
	request.exec.stmt_id = stmt_id;

	TEST_CLIENT__WRITE;
	TEST_CLIENT__READ;

	TEST_CLIENT__CLOSE;
}

int test_client_query(struct test_client *c, uint32_t db_id, uint32_t stmt_id)
{
	TEST_CLIENT__INIT;

	request.type = DQLITE_REQUEST_QUERY;
	request.exec.db_id = db_id;
	request.exec.stmt_id = stmt_id;

	TEST_CLIENT__WRITE;
	TEST_CLIENT__READ;

	TEST_CLIENT__CLOSE;
}

int test_client_finalize(struct test_client *c, uint32_t db_id, uint32_t stmt_id)
{
	TEST_CLIENT__INIT;

	request.type = DQLITE_REQUEST_FINALIZE;
	request.finalize.db_id = db_id;
	request.finalize.stmt_id = stmt_id;

	TEST_CLIENT__WRITE;
	TEST_CLIENT__READ;

	TEST_CLIENT__CLOSE;
}

void test_client_close(struct test_client *c){
}
