#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <unistd.h>

#include <sqlite3.h>

#include "../src/message.h"
#include "../include/dqlite.h"

#include "client.h"
#include "request.h"
#include "suite.h"

void test_client_init(struct test_client *c, int fd)
{
	assert(c != NULL);

	c->fd = fd;
}

int test_client_handshake(struct test_client *c){
	int err;
	uint32_t version;

	version = dqlite__message_flip32(DQLITE_PROTOCOL_VERSION);

	err = write(c->fd, &version, 4);
	if( err<0 ){
		test_suite_printf("failed to write to client socket: %s", strerror(errno));
		return 1;
	}

	return 0;
}

#define TEST_CLIENT__INIT \
	int err;				\
	struct dqlite__message message;		\
	uint8_t *buf;				\
	size_t len;				\
	dqlite__message_init(&message)

#define TEST_CLIENT__WRITE \
	dqlite__message_header_buf(&message, &buf, &len);	\
	err = write(c->fd, buf, len); \
	if( err<0 ){ \
		test_suite_printf("failed to write request header: %s", strerror(errno)); \
		dqlite__message_close(&message);			\
		return 1; \
	} \
	dqlite__message_body_buf(&message, &buf, &len);	\
	err = write(c->fd, buf, len); \
	if( err<0 ){ \
		test_suite_printf("failed to write request body: %s", strerror(errno)); \
		dqlite__message_close(&message);			\
		return 1; \
	} \
	dqlite__message_close(&message); \
	return 0

int test_client_helo(struct test_client *c, char **leader, uint8_t *heartbeat)
{
	TEST_CLIENT__INIT;

	test_request_helo(&message, 123);

	TEST_CLIENT__WRITE;

	return 0;
}

int test_client_open(struct test_client *c, const char *name, uint64_t *id)
{
	TEST_CLIENT__INIT;

	test_request_open(&message, "test.db");

	TEST_CLIENT__WRITE;

	return 0;
}

void test_client_close(struct test_client *c){
}
