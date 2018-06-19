#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <unistd.h>

#include <sqlite3.h>

#include "../src/binary.h"
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
	uint64_t protocol;

	protocol = dqlite__flip64(DQLITE_PROTOCOL_VERSION);

	err = write(c->fd, &protocol, sizeof(protocol));
	if( err<0 ){
		test_suite_printf("failed to write to client socket: %s", strerror(errno));
		return 1;
	}

	return 0;
}

#define TEST_CLIENT__INIT \
	int err;				\
	struct test_request request;		\
	uv_buf_t bufs[3];			\
	test_request_init(&request)

#define TEST_CLIENT__WRITE \
	dqlite__message_send_start(&request.message, bufs);	\
	err = write(c->fd, bufs[0].base, bufs[0].len); \
	if( err<0 ){ \
		test_suite_printf("failed to write request header: %s", strerror(errno)); \
		dqlite__message_close(&request.message);			\
		return 1; \
	} \
	err = write(c->fd, bufs[1].base, bufs[1].len); \
	if( err<0 ){ \
		test_suite_printf("failed to write request body: %s", strerror(errno)); \
		dqlite__message_close(&request.message);			\
		return 1; \
	} \
	test_request_close(&request); \
	return 0

int test_client_helo(struct test_client *c, char **leader, uint8_t *heartbeat)
{
	TEST_CLIENT__INIT;

	request.type = DQLITE_HELO;
	request.helo.client_id = 123;

	err = test_request_encode(&request);
	if (err != 0) {
		test_suite_printf("failed to encode request: %s", &request.error);
	}

	TEST_CLIENT__WRITE;

	return 0;
}

int test_client_open(struct test_client *c, const char *name, uint64_t *id)
{
	TEST_CLIENT__INIT;

	//test_request_open(&message, "test.db");

	TEST_CLIENT__WRITE;

	return 0;
}

void test_client_close(struct test_client *c){
}
