#ifndef DQLITE_TEST_REQUEST_H
#define DQLITE_TEST_REQUEST_H

#include <unistd.h>
#include <stdint.h>

#include <capnp_c.h>

#include "../src/protocol.capnp.h"

#define TEST_REQUEST_BUF_SIZE 4096

/* Helper to write dqlite requests */
struct test_request {
	struct capn          session;
	capn_ptr             root;
	struct capn_segment *segment;
	struct Request       request;
	Request_ptr          request_ptr;
	uint8_t              buf[TEST_REQUEST_BUF_SIZE];
	size_t               size;
};

/* Write a Helo request */
void test_request_helo(struct test_request *r);

/* Write a Heartbeat request */
void test_request_heartbeat(struct test_request *r);

#endif /* DQLITE_TEST_REQUEST_H */
