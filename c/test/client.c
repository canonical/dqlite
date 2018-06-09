#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <unistd.h>

#include <capnp_c.h>
#include <sqlite3.h>

#include "../src/protocol.capnp.h"
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

	version = capn_flip32(DQLITE_PROTOCOL_VERSION);

	err = write(c->fd, &version, 4);
	if( err<0 ){
		test_suite_printf("failed to write to client socket: %s", strerror(errno));
		return 1;
	}

	return 0;
}

int test_client_helo(struct test_client *c, char **leader, uint8_t *heartbeat)
{
	int err;
	struct capn session;
	capn_ptr root;
	struct capn_segment *segment;
	struct Request request;
	Request_ptr requestPtr;
	ssize_t size;
	FILE *in;

	capn_init_malloc(&session);
	root = capn_root(&session);
	segment = root.seg;

	request.which = Request_helo;
	requestPtr = new_Request(segment);
	write_Request(&request, requestPtr);

	err = capn_setp(root, 0, requestPtr.p);
	if( err ){
		test_suite_printf("failed to marshal leader request: %d", err);
		return 1;
	}

	struct test_request r;
	memset(&r, 0, sizeof(r));
	test_request_helo(&r);
	write(c->fd, r.buf, r.size);

	in = fdopen(c->fd, "r");
	CU_ASSERT_PTR_NOT_NULL( in );

	err = capn_init_fp(&session, in, 0);
	CU_ASSERT_EQUAL(err, 0);

	Welcome_ptr welcomePtr;
	struct Welcome welcome;

	root = capn_root(&session);

	welcomePtr.p = capn_getp(root, 0 /* off */, 1 /* resolve */);
	read_Welcome(&welcome, welcomePtr);

	CU_ASSERT_STRING_EQUAL(welcome.leader.str,  "127.0.0.1:666");

	return 0;
}

void test_client_close(struct test_client *c){
}
