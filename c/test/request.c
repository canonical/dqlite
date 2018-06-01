#include <assert.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>

#include <CUnit/CUnit.h>
#include <capnp_c.h>

#include "../src/protocol.capnp.h"

#include "suite.h"
#include "request.h"

/* Initialize a test request */
void test__request_init(struct test_request *r, enum Request_which which) {
	int err;

	capn_init_malloc(&r->session);
	r->root = capn_root(&r->session);
	r->segment = r->root.seg;
	r->request_ptr = new_Request(r->segment);

	r->request.which = which;

	write_Request(&r->request, r->request_ptr);

	err = capn_setp(capn_root(&r->session), 0, r->request_ptr.p);
	if (err != 0){
		CU_FAIL_FATAL("failed init request");
	}

	memset(r->buf, 0, TEST_REQUEST_BUF_SIZE);
	r->size =0;
}

void test_request_leader(struct test_request *r) {
	test__request_init(r, Request_leader);
	r->size = capn_write_mem(&r->session, r->buf, TEST_REQUEST_BUF_SIZE, 0);

	if(r->size <= 0){
		CU_FAIL_FATAL("failed to write request");
	}
}

void test_request_heartbeat(struct test_request *r) {
	test__request_init(r, Request_heartbeat);
	r->size = capn_write_mem(&r->session, r->buf, TEST_REQUEST_BUF_SIZE, 0);

	if(r->size <= 0){
		CU_FAIL_FATAL("failed to write request");
	}
}
