#include <assert.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>

#include <CUnit/CUnit.h>

#include "../src/message.h"
#include "../include/dqlite.h"

#include "suite.h"
#include "request.h"

void test_request_helo(struct dqlite__message *m, uint64_t client_id) {
	int err;

	assert(m != NULL);

	err = dqlite__message_write_uint64(m, client_id);
	CU_ASSERT_EQUAL(err, 0);

	dqlite__message_flush(m, DQLITE_HELO, 0);
}

void test_request_heartbeat(struct dqlite__message *m, uint64_t timestamp) {
	int err;

	assert(m != NULL);

	err = dqlite__message_write_uint64(m, timestamp);
	CU_ASSERT_EQUAL(err, 0);

	dqlite__message_flush(m, DQLITE_HEARTBEAT, 0);
}

void test_request_open(struct dqlite__message *m, const char *name) {
	int err;

	assert(m != NULL);

	err = dqlite__message_write_text(m, name);
	CU_ASSERT_EQUAL(err, 0);

	dqlite__message_flush(m, DQLITE_OPEN, 0);
}

