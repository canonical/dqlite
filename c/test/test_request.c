#include <stdint.h>

#include <CUnit/CUnit.h>

#include "../src/message.h"
#include "../src/request.h"

#include "message.h"

static struct dqlite__request request;

void test_dqlite__request_setup()
{
	dqlite__request_init(&request);
}

void test_dqlite__request_teardown()
{
	dqlite__request_close(&request);
}

void test_dqlite__request_decode_leader()
{
	int err;

	test_message_send_leader(0, &request.message);

	err = dqlite__request_decode(&request);
	CU_ASSERT_EQUAL(err, 0);
}

void test_dqlite__request_decode_client()
{
	int err;

	test_message_send_client(123, &request.message);

	err = dqlite__request_decode(&request);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(request.client.id, 123);
}

void test_dqlite__request_decode_heartbeat()
{
	int err;

	test_message_send_heartbeat(666, &request.message);

	err = dqlite__request_decode(&request);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(request.heartbeat.timestamp, 666);
}

void test_dqlite__request_decode_open()
{
	int err;

	test_message_send_open("test.db", 123, "volatile", &request.message);

	err = dqlite__request_decode(&request);
	CU_ASSERT_EQUAL_FATAL(err, 0);


	CU_ASSERT_STRING_EQUAL(request.open.name, "test.db");
	CU_ASSERT_EQUAL(request.open.flags, 123);
	CU_ASSERT_STRING_EQUAL(request.open.vfs, "volatile");
}
