#include <stdint.h>

#include <CUnit/CUnit.h>

#include "../src/message.h"
#include "../src/request.h"

#include "request.h"

static struct dqlite__request request;

void test_dqlite__request_setup()
{
	dqlite__request_init(&request);
}

void test_dqlite__request_teardown()
{
	dqlite__request_close(&request);
}

/*
 * dqlite__request_decode_suite
 */

/* void test_dqlite__request_old_body_received_too_large() */
/* { */
/* 	int err; */

/* 	request.message.words = 1280000; */

/* 	err = dqlite__request_old_body_received(&request); */

/* 	CU_ASSERT_EQUAL(err, DQLITE_PARSE); */
/* 	CU_ASSERT_STRING_EQUAL(request.error, "request too large: 10240000"); */
/* } */

/* void test_dqlite__request_old_body_received_malformed_helo() */
/* { */
/* 	int err; */
/* 	request.message.type = DQLITE_HELO; */
/* 	request.message.words = 2; */

/* 	err = dqlite__request_old_body_received(&request); */

/* 	CU_ASSERT_EQUAL(err, DQLITE_PARSE); */
/* 	CU_ASSERT_STRING_EQUAL(request.error, "malformed Helo: 16 bytes"); */
/* } */

/* void test_dqlite__request_old_body_received_malformed_heartbeat() */
/* { */
/* 	int err; */
/* 	request.message.type = DQLITE_HEARTBEAT; */
/* 	request.message.words = 2; */

/* 	err = dqlite__request_old_body_received(&request); */

/* 	CU_ASSERT_EQUAL(err, DQLITE_PARSE); */
/* 	CU_ASSERT_STRING_EQUAL(request.error, "malformed Heartbeat: 16 bytes"); */
/* } */

void test_dqlite__request_decode_helo()
{
	int err;

	test_message_send_helo(123, &request.message);

	err = dqlite__request_decode(&request);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(request.helo.client_id, 123);
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
