#include <stdint.h>

#include <CUnit/CUnit.h>

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
 * dqlite__request_message_suite
 */

void test_dqlite__request_body_received_too_large()
{
	int err;

	request.message.words = 1280000;

	err = dqlite__request_body_received(&request);

	CU_ASSERT_EQUAL(err, DQLITE_PARSE);
	CU_ASSERT_STRING_EQUAL(request.error, "request too large: 10240000");
}

void test_dqlite__request_body_received_malformed_helo()
{
	int err;
	request.message.type = DQLITE_HELO;
	request.message.words = 2;

	err = dqlite__request_body_received(&request);

	CU_ASSERT_EQUAL(err, DQLITE_PARSE);
	CU_ASSERT_STRING_EQUAL(request.error, "malformed Helo: 16 bytes");
}

void test_dqlite__request_body_received_malformed_heartbeat()
{
	int err;
	request.message.type = DQLITE_HEARTBEAT;
	request.message.words = 2;

	err = dqlite__request_body_received(&request);

	CU_ASSERT_EQUAL(err, DQLITE_PARSE);
	CU_ASSERT_STRING_EQUAL(request.error, "malformed Heartbeat: 16 bytes");
}

void test_dqlite__request_body_received_helo()
{
	int err;

	test_request_helo(&request.message, 123);

	err = dqlite__request_body_received(&request);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(request.helo.client_id, 123);
}

void test_dqlite__request_body_received_heartbeat()
{
	int err;

	test_request_heartbeat(&request.message, 666);

	err = dqlite__request_body_received(&request);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(request.heartbeat.timestamp, 666);
}

void test_dqlite__request_body_received_open()
{
	int err;

	test_request_open(&request.message, "test.db");

	err = dqlite__request_body_received(&request);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	CU_ASSERT_STRING_EQUAL(request.open.name, "test.db");
}

/*
 * dqlite__request_name_type_suite
 */

void test_dqlite__request_type__name_helo()
{
	int type;
	const char* name;

	request.message.type = DQLITE_HELO;

	type = dqlite__request_type(&request);
	name = dqlite__request_type_name(&request);

	CU_ASSERT_EQUAL(type, DQLITE_HELO);
	CU_ASSERT_STRING_EQUAL(name, "Helo");
}

void test_dqlite__request_type__name_heartbeat()
{
	int type;
	const char* name;

	request.message.type = DQLITE_HEARTBEAT;

	type = dqlite__request_type(&request);
	name = dqlite__request_type_name(&request);

	CU_ASSERT_EQUAL(type, DQLITE_HEARTBEAT);
	CU_ASSERT_STRING_EQUAL(name, "Heartbeat");
}

void test_dqlite__request_type__name_open()
{
	int type;
	const char* name;

	request.message.type = DQLITE_OPEN;

	type = dqlite__request_type(&request);
	name = dqlite__request_type_name(&request);

	CU_ASSERT_EQUAL(type, DQLITE_OPEN);
	CU_ASSERT_STRING_EQUAL(name, "Open");
}

void test_dqlite__request_type__name_unknown()
{
	int type;
	const char* name;

	request.message.type = 255;

	type = dqlite__request_type(&request);
	name = dqlite__request_type_name(&request);

	CU_ASSERT_EQUAL(type, 255);
	CU_ASSERT_STRING_EQUAL(name, "Unknown");
}
