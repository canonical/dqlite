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
 * dqlite__request_preamble_suite
 */

void test_dqlite__request_preamble_size()
{
	size_t size = dqlite__request_preamble_size(&request);

	CU_ASSERT_EQUAL(size, 4);
}

void test_dqlite__request_preamble_one_segment()
{
	int err;
	uint8_t buf[4] = {0, 0, 0, 0};

	err = dqlite__request_preamble(&request, buf);

	CU_ASSERT_EQUAL(err, 0);
}

void test_dqlite__request_preamble_two_segments()
{
	int err;
	uint8_t buf[4] = {1, 0, 0, 0};

	err = dqlite__request_preamble(&request, buf);

	CU_ASSERT_EQUAL(err, DQLITE_REQUEST_ERR_PARSE);
	CU_ASSERT_STRING_EQUAL(request.error, "too many segments: 2");
}


/*
 * dqlite__request_header_suite
 */

void test_dqlite__request_header_size()
{
	int err;
	uint8_t buf[4] = {0, 0, 0, 0};
	size_t size;

	err = dqlite__request_preamble(&request, buf);

	CU_ASSERT_EQUAL_FATAL(err, 0);

	size = dqlite__request_header_size(&request);

	CU_ASSERT_EQUAL(size, 4);
}

void test_dqlite__request_header_valid_segment(){
	int err;
	uint8_t buf1[4] = {0, 0, 0, 0};
	uint8_t buf2[4] = {1, 1, 0, 0};

	err = dqlite__request_preamble(&request, buf1);

	CU_ASSERT_EQUAL_FATAL(err, 0);

	err = dqlite__request_header(&request, buf2);

	CU_ASSERT_EQUAL(err, 0);
}

void test_dqlite__request_header_empty_segment()
{
	int err;
	uint8_t buf1[4] = {0, 0, 0, 0};
	uint8_t buf2[4] = {0, 0, 0, 0};

	err = dqlite__request_preamble(&request, buf1);

	CU_ASSERT_EQUAL_FATAL(err, 0);

	err = dqlite__request_header(&request, buf2);

	CU_ASSERT_EQUAL(err, DQLITE_REQUEST_ERR_PARSE);
	CU_ASSERT_STRING_EQUAL(request.error, "invalid segment size: 0");
}

void test_dqlite__request_header_big_segment()
{
	int err;
	uint8_t buf1[4] = {0, 0, 0, 0};
	uint8_t buf2[4] = {0, 0, 0, 1};

	err = dqlite__request_preamble(&request, buf1);

	CU_ASSERT_EQUAL_FATAL(err, 0);

	err = dqlite__request_header(&request, buf2);

	CU_ASSERT_EQUAL(err, DQLITE_REQUEST_ERR_PARSE);
	CU_ASSERT_STRING_EQUAL(request.error, "invalid segment size: 16777216");
}


/*
 * dqlite__request_header_suite
 */

static void test_dqlite__request_write(struct test_request *r)
{
	int err;

	err = dqlite__request_preamble(&request, r->buf);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	err = dqlite__request_header(&request, r->buf + 4);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	err = dqlite__request_data(&request, r->buf + 8);
	CU_ASSERT_EQUAL_FATAL(err, 0);
}

void test_dqlite__request_type_leader()
{
	struct test_request r;
	int type;
	const char* name;

	test_request_leader(&r);

	test_dqlite__request_write(&r);

	type = dqlite__request_type(&request);
	name = dqlite__request_type_name(&request);

	CU_ASSERT_EQUAL(type, DQLITE_REQUEST_LEADER);
	CU_ASSERT_STRING_EQUAL(name, "Leader");
}

void test_dqlite__request_type_heartbeat()
{
	struct test_request r;
	int type;
	const char* name;

	test_request_heartbeat(&r);

	test_dqlite__request_write(&r);

	type = dqlite__request_type(&request);
	name = dqlite__request_type_name(&request);

	CU_ASSERT_EQUAL(type, DQLITE_REQUEST_HEARTBEAT);
	CU_ASSERT_STRING_EQUAL(name, "Heartbeat");
}

void test_dqlite__request_type_unknown()
{
	struct test_request r;
	int type;
	const char* name;

	test_request_heartbeat(&r);

	/* The 16-th byte holds the type number */
	*(r.buf + 16) = 255;

	test_dqlite__request_write(&r);

	type = dqlite__request_type(&request);
	name = dqlite__request_type_name(&request);

	CU_ASSERT_EQUAL(type, 255);
	CU_ASSERT_STRING_EQUAL(name, "Unknown");
}
