#include <stdint.h>

#include <CUnit/CUnit.h>

#include "../src/response.h"

#include "response.h"

static struct dqlite__response response;

void test_dqlite__response_setup()
{
	dqlite__response_init(&response);
}

void test_dqlite__response_teardown()
{
	dqlite__response_close(&response);
}

void test_dqlite__response_welcome()
{
	int err;
	struct test_response_welcome welcome;

	err = dqlite__response_welcome(&response, "1.2.3.4:666", 15000);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	welcome = test_response_welcome_parse(&response);

	CU_ASSERT_STRING_EQUAL(welcome.leader, "1.2.3.4:666");
	CU_ASSERT_EQUAL(welcome.heartbeat_timeout, 15000);
}

void test_dqlite__response_servers()
{
	int err;
	struct test_response_servers servers;
	const char *addresses[3] = {
		"1.2.3.4:666",
		"5.6.7.8:999",
		NULL
	};

	err = dqlite__response_servers(&response, addresses);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	servers = test_response_servers_parse(&response);

	CU_ASSERT_STRING_EQUAL(servers.addresses[0], "1.2.3.4:666");
	CU_ASSERT_STRING_EQUAL(servers.addresses[1], "5.6.7.8:999");
	CU_ASSERT_PTR_NULL(servers.addresses[2]);
}

void test_dqlite__response_db()
{
	int err;
	struct test_response_db db;

	err = dqlite__response_db(&response, 123);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	db = test_response_db_parse(&response);

	CU_ASSERT_EQUAL(db.id, 123);
}
