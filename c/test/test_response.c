#include <stdint.h>

#include <CUnit/CUnit.h>

#include "../src/message.h"
#include "../src/response.h"

#include "message.h"

static struct dqlite__response response;

void test_dqlite__response_setup()
{
	dqlite__response_init(&response);
}

void test_dqlite__response_teardown()
{
	dqlite__response_close(&response);
}

void test_dqlite__response_server()
{
	int err;

	test_message_send_server("1.2.3.4:666", &response.message);

	err = dqlite__response_decode(&response);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_STRING_EQUAL(response.server.address, "1.2.3.4:666");
}

void test_dqlite__response_welcome()
{
	int err;

	test_message_send_welcome(15000, &response.message);

	err = dqlite__response_decode(&response);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(response.welcome.heartbeat_timeout, 15000);
}

void test_dqlite__response_servers()
{
	int err;

	text_t addresses[3] = {
		"1.2.3.4:666",
		"5.6.7.8:999",
		NULL
	};

	test_message_send_servers(addresses, &response.message);

	err = dqlite__response_decode(&response);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_PTR_NOT_NULL(response.servers.addresses);

	CU_ASSERT_STRING_EQUAL(response.servers.addresses[0], "1.2.3.4:666");
	CU_ASSERT_STRING_EQUAL(response.servers.addresses[1], "5.6.7.8:999");
	CU_ASSERT_PTR_NULL(response.servers.addresses[2]);

	sqlite3_free(response.servers.addresses);

}

void test_dqlite__response_db()
{
	int err;

	test_message_send_db(123, 0 /* __pad__ */, &response.message);

	err = dqlite__response_decode(&response);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(response.db.id, 123);
}
