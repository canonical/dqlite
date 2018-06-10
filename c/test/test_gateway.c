#include <CUnit/CUnit.h>

#include "../src/gateway.h"
#include "../src/response.h"

#include "cluster.h"
#include "suite.h"
#include "request.h"
#include "response.h"

static struct dqlite__gateway gateway;
static struct dqlite__request request;
struct dqlite__response *response;

void test_dqlite__gateway_setup()
{
	FILE *log = test_suite_dqlite_log();

	dqlite__request_init(&request);
	dqlite__gateway_init(&gateway, log, test_cluster());
}

void test_dqlite__gateway_teardown()
{
	dqlite__gateway_close(&gateway);
	dqlite__request_close(&request);
}

void test_dqlite__gateway_handle_connect()
{
	int err;
	struct test_response_welcome welcome;

	test_request_helo(&request.message, 123);
	dqlite__request_body_received(&request);

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	dqlite__request_processed(&request);

	welcome = test_response_welcome_parse(response);
	CU_ASSERT_STRING_EQUAL(welcome.leader,  "127.0.0.1:666");
}

void test_dqlite__gateway_handle_connect_wrong_request_type()
{
	int err;

	test_request_heartbeat(&request.message, 666);

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, DQLITE_PROTO);

	CU_ASSERT_STRING_EQUAL(gateway.error, "expected Helo, got Heartbeat");
}

void test_dqlite__gateway_heartbeat()
{
	int err;
	struct test_response_servers servers;

	test_dqlite__gateway_handle_connect();

	test_request_heartbeat(&request.message, 666);
	dqlite__request_body_received(&request);

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	dqlite__request_processed(&request);

	servers = test_response_servers_parse(response);

	CU_ASSERT_STRING_EQUAL(servers.addresses[0], "1.2.3.4:666");
	CU_ASSERT_STRING_EQUAL(servers.addresses[1], "5.6.7.8:666");
}

void test_dqlite__gateway_open()
{
	/* return; */
	/* int err; */
	/* struct test_request r; */
	/* Db_ptr ptr; */
	/* struct Db db; */

	/* test_dqlite__gateway_handle_connect(); */

	/* test_request_open(&r, "test.db"); */
	/* test_dqlite__request_write(&r); */

	/* err = dqlite__gateway_handle(&gateway, &request, &response); */
	/* CU_ASSERT_EQUAL_FATAL(err, 0); */

	/* TEST_DQLITE__RESPONSE_READ(ptr); */

	/* read_Db(&db, ptr); */

	/* CU_ASSERT_EQUAL(db.id, 1); */
}
