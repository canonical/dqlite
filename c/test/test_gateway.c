#include <CUnit/CUnit.h>

#include "../src/gateway.h"
#include "../src/protocol.capnp.h"
#include "../src/response.h"

#include "cluster.h"
#include "suite.h"
#include "request.h"

static struct dqlite__gateway gateway;
static struct dqlite__request request;
struct dqlite__response *response;
static struct capn session;
static capn_ptr root;

void test_dqlite__gateway_setup()
{
	FILE *log = test_suite_dqlite_log();

	dqlite__gateway_init(&gateway, log, test_cluster());
	dqlite__request_init(&request);
	capn_init_malloc(&session);
}

void test_dqlite__gateway_teardown()
{
	dqlite__request_close(&request);
	dqlite__gateway_close(&gateway);
}


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

static void test_dqlite__response_read() {
	int err;
	uint8_t *data;
	size_t size;

	data = dqlite__response_data(response);
	CU_ASSERT_PTR_NOT_NULL_FATAL(data);

	size = dqlite__response_size(response);
	CU_ASSERT_FATAL(size);

	err = capn_init_mem(&session, data, size, 0);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	root = capn_root(&session);
}

#define TEST_DQLITE__RESPONSE_READ(PTR) \
	test_dqlite__response_read(); \
	PTR.p = capn_getp(root, 0, 1);

void test_dqlite__gateway_handle_connect()
{
	int err;
	struct test_request r;
	Server_ptr ptr;
	struct Server server;

	test_request_leader(&r);
	test_dqlite__request_write(&r);

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	TEST_DQLITE__RESPONSE_READ(ptr);

	read_Server(&server, ptr);

	CU_ASSERT_STRING_EQUAL(server.address.str,  "127.0.0.1:666");
}

void test_dqlite__gateway_handle_connect_wrong_request_type()
{
	int err;
	struct test_request r;

	test_request_heartbeat(&r);
	test_dqlite__request_write(&r);

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, DQLITE_PROTO);

	CU_ASSERT_STRING_EQUAL(gateway.error, "expected Leader, got Heartbeat");
}
