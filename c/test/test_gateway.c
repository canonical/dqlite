#include <CUnit/CUnit.h>

#include "../src/gateway.h"
#include "../src/response.h"
#include "../src/vfs.h"

#include "cluster.h"
#include "suite.h"
#include "request.h"
#include "response.h"

static sqlite3_vfs* vfs;
static struct dqlite__gateway gateway;
static struct dqlite__request request;
struct dqlite__response *response;

void test_dqlite__gateway_setup()
{
	FILE *log = test_suite_dqlite_log();
	int err;

	err = dqlite__vfs_register("volatile", &vfs);

	if (err != 0) {
		test_suite_errorf("failed to register vfs: %s - %d", sqlite3_errstr(err), err);
		CU_FAIL("test setup failed");
	}

	dqlite__request_init(&request);
	dqlite__gateway_init(&gateway, log, test_cluster());
}

void test_dqlite__gateway_teardown()
{
	dqlite__gateway_close(&gateway);
	dqlite__request_close(&request);
	dqlite__vfs_unregister(vfs);
}

void test_dqlite__gateway_helo()
{
	int err;

	request.type = DQLITE_HELO;
	request.helo.client_id = 123;

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_PTR_NOT_NULL(response);
	CU_ASSERT_EQUAL(response->type, DQLITE_WELCOME);

	CU_ASSERT_STRING_EQUAL(response->welcome.leader,  "127.0.0.1:666");
}

void test_dqlite__gateway_heartbeat()
{
	int err;

	request.type = DQLITE_HEARTBEAT;
	request.heartbeat.timestamp = 12345;

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_PTR_NOT_NULL(response);
	CU_ASSERT_EQUAL(response->type, DQLITE_SERVERS);

	CU_ASSERT_STRING_EQUAL(response->servers.addresses[0], "1.2.3.4:666");
	CU_ASSERT_STRING_EQUAL(response->servers.addresses[1], "5.6.7.8:666");
	CU_ASSERT_PTR_NULL(response->servers.addresses[2]);
}

void test_dqlite__gateway_open()
{
	int err;

	request.type = DQLITE_OPEN;
	request.open.name = "test.db";
	request.open.flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	request.open.vfs = "volatile";

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_PTR_NOT_NULL(response);
	CU_ASSERT_EQUAL(response->type, DQLITE_DB);

	CU_ASSERT_EQUAL(response->db.id, 0);
}

void test_dqlite__gateway_open_error()
{
	int err;

	request.type = DQLITE_OPEN;
	request.open.name = "test.db";
	request.open.flags = SQLITE_OPEN_CREATE;
	request.open.vfs = "volatile";

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_PTR_NOT_NULL(response);

	CU_ASSERT_EQUAL(response->type, DQLITE_DB_ERROR);
	CU_ASSERT_EQUAL(response->db_error.code, SQLITE_MISUSE);
	CU_ASSERT_EQUAL(response->db_error.extended_code, SQLITE_MISUSE);
	CU_ASSERT_STRING_EQUAL(response->db_error.description, "bad parameter or other API misuse");
}

void test_dqlite__gateway_prepare()
{
	int err;

	request.type = DQLITE_OPEN;
	request.open.name = "test.db";
	request.open.flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	request.open.vfs = "volatile";

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, 0);

	request.type = DQLITE_PREPARE;
	request.prepare.db_id = response->db.id;
	request.prepare.sql = "CREATE TABLE foo (n INT)";

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_PTR_NOT_NULL(response);
	CU_ASSERT_EQUAL(response->type, DQLITE_STMT);

	CU_ASSERT_EQUAL(response->stmt.db_id, 0);
	CU_ASSERT_EQUAL(response->stmt.id, 0);
}

void test_dqlite__gateway_prepare_error()
{
	int err;

	request.type = DQLITE_OPEN;
	request.open.name = "test.db";
	request.open.flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	request.open.vfs = "volatile";

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, 0);

	request.type = DQLITE_PREPARE;
	request.prepare.db_id = response->db.id;
	request.prepare.sql = "garbage";

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_PTR_NOT_NULL(response);

	CU_ASSERT_EQUAL(response->type, DQLITE_DB_ERROR);
	CU_ASSERT_EQUAL(response->db_error.code, SQLITE_ERROR);
	CU_ASSERT_EQUAL(response->db_error.extended_code, SQLITE_ERROR);
}

void test_dqlite__gateway_prepare_invalid_db_id()
{
	int err;

	request.type = DQLITE_PREPARE;
	request.prepare.db_id = 123;
	request.prepare.sql = "CREATE TABLE foo (n INT)";

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, DQLITE_NOTFOUND);

	CU_ASSERT_STRING_EQUAL(gateway.error, "failed to handle prepare: no db with id 123");
}

void test_dqlite__gateway_exec()
{
	int err;
	uint32_t db_id;
	uint32_t stmt_id;

	request.type = DQLITE_OPEN;
	request.open.name = "test.db";
	request.open.flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	request.open.vfs = "volatile";

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, 0);

	db_id = response->db.id;

	request.type = DQLITE_PREPARE;
	request.prepare.db_id = db_id;
	request.prepare.sql = "CREATE TABLE foo (n INT)";

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, 0);

	stmt_id = response->stmt.id;

	request.type = DQLITE_EXEC;
	request.exec.db_id = db_id;
	request.exec.stmt_id = stmt_id;

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_PTR_NOT_NULL(response);
	CU_ASSERT_EQUAL(response->type, DQLITE_RESULT);

	request.type = DQLITE_PREPARE;
	request.prepare.db_id = db_id;
	request.prepare.sql = "INSERT INTO foo(n) VALUES(1)";

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, 0);

	stmt_id = response->stmt.id;

	request.type = DQLITE_EXEC;
	request.exec.db_id = db_id;
	request.exec.stmt_id = stmt_id;

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_PTR_NOT_NULL(response);
	CU_ASSERT_EQUAL(response->type, DQLITE_RESULT);

	CU_ASSERT_EQUAL(response->result.last_insert_id, 1);
	CU_ASSERT_EQUAL(response->result.rows_affected, 1);
}
