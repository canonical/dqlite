#include <CUnit/CUnit.h>

#include <sqlite3.h>

#include "../src/gateway.h"
#include "../src/response.h"
#include "../include/dqlite.h"

#include "cluster.h"
#include "replication.h"
#include "suite.h"

static sqlite3_vfs* vfs;
static sqlite3_wal_replication *replication;
static struct dqlite__gateway gateway;
static struct dqlite__request request;
struct dqlite__response *response;

/* Send a valid open request and return the database ID */
static void test_dqlite__gateway_send_open(uint32_t *db_id)
{
	int err;

	request.type = DQLITE_REQUEST_OPEN;
	request.open.name = "test.db";
	request.open.flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	request.open.vfs = replication->zName;

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_PTR_NOT_NULL(response);
	CU_ASSERT_EQUAL(response->type, DQLITE_RESPONSE_DB);

	*db_id = response->db.id;

	dqlite__gateway_finish(&gateway, response);
}

/* Send a valid prepare request and return the statement ID */
static void test_dqlite__gateway_send_prepare(uint32_t db_id, const char *sql, uint32_t *stmt_id)
{
	int err;

	request.type = DQLITE_REQUEST_PREPARE;
	request.prepare.db_id = db_id;
	request.prepare.sql = sql;

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_PTR_NOT_NULL(response);
	CU_ASSERT_EQUAL(response->type, DQLITE_RESPONSE_STMT);

	CU_ASSERT_EQUAL(response->stmt.db_id, db_id);

	*stmt_id = response->stmt.id;

	dqlite__gateway_finish(&gateway, response);
}

/* Send a valid exec request and return the result */
static void test_dqlite__gateway_send_exec(
	uint32_t db_id, uint32_t stmt_id,
	uint64_t *last_insert_id, uint64_t *rows_affected)
{
	int err;
	request.type = DQLITE_REQUEST_EXEC;
	request.exec.db_id = db_id;
	request.exec.stmt_id = stmt_id;

	request.message.words = 1;
	request.message.offset1 = 8;

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_PTR_NOT_NULL(response);
	CU_ASSERT_EQUAL(response->type, DQLITE_RESPONSE_RESULT);

	*last_insert_id = response->result.last_insert_id;
	*rows_affected = response->result.rows_affected;

	dqlite__gateway_finish(&gateway, response);
}

void test_dqlite__gateway_setup()
{
	FILE *log = test_suite_dqlite_log();
	int err;

	replication = test_replication();

	err = sqlite3_wal_replication_register(replication, 0);
	if (err != 0) {
		test_suite_errorf("failed to register wal: %s - %d", sqlite3_errstr(err), err);
		CU_FAIL("test setup failed");
	}

	err = dqlite_vfs_register(replication->zName, &vfs);
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
	sqlite3_wal_replication_unregister(replication);
	dqlite_vfs_unregister(vfs);
}

void test_dqlite__gateway_leader()
{
	int err;

	request.type = DQLITE_REQUEST_LEADER;

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_PTR_NOT_NULL(response);
	CU_ASSERT_EQUAL(response->type, DQLITE_RESPONSE_SERVER);

	CU_ASSERT_STRING_EQUAL(response->server.address,  "127.0.0.1:666");
}

void test_dqlite__gateway_client()
{
	int err;

	request.type = DQLITE_REQUEST_CLIENT;
	request.client.id = 123;

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_PTR_NOT_NULL(response);
	CU_ASSERT_EQUAL(response->type, DQLITE_RESPONSE_WELCOME);

	CU_ASSERT_EQUAL(response->welcome.heartbeat_timeout, 15000);
}

void test_dqlite__gateway_heartbeat()
{
	int err;

	request.type = DQLITE_REQUEST_HEARTBEAT;
	request.heartbeat.timestamp = 12345;

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_PTR_NOT_NULL(response);
	CU_ASSERT_EQUAL(response->type, DQLITE_RESPONSE_SERVERS);

	CU_ASSERT_STRING_EQUAL(response->servers.addresses[0], "1.2.3.4:666");
	CU_ASSERT_STRING_EQUAL(response->servers.addresses[1], "5.6.7.8:666");
	CU_ASSERT_PTR_NULL(response->servers.addresses[2]);
}

void test_dqlite__gateway_open()
{
	uint32_t db_id;

	test_dqlite__gateway_send_open(&db_id);

	CU_ASSERT_EQUAL(response->db.id, 0);
}

void test_dqlite__gateway_open_error()
{
	int err;

	request.type = DQLITE_REQUEST_OPEN;
	request.open.name = "test.db";
	request.open.flags = SQLITE_OPEN_CREATE;
	request.open.vfs = replication->zName;

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_PTR_NOT_NULL(response);

	CU_ASSERT_EQUAL(response->type, DQLITE_RESPONSE_DB_ERROR);
	CU_ASSERT_EQUAL(response->db_error.code, SQLITE_MISUSE);
	CU_ASSERT_EQUAL(response->db_error.extended_code, SQLITE_MISUSE);
	CU_ASSERT_STRING_EQUAL(response->db_error.description, "bad parameter or other API misuse");
}

void test_dqlite__gateway_prepare()
{
	uint32_t db_id;
	uint32_t stmt_id;

	test_dqlite__gateway_send_open(&db_id);

	test_dqlite__gateway_send_prepare(db_id, "CREATE TABLE foo (n INT)", &stmt_id);

	CU_ASSERT_EQUAL(stmt_id, 0);
}

void test_dqlite__gateway_prepare_error()
{
	int err;
	uint32_t db_id;

	test_dqlite__gateway_send_open(&db_id);

	request.type = DQLITE_REQUEST_PREPARE;
	request.prepare.db_id = response->db.id;
	request.prepare.sql = "garbage";

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_PTR_NOT_NULL(response);

	CU_ASSERT_EQUAL(response->type, DQLITE_RESPONSE_DB_ERROR);
	CU_ASSERT_EQUAL(response->db_error.code, SQLITE_ERROR);
	CU_ASSERT_EQUAL(response->db_error.extended_code, SQLITE_ERROR);
}

void test_dqlite__gateway_prepare_invalid_db_id()
{
	int err;

	request.type = DQLITE_REQUEST_PREPARE;
	request.prepare.db_id = 123;
	request.prepare.sql = "CREATE TABLE foo (n INT)";

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, DQLITE_NOTFOUND);

	CU_ASSERT_STRING_EQUAL(gateway.error, "failed to handle prepare: no db with id 123");
}

void test_dqlite__gateway_exec()
{
	uint32_t db_id;
	uint32_t stmt_id;
	uint64_t last_insert_id;
	uint64_t rows_affected;

	test_dqlite__gateway_send_open(&db_id);

	test_dqlite__gateway_send_prepare(db_id, "CREATE TABLE foo (n INT)", &stmt_id);

	test_dqlite__gateway_send_exec(db_id, stmt_id, &last_insert_id, &rows_affected);

	test_dqlite__gateway_send_prepare(db_id, "INSERT INTO foo(n) VALUES(1)", &stmt_id);

	test_dqlite__gateway_send_exec(db_id, stmt_id, &last_insert_id, &rows_affected);

	CU_ASSERT_EQUAL(last_insert_id, 1);
	CU_ASSERT_EQUAL(rows_affected, 1);
}

void test_dqlite__gateway_exec_with_params()
{
	int err;
	uint32_t db_id;
	uint32_t stmt_id;
	uint64_t last_insert_id;
	uint64_t rows_affected;

	test_dqlite__gateway_send_open(&db_id);

	test_dqlite__gateway_send_prepare(db_id, "CREATE TABLE foo (n INT, t TEXT, f FLOAT)", &stmt_id);

	test_dqlite__gateway_send_exec(db_id, stmt_id, &last_insert_id, &rows_affected);

	test_dqlite__gateway_send_prepare(db_id, "INSERT INTO foo(n,t,f) VALUES(?,?,?)", &stmt_id);

	request.type = DQLITE_REQUEST_EXEC;
	request.exec.db_id = db_id;
	request.exec.stmt_id = stmt_id;

	request.message.words = 5;
	request.message.offset1 = 8;

	err = dqlite__message_body_put_uint8(&request.message, 3); /* N of params */
	CU_ASSERT_EQUAL(err, 0);

	err = dqlite__message_body_put_uint8(&request.message, SQLITE_INTEGER); /* param type */
	CU_ASSERT_EQUAL(err, 0);

	err = dqlite__message_body_put_uint8(&request.message, SQLITE_TEXT); /* param type */
	CU_ASSERT_EQUAL(err, 0);

	err = dqlite__message_body_put_uint8(&request.message, SQLITE_NULL); /* param type */
	CU_ASSERT_EQUAL(err, 0);

	request.message.offset1 = 16; /* skip padding bytes */

	err = dqlite__message_body_put_int64(&request.message, 1); /* param value */
	CU_ASSERT_EQUAL(err, 0);

	err = dqlite__message_body_put_text(&request.message, "hello"); /* param value */
	CU_ASSERT_EQUAL(err, 0);

	err = dqlite__message_body_put_int64(&request.message, 0); /* param value */
	CU_ASSERT_EQUAL(err, 0);

	request.message.offset1 = 8; /* rewind */

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_PTR_NOT_NULL(response);
	CU_ASSERT_EQUAL(response->type, DQLITE_RESPONSE_RESULT);

	CU_ASSERT_EQUAL(response->result.last_insert_id, 1);
	CU_ASSERT_EQUAL(response->result.rows_affected, 1);
}

void test_dqlite__gateway_exec_invalid_stmt_id()
{
	int err;
	uint32_t db_id;

	test_dqlite__gateway_send_open(&db_id);

	request.type = DQLITE_REQUEST_EXEC;
	request.exec.db_id = db_id;
	request.exec.stmt_id = 666;

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, DQLITE_NOTFOUND);

	CU_ASSERT_STRING_EQUAL(gateway.error, "failed to handle exec: no stmt with id 666");
}

void test_dqlite__gateway_query()
{
	int err;
	uint32_t db_id;
	uint32_t stmt_id;
	uint64_t last_insert_id;
	uint64_t rows_affected;
	uint64_t column_count;
	const char* column_name;
	uint64_t header;
	int64_t n;

	test_dqlite__gateway_send_open(&db_id);

	test_dqlite__gateway_send_prepare(db_id, "CREATE TABLE foo (n INT)", &stmt_id);

	test_dqlite__gateway_send_exec(db_id, stmt_id, &last_insert_id, &rows_affected);

	test_dqlite__gateway_send_prepare(db_id, "INSERT INTO foo(n) VALUES(-12)", &stmt_id);

	test_dqlite__gateway_send_exec(db_id, stmt_id, &last_insert_id, &rows_affected);

	test_dqlite__gateway_send_prepare(db_id, "SELECT n FROM foo", &stmt_id);

	request.type = DQLITE_REQUEST_QUERY;
	request.query.db_id = db_id;
	request.query.stmt_id = stmt_id;

	request.message.words = 1;
	request.message.offset1 = 8;

	response->message.offset1 = 0;

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_PTR_NOT_NULL(response);
	CU_ASSERT_EQUAL(response->type, DQLITE_RESPONSE_ROWS);

	/* Four words were written, one with the column count, one with the
	 * column name, one with the row header and one with the row column */
	CU_ASSERT_EQUAL(response->message.offset1, 32);

	response->message.words = 4;
	response->message.offset1 = 0;

	/* Read the column count */
	err = dqlite__message_body_get_uint64(&response->message, &column_count);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(column_count, 1);

	/* Read the column name */
	err = dqlite__message_body_get_text(&response->message, &column_name);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_STRING_EQUAL(column_name, "n");

	/* Read the header */
	err = dqlite__message_body_get_uint64(&response->message, &header);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL((uint8_t*)(&header)[0], SQLITE_INTEGER);

	/* Read the value */
	err = dqlite__message_body_get_int64(&response->message, &n);
	CU_ASSERT_EQUAL(err, DQLITE_EOM);

	CU_ASSERT_EQUAL(n, -12);
}

void test_dqlite__gateway_query_multi_column()
{
	int err;
	uint32_t db_id;
	uint32_t stmt_id;
	uint64_t last_insert_id;
	uint64_t rows_affected;
	uint64_t column_count;
	const char *column_name;
	uint64_t header;
	int64_t n;
	text_t t;
	uint64_t null;

	test_dqlite__gateway_send_open(&db_id);

	test_dqlite__gateway_send_prepare(db_id, "CREATE TABLE foo (n INT, t TEXT, f FLOAT)", &stmt_id);

	test_dqlite__gateway_send_exec(db_id, stmt_id, &last_insert_id, &rows_affected);

	test_dqlite__gateway_send_prepare(db_id, "INSERT INTO foo(n,t,f) VALUES(8,'hello',NULL)", &stmt_id);

	test_dqlite__gateway_send_exec(db_id, stmt_id, &last_insert_id, &rows_affected);

	test_dqlite__gateway_send_prepare(db_id, "SELECT n,t,f FROM foo", &stmt_id);

	request.type = DQLITE_REQUEST_QUERY;
	request.query.db_id = db_id;
	request.query.stmt_id = stmt_id;

	request.message.words = 1;
	request.message.offset1 = 8;

	response->message.offset1 = 0;

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_PTR_NOT_NULL(response);
	CU_ASSERT_EQUAL(response->type, DQLITE_RESPONSE_ROWS);

	/* Eight words were written, one for the column count, three with the
	 * column names, one for the row header and three for the row columns */
	CU_ASSERT_EQUAL(response->message.offset1, 64);

	response->message.words = 8;
	response->message.offset1 = 0;

	/* Read the column count */
	err = dqlite__message_body_get_uint64(&response->message, &column_count);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(column_count, 3);

	/* Read the column names */
	err = dqlite__message_body_get_text(&response->message, &column_name);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_STRING_EQUAL(column_name, "n");

	err = dqlite__message_body_get_text(&response->message, &column_name);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_STRING_EQUAL(column_name, "t");

	err = dqlite__message_body_get_text(&response->message, &column_name);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_STRING_EQUAL(column_name, "f");

	/* Read the header */
	err = dqlite__message_body_get_uint64(&response->message, &header);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL((*(uint8_t*)(&header)) & 0x0f, SQLITE_INTEGER);
	CU_ASSERT_EQUAL((*(uint8_t*)(&header)) >> 4, SQLITE_TEXT);
	CU_ASSERT_EQUAL((*((uint8_t*)(&header) + 1)) & 0x0f, SQLITE_NULL);

	/* Read column n */
	err = dqlite__message_body_get_int64(&response->message, &n);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(n, 8);

	/* Read column t */
	err = dqlite__message_body_get_text(&response->message, &t);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_STRING_EQUAL(t, "hello");

	/* Read column f */
	err = dqlite__message_body_get_uint64(&response->message, &null);
	CU_ASSERT_EQUAL(err, DQLITE_EOM);

	CU_ASSERT_EQUAL(null, 0);
}

void test_dqlite__gateway_query_multi_row()
{
	int err;
	uint32_t db_id;
	uint32_t stmt_id;
	uint64_t last_insert_id;
	uint64_t rows_affected;
	uint64_t column_count;
	const char *column_name;
	uint64_t header;
	int64_t n;
	text_t t;
	uint64_t null;
	double f;

	test_dqlite__gateway_send_open(&db_id);

	test_dqlite__gateway_send_prepare(db_id, "CREATE TABLE foo (n INT, t TEXT, f FLOAT)", &stmt_id);

	test_dqlite__gateway_send_exec(db_id, stmt_id, &last_insert_id, &rows_affected);

	test_dqlite__gateway_send_prepare(db_id, "INSERT INTO foo(n,t,f) VALUES(8,'hello',NULL)", &stmt_id);

	test_dqlite__gateway_send_exec(db_id, stmt_id, &last_insert_id, &rows_affected);

	test_dqlite__gateway_send_prepare(db_id, "INSERT INTO foo(n,t,f) VALUES(-1,'world',3.1415)", &stmt_id);

	test_dqlite__gateway_send_exec(db_id, stmt_id, &last_insert_id, &rows_affected);

	test_dqlite__gateway_send_prepare(db_id, "SELECT n,t,f FROM foo", &stmt_id);

	request.type = DQLITE_REQUEST_QUERY;
	request.query.db_id = db_id;
	request.query.stmt_id = stmt_id;

	request.message.words = 1;
	request.message.offset1 = 8;

	response->message.offset1 = 0;

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_PTR_NOT_NULL(response);
	CU_ASSERT_EQUAL(response->type, DQLITE_RESPONSE_ROWS);

	/* Twelve words were written (one column count, three column names, two
	 * header rows and size row columns). */
	CU_ASSERT_EQUAL(response->message.offset1, 96);

	response->message.words = 12;
	response->message.offset1 = 0;

	/* Read the column count */
	err = dqlite__message_body_get_uint64(&response->message, &column_count);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(column_count, 3);

	/* Read the column names */
	err = dqlite__message_body_get_text(&response->message, &column_name);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_STRING_EQUAL(column_name, "n");

	err = dqlite__message_body_get_text(&response->message, &column_name);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_STRING_EQUAL(column_name, "t");

	err = dqlite__message_body_get_text(&response->message, &column_name);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_STRING_EQUAL(column_name, "f");

	/* Read the header (first row) */
	err = dqlite__message_body_get_uint64(&response->message, &header);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL((*(uint8_t*)(&header)) & 0x0f, SQLITE_INTEGER);
	CU_ASSERT_EQUAL((*(uint8_t*)(&header)) >> 4, SQLITE_TEXT);
	CU_ASSERT_EQUAL((*((uint8_t*)(&header) + 1)) & 0x0f, SQLITE_NULL);

	/* Read column n */
	err = dqlite__message_body_get_int64(&response->message, &n);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(n, 8);

	/* Read column t */
	err = dqlite__message_body_get_text(&response->message, &t);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_STRING_EQUAL(t, "hello");

	/* Read column f */
	err = dqlite__message_body_get_uint64(&response->message, &null);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(null, 0);

	/* Read the header (second row) */
	err = dqlite__message_body_get_uint64(&response->message, &header);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL((*(uint8_t*)(&header)) & 0x0f, SQLITE_INTEGER);
	CU_ASSERT_EQUAL((*(uint8_t*)(&header)) >> 4, SQLITE_TEXT);
	CU_ASSERT_EQUAL((*((uint8_t*)(&header) + 1)) & 0x0f, SQLITE_FLOAT);

	/* Read column n */
	err = dqlite__message_body_get_int64(&response->message, &n);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(n, -1);

	/* Read column t */
	err = dqlite__message_body_get_text(&response->message, &t);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_STRING_EQUAL(t, "world");

	/* Read column f */
	err = dqlite__message_body_get_double(&response->message, &f);
	CU_ASSERT_EQUAL(err, DQLITE_EOM);

	CU_ASSERT_EQUAL(f, 3.1415);
}

void test_dqlite__gateway_finalize()
{
	int err;
	uint32_t db_id;
	uint32_t stmt_id;

	request.type = DQLITE_REQUEST_OPEN;
	request.open.name = "test.db";
	request.open.flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	request.open.vfs = replication->zName;

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, 0);

	db_id = response->db.id;

	request.type = DQLITE_REQUEST_PREPARE;
	request.prepare.db_id = db_id;
	request.prepare.sql = "CREATE TABLE foo (n INT)";

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, 0);

	stmt_id = response->stmt.id;

	request.type = DQLITE_REQUEST_FINALIZE;
	request.finalize.db_id = db_id;
	request.finalize.stmt_id = stmt_id;

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_PTR_NOT_NULL(response);
	CU_ASSERT_EQUAL(response->type, DQLITE_RESPONSE_EMPTY);
}

void test_dqlite__gateway_exec_sql()
{
	int err;
	uint32_t db_id;
	uint32_t stmt_id;
	uint64_t last_insert_id;
	uint64_t rows_affected;

	test_dqlite__gateway_send_open(&db_id);

	test_dqlite__gateway_send_prepare(db_id, "CREATE TABLE foo (n INT, t TEXT, f FLOAT)", &stmt_id);

	test_dqlite__gateway_send_exec(db_id, stmt_id, &last_insert_id, &rows_affected);

	request.type = DQLITE_REQUEST_EXEC_SQL;
	request.exec_sql.db_id = db_id;
	request.exec_sql.sql =  "INSERT INTO foo(n,t,f) VALUES(?,?,?)";

	request.message.words = 5;
	request.message.offset1 = 8;

	err = dqlite__message_body_put_uint8(&request.message, 3); /* N of params */
	CU_ASSERT_EQUAL(err, 0);

	err = dqlite__message_body_put_uint8(&request.message, SQLITE_INTEGER); /* param type */
	CU_ASSERT_EQUAL(err, 0);

	err = dqlite__message_body_put_uint8(&request.message, SQLITE_TEXT); /* param type */
	CU_ASSERT_EQUAL(err, 0);

	err = dqlite__message_body_put_uint8(&request.message, SQLITE_NULL); /* param type */
	CU_ASSERT_EQUAL(err, 0);

	request.message.offset1 = 16; /* skip padding bytes */

	err = dqlite__message_body_put_int64(&request.message, 1); /* param value */
	CU_ASSERT_EQUAL(err, 0);

	err = dqlite__message_body_put_text(&request.message, "hello"); /* param value */
	CU_ASSERT_EQUAL(err, 0);

	err = dqlite__message_body_put_int64(&request.message, 0); /* param value */
	CU_ASSERT_EQUAL(err, 0);

	request.message.offset1 = 8; /* rewind */

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_PTR_NOT_NULL(response);
	CU_ASSERT_EQUAL(response->type, DQLITE_RESPONSE_RESULT);

	CU_ASSERT_EQUAL(response->result.last_insert_id, 1);
	CU_ASSERT_EQUAL(response->result.rows_affected, 1);
}

void test_dqlite__gateway_query_sql()
{
	int err;
	uint32_t db_id;
	uint32_t stmt_id;
	uint64_t last_insert_id;
	uint64_t rows_affected;
	uint64_t column_count;
	const char *column_name;
	uint64_t header;
	int64_t n;

	test_dqlite__gateway_send_open(&db_id);

	test_dqlite__gateway_send_prepare(db_id, "CREATE TABLE foo (n INT)", &stmt_id);

	test_dqlite__gateway_send_exec(db_id, stmt_id, &last_insert_id, &rows_affected);

	test_dqlite__gateway_send_prepare(db_id, "INSERT INTO foo(n) VALUES(-12)", &stmt_id);

	test_dqlite__gateway_send_exec(db_id, stmt_id, &last_insert_id, &rows_affected);

	request.type = DQLITE_REQUEST_QUERY_SQL;
	request.query_sql.db_id = db_id;
	request.query_sql.sql = "SELECT n FROM foo";

	request.message.words = 1;
	request.message.offset1 = 8;

	response->message.offset1 = 0;

	err = dqlite__gateway_handle(&gateway, &request, &response);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_PTR_NOT_NULL(response);
	CU_ASSERT_EQUAL(response->type, DQLITE_RESPONSE_ROWS);

	/* Four words were written, one with the column count, one with the
	 * column name, one with the row header and one with the row column */
	CU_ASSERT_EQUAL(response->message.offset1, 32);

	response->message.words = 4;
	response->message.offset1 = 0;

	/* Read the column count */
	err = dqlite__message_body_get_uint64(&response->message, &column_count);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(column_count, 1);

	/* Read the column name */
	err = dqlite__message_body_get_text(&response->message, &column_name);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_STRING_EQUAL(column_name, "n");

	/* Read the header */
	err = dqlite__message_body_get_uint64(&response->message, &header);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL((uint8_t*)(&header)[0], SQLITE_INTEGER);

	/* Read the value */
	err = dqlite__message_body_get_int64(&response->message, &n);
	CU_ASSERT_EQUAL(err, DQLITE_EOM);

	CU_ASSERT_EQUAL(n, -12);
}
