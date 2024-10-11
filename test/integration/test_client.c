#include "../lib/client.h"
#include "../lib/heap.h"
#include "../lib/runner.h"
#include "../lib/server.h"
#include "../lib/sqlite.h"

/******************************************************************************
 *
 * Handle client requests
 *
 ******************************************************************************/

SUITE(client);

static char *bools[] = { "0", "1", NULL };

static MunitParameterEnum client_params[] = {
	{ "disk_mode", bools },
	{ NULL, NULL },
};

struct fixture {
	struct test_server server;
	struct client_proto *client;
	struct rows rows;
};

static void *setUp(const MunitParameter params[], void *user_data)
{
	struct fixture *f = munit_malloc(sizeof *f);
	(void)user_data;
	f->rows = (struct rows){};
	test_heap_setup(params, user_data);
	test_sqlite_setup(params);
	test_server_setup(&f->server, 1, params);
	test_server_start(&f->server, params);
	f->client = test_server_client(&f->server);
	HANDSHAKE;
	OPEN;
	return f;
}

static void tearDown(void *data)
{
	struct fixture *f = data;
	test_server_tear_down(&f->server);
	test_sqlite_tear_down();
	test_heap_tear_down(data);

	clientCloseRows(&f->rows);
	free(f);
}

TEST(client, exec, setUp, tearDown, 0, client_params)
{
	struct fixture *f = data;
	uint32_t stmt_id;
	uint64_t last_insert_id;
	uint64_t rows_affected;
	(void)params;
	PREPARE("CREATE TABLE test (n INT)", &stmt_id);
	EXEC(stmt_id, &last_insert_id, &rows_affected);
	return MUNIT_OK;
}

TEST(client, execWithOneParam, setUp, tearDown, 0, client_params)
{
	struct fixture *f = data;
	uint32_t stmt_id;
	uint64_t last_insert_id;
	uint64_t rows_affected;
	struct value param = { 0 };
	int rv;
	(void)params;
	PREPARE("CREATE TABLE test (n INT)", &stmt_id);
	EXEC(stmt_id, &last_insert_id, &rows_affected);
	PREPARE("INSERT INTO test (n) VALUES(?)", &stmt_id);
	param.type = SQLITE_INTEGER;
	param.integer = 17;
	rv = clientSendExec(f->client, stmt_id, &param, 1, NULL);
	munit_assert_int(rv, ==, 0);
	rv = clientRecvResult(f->client, &last_insert_id, &rows_affected, NULL);
	munit_assert_int(rv, ==, 0);
	return MUNIT_OK;
}

TEST(client, execSql, setUp, tearDown, 0, client_params)
{
	struct fixture *f = data;
	uint64_t last_insert_id;
	uint64_t rows_affected;
	(void)params;
	EXEC_SQL("CREATE TABLE test (n INT)", &last_insert_id, &rows_affected);
	return MUNIT_OK;
}

TEST(client, query, setUp, tearDown, 0, client_params)
{
	struct fixture *f = data;
	uint32_t stmt_id;
	uint64_t last_insert_id;
	uint64_t rows_affected;
	unsigned i;
	(void)params;

	PREPARE("CREATE TABLE test (n INT)", &stmt_id);
	EXEC(stmt_id, &last_insert_id, &rows_affected);

	PREPARE("BEGIN", &stmt_id);
	EXEC(stmt_id, &last_insert_id, &rows_affected);

	PREPARE("INSERT INTO test (n) VALUES(123)", &stmt_id);
	for (i = 0; i < 256; i++) {
		EXEC(stmt_id, &last_insert_id, &rows_affected);
	}

	PREPARE("COMMIT", &stmt_id);
	EXEC(stmt_id, &last_insert_id, &rows_affected);

	PREPARE("SELECT n FROM test", &stmt_id);
	QUERY_DONE(stmt_id, &f->rows, {});

	return MUNIT_OK;
}

TEST(client, querySql, setUp, tearDown, 0, client_params)
{
	struct fixture *f = data;
	uint32_t stmt_id;
	uint64_t last_insert_id;
	uint64_t rows_affected;
	unsigned i;
	(void)params;
	EXEC_SQL("CREATE TABLE test (n INT)", &last_insert_id, &rows_affected);

	EXEC_SQL("BEGIN", &last_insert_id, &rows_affected);

	PREPARE("INSERT INTO test (n) VALUES(123)", &stmt_id);
	for (i = 0; i < 256; i++) {
		EXEC(stmt_id, &last_insert_id, &rows_affected);
	}

	EXEC_SQL("COMMIT", &last_insert_id, &rows_affected);
	QUERY_SQL_DONE("SELECT n FROM test", &f->rows, {});

	return MUNIT_OK;
}

/* Stress test of an EXEC_SQL with many ';'-separated statements. */
TEST(client, semicolons, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	(void)params;

	static const char create_sql[] = "CREATE TABLE IF NOT EXISTS test (n INT);";
	static const char insert_sql[] = "INSERT INTO test (n) VALUES (17);";

	size_t n = 10000;
	size_t create_len = sizeof(create_sql) - 1;
	size_t insert_len = sizeof(insert_sql) - 1;
	size_t len = n * create_len + insert_len + 1;
	char *sql = munit_malloc(len);
	char *p = sql;
	for (size_t i = 0; i < n; i++) {
		memcpy(p, create_sql, create_len);
		p += create_len;
	}
	memcpy(p, insert_sql, insert_len);
	p += insert_len;
	munit_assert_ptr(p, ==, sql + len - 1);
	*p = '\0';

	uint64_t last_insert_id;
	uint64_t rows_affected;
	EXEC_SQL(sql, &last_insert_id, &rows_affected);
	free(sql);

	/* Check that all the statements were executed. */
	struct row *row;
	QUERY_SQL("SELECT n FROM test", &f->rows);
	munit_assert_uint(f->rows.column_count, ==, 1);
	munit_assert_string_equal(f->rows.column_names[0], "n");
	row = f->rows.next;
	munit_assert_int(row->values[0].type, ==, SQLITE_INTEGER);
	munit_assert_int64(row->values[0].integer, ==, 17);
	munit_assert_ptr_null(row->next);

	return MUNIT_OK;
}
