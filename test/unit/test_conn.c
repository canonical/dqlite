#include <raft.h>
#include <raft/uv.h>

#include "../../include/dqlite.h"

#include "../lib/client.h"
#include "../lib/config.h"
#include "../lib/heap.h"
#include "../lib/logger.h"
#include "../lib/raft.h"
#include "../lib/registry.h"
#include "../lib/runner.h"
#include "../lib/sqlite.h"
#include "../lib/vfs.h"

#include "../../src/client.h"
#include "../../src/conn.h"
#include "../../src/gateway.h"
#include "../../src/lib/transport.h"
#include "../../src/transport.h"

TEST_MODULE(conn);

/******************************************************************************
 *
 * Fixture
 *
 ******************************************************************************/

static void connCloseCb(struct conn *conn)
{
	bool *closed = conn->queue[0];
	*closed = true;
}

#define FIXTURE              \
	FIXTURE_LOGGER;      \
	FIXTURE_VFS;         \
	FIXTURE_CONFIG;      \
	FIXTURE_REGISTRY;    \
	FIXTURE_RAFT;        \
	FIXTURE_CLIENT;      \
	struct conn conn;    \
	bool closed;

#define SETUP                                                                \
	struct uv_stream_s *stream;                                          \
	int rv;                                                              \
	SETUP_HEAP;                                                          \
	SETUP_SQLITE;                                                        \
	SETUP_LOGGER;                                                        \
	SETUP_VFS;                                                           \
	SETUP_CONFIG;                                                        \
	SETUP_REGISTRY;                                                      \
	SETUP_RAFT;                                                          \
	SETUP_CLIENT;                                                        \
	RAFT_BOOTSTRAP;                                                      \
	RAFT_START;                                                          \
	rv = transport__stream(&f->loop, f->server, &stream);                \
	munit_assert_int(rv, ==, 0);                                         \
	f->closed = false;                                                   \
	f->conn.queue[0] = &f->closed;                                       \
	rv = conn__start(&f->conn, &f->config, &f->loop, &f->registry,       \
			 &f->raft, stream, &f->raft_transport, connCloseCb); \
	munit_assert_int(rv, ==, 0)

#define TEAR_DOWN                         \
	conn__stop(&f->conn);             \
	while (!f->closed) {              \
		test_uv_run(&f->loop, 1); \
	};                                \
	TEAR_DOWN_RAFT;                   \
	TEAR_DOWN_CLIENT;                 \
	TEAR_DOWN_REGISTRY;               \
	TEAR_DOWN_CONFIG;                 \
	TEAR_DOWN_VFS;                    \
	TEAR_DOWN_LOGGER;                 \
	TEAR_DOWN_SQLITE;                 \
	TEAR_DOWN_HEAP

/******************************************************************************
 *
 * Helper macros.
 *
 ******************************************************************************/

/* Send the initial client handshake. */
#define HANDSHAKE_CONN                                 \
	{                                              \
		int rv2;                               \
		rv2 = clientSendHandshake(&f->client); \
		munit_assert_int(rv2, ==, 0);          \
		test_uv_run(&f->loop, 1);              \
	}

/* Open a test database. */
#define OPEN_CONN                                         \
	{                                                 \
		int rv2;                                  \
		rv2 = clientSendOpen(&f->client, "test"); \
		munit_assert_int(rv2, ==, 0);             \
		test_uv_run(&f->loop, 2);                 \
		rv2 = clientRecvDb(&f->client);           \
		munit_assert_int(rv2, ==, 0);             \
	}

/* Prepare a statement. */
#define PREPARE_CONN(SQL, STMT_ID)                         \
	{                                                  \
		int rv2;                                   \
		rv2 = clientSendPrepare(&f->client, SQL);  \
		munit_assert_int(rv2, ==, 0);              \
		test_uv_run(&f->loop, 1);                  \
		rv2 = clientRecvStmt(&f->client, STMT_ID); \
		munit_assert_int(rv2, ==, 0);              \
	}

/* Execute a statement. */
#define EXEC_CONN(STMT_ID, LAST_INSERT_ID, ROWS_AFFECTED, LOOP)    \
	{                                                          \
		int rv2;                                           \
		rv2 = clientSendExec(&f->client, STMT_ID);         \
		munit_assert_int(rv2, ==, 0);                      \
		test_uv_run(&f->loop, LOOP);                       \
		rv2 = clientRecvResult(&f->client, LAST_INSERT_ID, \
				       ROWS_AFFECTED);             \
		munit_assert_int(rv2, ==, 0);                      \
	}

/* Execute a non-prepared statement. */
#define EXEC_SQL_CONN(SQL, LAST_INSERT_ID, ROWS_AFFECTED, LOOP)    \
	{                                                          \
		int rv2;                                           \
		rv2 = clientSendExecSQL(&f->client, SQL);          \
		munit_assert_int(rv2, ==, 0);                      \
		test_uv_run(&f->loop, LOOP);                       \
		rv2 = clientRecvResult(&f->client, LAST_INSERT_ID, \
				       ROWS_AFFECTED);             \
		munit_assert_int(rv2, ==, 0);                      \
	}

/* Perform a query. */
#define QUERY_CONN(STMT_ID, ROWS)                           \
	{                                                   \
		int rv2;                                    \
		rv2 = clientSendQuery(&f->client, STMT_ID); \
		munit_assert_int(rv2, ==, 0);               \
		test_uv_run(&f->loop, 2);                   \
		rv2 = clientRecvRows(&f->client, ROWS);     \
		munit_assert_int(rv2, ==, 0);               \
	}

/* Perform a non-prepared query. */
#define QUERY_SQL_CONN(SQL, ROWS)                              \
	{                                                      \
		int rv2;                                       \
		rv2 = clientSendQuerySql(&f->client, SQL);     \
		munit_assert_int(rv2, ==, 0);                  \
		test_uv_run(&f->loop, 2);                      \
		rv2 = clientRecvRows(&f->client, ROWS);        \
		munit_assert_int(rv2, ==, 0);                  \
	}

/******************************************************************************
 *
 * Handle the handshake
 *
 ******************************************************************************/

TEST_SUITE(handshake);

struct handshake_fixture
{
	FIXTURE;
};

TEST_SETUP(handshake)
{
	struct handshake_fixture *f = munit_malloc(sizeof *f);
	SETUP;
	return f;
}

TEST_TEAR_DOWN(handshake)
{
	struct handshake_fixture *f = data;
	TEAR_DOWN;
	free(f);
}

TEST_CASE(handshake, success, NULL)
{
	struct handshake_fixture *f = data;
	(void)params;
	HANDSHAKE_CONN;
	return MUNIT_OK;
}

/******************************************************************************
 *
 * Handle an open request
 *
 ******************************************************************************/

TEST_SUITE(open);

struct open_fixture
{
	FIXTURE;
};

TEST_SETUP(open)
{
	struct open_fixture *f = munit_malloc(sizeof *f);
	SETUP;
	HANDSHAKE_CONN;
	return f;
}

TEST_TEAR_DOWN(open)
{
	struct open_fixture *f = data;
	TEAR_DOWN;
	free(f);
}

TEST_CASE(open, success, NULL)
{
	struct open_fixture *f = data;
	(void)params;
	OPEN_CONN;
	return MUNIT_OK;
}

/******************************************************************************
 *
 * Handle an prepare request
 *
 ******************************************************************************/

TEST_SUITE(prepare);

struct prepare_fixture
{
	FIXTURE;
};

TEST_SETUP(prepare)
{
	struct prepare_fixture *f = munit_malloc(sizeof *f);
	SETUP;
	HANDSHAKE_CONN;
	OPEN_CONN;
	return f;
}

TEST_TEAR_DOWN(prepare)
{
	struct prepare_fixture *f = data;
	TEAR_DOWN;
	free(f);
}

TEST_CASE(prepare, success, NULL)
{
	struct prepare_fixture *f = data;
	unsigned stmt_id;
	(void)params;
	PREPARE_CONN("CREATE TABLE test (n INT)", &stmt_id);
	munit_assert_int(stmt_id, ==, 0);
	return MUNIT_OK;
}

/******************************************************************************
 *
 * Handle an exec
 *
 ******************************************************************************/

TEST_SUITE(exec);

struct exec_fixture
{
	FIXTURE;
	unsigned stmt_id;
};

TEST_SETUP(exec)
{
	struct exec_fixture *f = munit_malloc(sizeof *f);
	SETUP;
	HANDSHAKE_CONN;
	OPEN_CONN;
	return f;
}

TEST_TEAR_DOWN(exec)
{
	struct exec_fixture *f = data;
	TEAR_DOWN;
	free(f);
}

TEST_CASE(exec, success, NULL)
{
	struct exec_fixture *f = data;
	unsigned last_insert_id;
	unsigned rows_affected;
	(void)params;
	PREPARE_CONN("CREATE TABLE test (n INT)", &f->stmt_id);
	EXEC_CONN(f->stmt_id, &last_insert_id, &rows_affected, 8);
	munit_assert_int(last_insert_id, ==, 0);
	munit_assert_int(rows_affected, ==, 0);
	return MUNIT_OK;
}

TEST_CASE(exec, result, NULL)
{
	struct exec_fixture *f = data;
	unsigned last_insert_id;
	unsigned rows_affected;
	(void)params;
	PREPARE_CONN("BEGIN", &f->stmt_id);
	EXEC_CONN(f->stmt_id, &last_insert_id, &rows_affected, 3);
	PREPARE_CONN("CREATE TABLE test (n INT)", &f->stmt_id);
	EXEC_CONN(f->stmt_id, &last_insert_id, &rows_affected, 6);
	PREPARE_CONN("INSERT INTO test (n) VALUES(123)", &f->stmt_id);
	EXEC_CONN(f->stmt_id, &last_insert_id, &rows_affected, 3);
	PREPARE_CONN("COMMIT", &f->stmt_id);
	EXEC_CONN(f->stmt_id, &last_insert_id, &rows_affected, 6);
	munit_assert_int(last_insert_id, ==, 1);
	munit_assert_int(rows_affected, ==, 1);
	return MUNIT_OK;
}

TEST_CASE(exec, close_while_in_flight, NULL)
{
	struct exec_fixture *f = data;
	unsigned last_insert_id;
	unsigned rows_affected;
	int rv;
	(void)params;

	EXEC_SQL_CONN("CREATE TABLE test (n)", &last_insert_id, &rows_affected, 7);
	rv = clientSendExecSQL(&f->client, "INSERT INTO test(n) VALUES(1)");
	munit_assert_int(rv, ==, 0);

	test_uv_run(&f->loop, 1);

	return MUNIT_OK;
}

/******************************************************************************
 *
 * Handle a query
 *
 ******************************************************************************/

TEST_SUITE(query);

struct query_fixture
{
	FIXTURE;
	unsigned stmt_id;
	unsigned insert_stmt_id;
	unsigned last_insert_id;
	unsigned rows_affected;
	struct rows rows;
};

TEST_SETUP(query)
{
	struct query_fixture *f = munit_malloc(sizeof *f);
	unsigned stmt_id;
	SETUP;
	HANDSHAKE_CONN;
	OPEN_CONN;
	PREPARE_CONN("CREATE TABLE test (n INT)", &stmt_id);
	EXEC_CONN(stmt_id, &f->last_insert_id, &f->rows_affected, 7);
	PREPARE_CONN("INSERT INTO test(n) VALUES (123)", &f->insert_stmt_id);
	EXEC_CONN(f->insert_stmt_id, &f->last_insert_id, &f->rows_affected, 4);
	return f;
}

TEST_TEAR_DOWN(query)
{
	struct query_fixture *f = data;
	clientCloseRows(&f->rows);
	TEAR_DOWN;
	free(f);
}

/* Perform a query yielding one row. */
TEST_CASE(query, one, NULL)
{
	struct query_fixture *f = data;
	struct row *row;
	(void)params;
	PREPARE_CONN("SELECT n FROM test", &f->stmt_id);
	QUERY_CONN(f->stmt_id, &f->rows);
	munit_assert_int(f->rows.column_count, ==, 1);
	munit_assert_string_equal(f->rows.column_names[0], "n");
	row = f->rows.next;
	munit_assert_ptr_not_null(row);
	munit_assert_ptr_null(row->next);
	munit_assert_int(row->values[0].type, ==, SQLITE_INTEGER);
	munit_assert_int(row->values[0].integer, ==, 123);
	return MUNIT_OK;
}
