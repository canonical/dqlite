#include <raft.h>
#include <raft/io_uv.h>

#include "../lib/heap.h"
#include "../lib/logger.h"
#include "../lib/config.h"
#include "../lib/raft.h"
#include "../lib/registry.h"
#include "../lib/replication.h"
#include "../lib/runner.h"
#include "../lib/socket.h"
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

#define FIXTURE                          \
	FIXTURE_LOGGER;                  \
	FIXTURE_VFS;                     \
	FIXTURE_OPTIONS;                 \
	FIXTURE_REGISTRY;                \
	FIXTURE_RAFT;                    \
	FIXTURE_REPLICATION;             \
	struct test_socket_pair sockets; \
	struct conn conn;                \
	struct client client

#define SETUP                                                         \
	int rv;                                                       \
	SETUP_HEAP;                                                   \
	SETUP_SQLITE;                                                 \
	SETUP_LOGGER;                                                 \
	SETUP_VFS;                                                    \
	SETUP_OPTIONS;                                                \
	SETUP_REGISTRY;                                               \
	SETUP_RAFT;                                                   \
	SETUP_REPLICATION;                                            \
	RAFT_BOOTSTRAP;                                               \
	RAFT_START;                                                   \
	test_socket_pair_setup(params, &f->sockets);                  \
	rv = conn__start(&f->conn, &f->logger, &f->loop, &f->options, \
			 &f->registry, &f->raft, f->sockets.server,   \
			 &f->raft_transport, NULL);                   \
	munit_assert_int(rv, ==, 0);                                  \
	client__init(&f->client, f->sockets.client)

#define TEAR_DOWN                                \
	client__close(&f->client);               \
	conn__stop(&f->conn);                    \
	f->sockets.client_disconnected = true;   \
	f->sockets.server_disconnected = true;   \
	test_socket_pair_tear_down(&f->sockets); \
	TEAR_DOWN_REPLICATION;                   \
	TEAR_DOWN_RAFT;                          \
	TEAR_DOWN_REGISTRY;                      \
	TEAR_DOWN_OPTIONS;                       \
	TEAR_DOWN_VFS;                           \
	TEAR_DOWN_LOGGER;                        \
	TEAR_DOWN_SQLITE;                        \
	TEAR_DOWN_HEAP

/******************************************************************************
 *
 * Helper macros.
 *
 ******************************************************************************/

/* Send the initial client handshake. */
#define HANDSHAKE                                         \
	{                                                 \
		int rv2;                                  \
		rv2 = client__send_handshake(&f->client); \
		munit_assert_int(rv2, ==, 0);             \
		test_uv_run(&f->loop, 1);                 \
	}

/* Open a test database. */
#define OPEN                                                 \
	{                                                    \
		int rv2;                                     \
		rv2 = client__send_open(&f->client, "test"); \
		munit_assert_int(rv2, ==, 0);                \
		test_uv_run(&f->loop, 2);                    \
		rv2 = client__recv_db(&f->client);           \
		munit_assert_int(rv2, ==, 0);                \
	}

/* Prepare a statement. */
#define PREPARE(SQL, STMT_ID)                                 \
	{                                                     \
		int rv2;                                      \
		rv2 = client__send_prepare(&f->client, SQL);  \
		munit_assert_int(rv2, ==, 0);                 \
		test_uv_run(&f->loop, 2);                     \
		rv2 = client__recv_stmt(&f->client, STMT_ID); \
		munit_assert_int(rv2, ==, 0);                 \
	}

/* Execute a statement. */
#define EXEC(STMT_ID, LAST_INSERT_ID, ROWS_AFFECTED)                  \
	{                                                             \
		int rv2;                                              \
		rv2 = client__send_exec(&f->client, STMT_ID);         \
		munit_assert_int(rv2, ==, 0);                         \
		test_uv_run(&f->loop, 6);                             \
		rv2 = client__recv_result(&f->client, LAST_INSERT_ID, \
					  ROWS_AFFECTED);             \
		munit_assert_int(rv2, ==, 0);                         \
	}

/* Perform a query. */
#define QUERY(STMT_ID, ROWS)                                   \
	{                                                      \
		int rv2;                                       \
		rv2 = client__send_query(&f->client, STMT_ID); \
		munit_assert_int(rv2, ==, 0);                  \
		test_uv_run(&f->loop, 2);                      \
		rv2 = client__recv_rows(&f->client, ROWS);     \
		munit_assert_int(rv2, ==, 0);                  \
	}

/******************************************************************************
 *
 * Assertions.
 *
 ******************************************************************************/

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
	HANDSHAKE;
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
	HANDSHAKE;
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
	OPEN;
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
	HANDSHAKE;
	OPEN;
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
	PREPARE("CREATE TABLE test (n INT)", &stmt_id);
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
	HANDSHAKE;
	OPEN;
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
	PREPARE("CREATE TABLE test (n INT)", &f->stmt_id);
	EXEC(f->stmt_id, &last_insert_id, &rows_affected);
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
	PREPARE("BEGIN", &f->stmt_id);
	EXEC(f->stmt_id, &last_insert_id, &rows_affected);
	PREPARE("CREATE TABLE test (n INT)", &f->stmt_id);
	EXEC(f->stmt_id, &last_insert_id, &rows_affected);
	PREPARE("INSERT INTO test (n) VALUES(123)", &f->stmt_id);
	EXEC(f->stmt_id, &last_insert_id, &rows_affected);
	PREPARE("COMMIT", &f->stmt_id);
	EXEC(f->stmt_id, &last_insert_id, &rows_affected);
	munit_assert_int(last_insert_id, ==, 1);
	munit_assert_int(rows_affected, ==, 1);
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
	struct rows rows;
};

TEST_SETUP(query)
{
	struct query_fixture *f = munit_malloc(sizeof *f);
	unsigned stmt_id;
	unsigned last_insert_id;
	unsigned rows_affected;
	SETUP;
	HANDSHAKE;
	OPEN;
	PREPARE("CREATE TABLE test (n INT)", &stmt_id);
	EXEC(stmt_id, &last_insert_id, &rows_affected);
	PREPARE("INSERT INTO test(n) VALUES (123)", &stmt_id);
	EXEC(stmt_id, &last_insert_id, &rows_affected);
	return f;
}

TEST_TEAR_DOWN(query)
{
	struct query_fixture *f = data;
	client__close_rows(&f->rows);
	TEAR_DOWN;
	free(f);
}

TEST_CASE(query, success, NULL)
{
	struct query_fixture *f = data;
	struct row *row;
	(void)params;
	PREPARE("SELECT n FROM test", &f->stmt_id);
	QUERY(f->stmt_id, &f->rows);
	munit_assert_int(f->rows.column_count, ==, 1);
	munit_assert_string_equal(f->rows.column_names[0], "n");
	row = f->rows.next;
	munit_assert_ptr_not_null(row);
	munit_assert_ptr_null(row->next);
	munit_assert_int(row->values[0].type, ==, SQLITE_INTEGER);
	munit_assert_int(row->values[0].integer, ==, 123);
	return MUNIT_OK;
}
