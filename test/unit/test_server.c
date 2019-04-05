#include "../lib/client.h"
#include "../lib/fs.h"
#include "../lib/heap.h"
#include "../lib/runner.h"
#include "../lib/socket.h"
#include "../lib/sqlite.h"
#include "../lib/thread.h"

#include "../../include/dqlite.h"

#include "../../src/client.h"
#include "../../src/server.h"

TEST_MODULE(server);

/******************************************************************************
 *
 * Fixture
 *
 ******************************************************************************/

#define FIXTURE         \
	FIXTURE_THREAD; \
	char *dir;      \
	struct dqlite dqlite

#define SETUP                                          \
	int rv;                                        \
	SETUP_HEAP;                                    \
	SETUP_SQLITE;                                  \
	f->dir = test_dir_setup();                     \
	rv = dqlite__init(&f->dqlite, 1, "1", f->dir); \
	munit_assert_int(rv, ==, 0)

#define TEAR_DOWN                   \
	dqlite__close(&f->dqlite);  \
	test_dir_tear_down(f->dir); \
	TEAR_DOWN_SQLITE;           \
	TEAR_DOWN_HEAP

/******************************************************************************
 *
 * Helper macros.
 *
 ******************************************************************************/

static void *run(void *arg)
{
	struct dqlite *d = arg;
	int rc;
	rc = dqlite_run(d);
	if (rc) {
		return (void *)1;
	}
	return NULL;
}

/* Bootstrap the underlying raft configuration  */
#define BOOTSTRAP                                   \
	{                                           \
		int rv2;                            \
		rv2 = dqlite_bootstrap(&f->dqlite); \
		munit_assert_int(rv2, ==, 0);       \
	}

/* Run the dqlite server in a thread */
#define START THREAD_START(run, &f->dqlite)

/* Wait for the server to be ready */
#define READY munit_assert_true(dqlite_ready(&f->dqlite))

/* Stop the server and wait for it to be done */
#define STOP                     \
	dqlite_stop(&f->dqlite); \
	THREAD_JOIN

/* Handle a new connection */
#define HANDLE(FD)                                   \
	{                                            \
		int rv2;                             \
		rv2 = dqlite_handle(&f->dqlite, FD); \
		munit_assert_int(rv2, ==, 0);        \
	}

/* Send the initial client handshake. */
#define HANDSHAKE                                         \
	{                                                 \
		int rv2;                                  \
		rv2 = client__send_handshake(&f->client); \
		munit_assert_int(rv2, ==, 0);             \
	}

/* Open a test database. */
#define OPEN                                                 \
	{                                                    \
		int rv2;                                     \
		rv2 = client__send_open(&f->client, "test"); \
		munit_assert_int(rv2, ==, 0);                \
		rv2 = client__recv_db(&f->client);           \
		munit_assert_int(rv2, ==, 0);                \
	}

/* Prepare a statement. */
#define PREPARE(SQL, STMT_ID)                                 \
	{                                                     \
		int rv2;                                      \
		rv2 = client__send_prepare(&f->client, SQL);  \
		munit_assert_int(rv2, ==, 0);                 \
		rv2 = client__recv_stmt(&f->client, STMT_ID); \
		munit_assert_int(rv2, ==, 0);                 \
	}

/* Execute a statement. */
#define EXEC(STMT_ID, LAST_INSERT_ID, ROWS_AFFECTED)                  \
	{                                                             \
		int rv2;                                              \
		rv2 = client__send_exec(&f->client, STMT_ID);         \
		munit_assert_int(rv2, ==, 0);                         \
		rv2 = client__recv_result(&f->client, LAST_INSERT_ID, \
					  ROWS_AFFECTED);             \
		munit_assert_int(rv2, ==, 0);                         \
	}

/******************************************************************************
 *
 * dqlite_run
 *
 ******************************************************************************/

struct run_fixture
{
	FIXTURE;
};

TEST_SUITE(run);
TEST_SETUP(run)
{
	struct run_fixture *f = munit_malloc(sizeof *f);
	SETUP;
	return f;
}
TEST_TEAR_DOWN(run)
{
	struct run_fixture *f = data;
	TEAR_DOWN;
	free(f);
}

TEST_CASE(run, success, NULL)
{
	struct run_fixture *f = data;
	START;
	READY;
	STOP;
	(void)params;
	return MUNIT_OK;
}

/******************************************************************************
 *
 * dqlite_handle
 *
 ******************************************************************************/

struct handle_fixture
{
	FIXTURE;
	struct test_socket_pair sockets;
};

TEST_SUITE(handle);
TEST_SETUP(handle)
{
	struct handle_fixture *f = munit_malloc(sizeof *f);
	SETUP;
	START;
	READY;
	test_socket_pair_setup(params, &f->sockets);
	return f;
}
TEST_TEAR_DOWN(handle)
{
	struct handle_fixture *f = data;
	test_socket_pair_tear_down(&f->sockets);
	STOP;
	TEAR_DOWN;
	free(f);
}

TEST_CASE(handle, success, NULL)
{
	struct handle_fixture *f = data;
	(void)params;
	HANDLE(f->sockets.server);
	f->sockets.server_disconnected = true;
	return MUNIT_OK;
}

/******************************************************************************
 *
 * Handle client requests
 *
 ******************************************************************************/

struct client_fixture
{
	FIXTURE;
	FIXTURE_CLIENT;
};

TEST_SUITE(client);
TEST_SETUP(client)
{
	struct client_fixture *f = munit_malloc(sizeof *f);
	SETUP;
	SETUP_CLIENT;
	BOOTSTRAP;
	START;
	READY;
	HANDLE(f->sockets.server);
	HANDSHAKE;
	OPEN;
	return f;
}

TEST_TEAR_DOWN(client)
{
	struct client_fixture *f = data;
	STOP;
	TEAR_DOWN_CLIENT;
	TEAR_DOWN;
	free(f);
}

TEST_CASE(client, exec, NULL)
{
	struct client_fixture *f = data;
	unsigned stmt_id;
	unsigned last_insert_id;
	unsigned rows_affected;
	(void)params;
	PREPARE("CREATE TABLE test (n INT)", &stmt_id);
	EXEC(stmt_id, &last_insert_id, &rows_affected);
	return MUNIT_OK;
}
