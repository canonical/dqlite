#include "../lib/fs.h"
#include "../lib/heap.h"
#include "../lib/runner.h"
#include "../lib/socket.h"
#include "../lib/sqlite.h"
#include "../lib/thread.h"

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
	HANDLE(f->sockets.client);
	f->sockets.server_disconnected = true;
	return MUNIT_OK;
}
