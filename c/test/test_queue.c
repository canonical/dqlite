#include <sqlite3.h>
#include <uv.h>

#include "../src/conn.h"
#include "../src/error.h"
#include "../src/queue.h"

#include "cluster.h"
#include "leak.h"
#include "munit.h"
#include "socket.h"

/******************************************************************************
 *
 * Helpers
 *
 ******************************************************************************/

struct fixture {
	struct test_socket_pair sockets;
	uv_loop_t               loop;
	struct dqlite__queue    queue;
};

/******************************************************************************
 *
 * Setup and tear down
 *
 ******************************************************************************/

static void *setup(const MunitParameter params[], void *user_data) {
	struct fixture *f;
	int             err;

	(void)params;
	(void)user_data;

	f = munit_malloc(sizeof *f);

	err = test_socket_pair_initialize(&f->sockets);
	munit_assert_int(err, ==, 0);

	err = uv_loop_init(&f->loop);
	munit_assert_int(err, ==, 0);

	dqlite__queue_init(&f->queue);
	return f;
}

static void tear_down(void *data) {
	struct fixture *f = data;
	int             err;

	dqlite__queue_close(&f->queue);

	err = uv_loop_close(&f->loop);
	munit_assert_int(err, ==, 0);

	err = test_socket_pair_cleanup(&f->sockets);
	munit_assert_int(err, ==, 0);

	test_assert_no_leaks();
}

/******************************************************************************
 *
 * Tests for dqlite__queue_push
 *
 ******************************************************************************/

static MunitResult test_push(const MunitParameter params[], void *data) {
	struct fixture *f = data;
	int             err;

	struct dqlite__conn       conn;
	struct dqlite__queue_item item;

	(void)params;

	dqlite__conn_init(&conn, 123, test_cluster(), &f->loop);

	err = dqlite__queue_item_init(&item, &conn);
	munit_assert_int(err, ==, 0);

	err = dqlite__queue_push(&f->queue, &item);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_equal(dqlite__queue_pop(&f->queue), &item);

	dqlite__queue_item_close(&item);
	dqlite__conn_close(&conn);

	return MUNIT_OK;
}

static MunitTest dqlite__queue_push_tests[] = {
    {"", test_push, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * Tests for dqlite__queue_process
 *
 ******************************************************************************/

static MunitResult test_process(const MunitParameter params[], void *data) {
	struct fixture *          f = data;
	int                       err;
	struct dqlite__queue_item item;
	struct dqlite__conn       conn;

	(void)params;

	dqlite__conn_init(&conn, f->sockets.server, test_cluster(), &f->loop);

	err = dqlite__queue_item_init(&item, &conn);
	munit_assert_int(err, ==, 0);

	err = dqlite__queue_push(&f->queue, &item);
	munit_assert_int(err, ==, 0);

	dqlite__queue_process(&f->queue);

	munit_assert(dqlite__error_is_null(&item.error));

	/* At this point enqueued item should have been processed and
	 * unblocked */
	dqlite__queue_item_wait(&item);

	munit_assert(dqlite__error_is_null(&item.error));

	/* Abort the newly created connection */
	err = test_socket_pair_client_disconnect(&f->sockets);
	munit_assert_int(err, ==, 0);

	err = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(err, ==, 0);

	f->sockets.server_disconnected = 1;

	dqlite__queue_item_close(&item);
	dqlite__conn_close(&conn);

	return MUNIT_OK;
}

static MunitTest dqlite__queue_process_tests[] = {
    {"", test_process, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * Suite
 *
 ******************************************************************************/

MunitSuite dqlite__queue_suites[] = {
    {"_process", dqlite__queue_process_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE},
    {"_push", dqlite__queue_push_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE},
    {NULL, NULL, NULL, 0, MUNIT_SUITE_OPTION_NONE},
};
