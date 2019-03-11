#include <sqlite3.h>
#include <uv.h>

#include "../src/conn.h"
#include "../src/error.h"
#include "../src/metrics.h"
#include "../src/options.h"
#include "../src/queue.h"

#include "./lib/heap.h"
#include "./lib/runner.h"
#include "./lib/socket.h"
#include "./lib/sqlite.h"
#include "./lib/uv.h"
#include "cluster.h"
#include "log.h"

TEST_MODULE(queue);

/******************************************************************************
 *
 * Helpers
 *
 ******************************************************************************/

struct fixture
{
	struct test_socket_pair sockets;
	uv_loop_t loop;
	struct dqlite__queue queue;
	struct options options;
	struct dqlite__metrics metrics;
	dqlite_logger *logger;
	dqlite_cluster *cluster;
};

/******************************************************************************
 *
 * Setup and tear down
 *
 ******************************************************************************/

static void *setup(const MunitParameter params[], void *user_data)
{
	struct fixture *f = munit_malloc(sizeof *f);

	(void)params;
	(void)user_data;

	test_heap_setup(params, user_data);
	test_sqlite_setup(params);
	test_socket_pair_setup(params, &f->sockets);
	test_uv_setup(params, &f->loop);

	dqlite__queue_init(&f->queue);

	options__init(&f->options);
	dqlite__metrics_init(&f->metrics);

	f->logger = test_logger();
	f->cluster =  test_cluster();

	return f;
}

static void tear_down(void *data)
{
	struct fixture *f = data;

	dqlite__queue_close(&f->queue);

	test_uv_tear_down(&f->loop);

	test_socket_pair_tear_down(&f->sockets);
	test_sqlite_tear_down();
	test_heap_tear_down(data);

	test_cluster_close(f->cluster);

	free(f->logger);
	free(f);
}

/******************************************************************************
 *
 * Tests for dqlite__queue_push
 *
 ******************************************************************************/

TEST_SUITE(push);
TEST_SETUP(push, setup);
TEST_TEAR_DOWN(push, tear_down);

TEST_CASE(push, success, NULL)
{
	struct fixture *f = data;
	int err;

	struct conn conn;
	struct dqlite__queue_item item;

	(void)params;

	conn__init(&conn, 123, f->logger, f->cluster, &f->loop,
		   &f->options, &f->metrics);

	err = dqlite__queue_item_init(&item, &conn);
	munit_assert_int(err, ==, 0);

	err = dqlite__queue_push(&f->queue, &item);
	munit_assert_int(err, ==, 0);

	munit_assert_ptr_equal(dqlite__queue_pop(&f->queue), &item);

	dqlite__queue_item_close(&item);
	conn__close(&conn);

	return MUNIT_OK;
}

/******************************************************************************
 *
 * Tests for dqlite__queue_process
 *
 ******************************************************************************/

TEST_SUITE(process);
TEST_SETUP(process, setup);
TEST_TEAR_DOWN(process, tear_down);

TEST_CASE(process, success, NULL)
{
	struct fixture *f = data;
	int err;
	struct dqlite__queue_item item;
	struct conn *conn;

	(void)params;

	conn = munit_malloc(sizeof *conn);

	conn__init(conn, f->sockets.server, f->logger, f->cluster,
		   &f->loop, &f->options, &f->metrics);

	err = dqlite__queue_item_init(&item, conn);
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
	test_socket_pair_client_disconnect(&f->sockets);

	err = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(err, ==, 1 /* Number of pending handles */);

	err = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(err, ==, 0);

	f->sockets.server_disconnected = 1;

	dqlite__queue_item_close(&item);

	return MUNIT_OK;
}
