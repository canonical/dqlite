#include <CUnit/CUnit.h>
#include <sqlite3.h>
#include <uv.h>

#include "../src/conn.h"
#include "../src/error.h"
#include "../src/queue.h"

#include "cluster.h"
#include "socket.h"
#include "suite.h"

static struct test_socket_pair sockets;
static uv_loop_t loop;
static struct dqlite__queue queue;

void test_dqlite__queue_setup()
{
	int err;

	err = test_socket_pair_initialize(&sockets);
	if (err != 0)
		CU_FAIL("test setup failed");

	err = uv_loop_init(&loop);
	if (err != 0) {
		test_suite_errorf("failed to init UV loop: %s - %d", uv_strerror(errno), err);
		CU_FAIL("test setup failed");
	}

	dqlite__queue_init(&queue);
}

void test_dqlite__queue_teardown()
{
	int err;

	dqlite__queue_close(&queue);

	err = uv_loop_close(&loop);
	if (err != 0) {
		test_suite_errorf("failed to close UV loop: %s - %d", uv_strerror(err), err);
		test_suite_teardown_fail();
		return;
	}

	err = test_socket_pair_cleanup(&sockets);
	if (err != 0) {
		test_suite_teardown_fail();
		return;
	}

	test_suite_teardown_pass();
}

/*
 * dqlite__queue_push_suite
 */

void test_dqlite__queue_push()
{
	int err;
	FILE *log = test_suite_dqlite_log();
	struct dqlite__conn conn;
	struct dqlite__queue_item item;

	dqlite__conn_init(&conn, log, 123, test_cluster());

	err = dqlite__queue_item_init(&item, &conn);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	err = dqlite__queue_push(&queue, &item);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	CU_ASSERT_PTR_EQUAL(dqlite__queue_pop(&queue), &item);

	dqlite__queue_item_close(&item);
	dqlite__conn_close(&conn);
}

/*
 * dqlite__queue_process_suite
 */

void test_dqlite__queue_process()
{
	int err;
	FILE *log = test_suite_dqlite_log();
	struct dqlite__queue_item item;
	struct dqlite__conn conn;

	dqlite__conn_init(&conn, log, sockets.server, test_cluster());

	err = dqlite__queue_item_init(&item, &conn);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	err = dqlite__queue_push(&queue, &item);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	dqlite__queue_process(&queue, &loop);

	CU_ASSERT_FATAL(dqlite__error_is_null(&item.error));

	/* At this point enqueued item should have been processed and
	 * unblocked */
	dqlite__queue_item_wait(&item);

	CU_ASSERT_FATAL(dqlite__error_is_null(&item.error));

	/* Abort the newly created connection */
	err = test_socket_pair_client_disconnect(&sockets);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	err = uv_run(&loop, UV_RUN_NOWAIT);
	CU_ASSERT_EQUAL(err, 0);

	sockets.server_disconnected = 1;

	dqlite__queue_item_close(&item);
	dqlite__conn_close(&conn);
}
