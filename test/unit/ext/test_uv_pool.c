#include "../../../src/lib/threadpool.h"
#include "../../lib/runner.h"
#include "../../lib/uv.h"
#include "src/utils.h"

TEST_MODULE(ext_uv_pool);

/******************************************************************************
 *
 * threadpool
 *
 ******************************************************************************/

enum { WORK_ITEMS_NR = 50000 };

struct fixture
{
	pool_loop_t ploop;
	pool_work_t work_req;
};

static void loop_setup(struct fixture *f)
{
	int rc;

	rc = uv_loop_init(&f->ploop.loop);
	munit_assert_int(rc, ==, 0);

	rc = pool_loop_init(&f->ploop);
	munit_assert_int(rc, ==, 0);
}

static void bottom_work_cb(pool_work_t *req)
{
	pool_loop_t *pl = uv_to_pool_loop(req->work_req.loop);
	munit_assert_uint(req->work_req.thread_idx, ==, pool_loop_thread_id(pl));
}

static void bottom_after_work_cb(pool_work_t *req)
{
	static int count = 0;

	/*
	 * Note: Close the uv_loop. The alternative can be seen in libuv v1.46
	 *       around special handling
	 *
	 *      - for initialization:
	 *      uv__handle_unref(&loop->wq_async);
	 *      loop->wq_async.flags |= UV_HANDLE_INTERNAL;
	 *
	 *      - and for finalization:
	 *      uv_walk() ... if (h->flags & UV_HANDLE_INTERNAL) continue;
	 */
	if (count == WORK_ITEMS_NR)
		pool_loop_close(req->loop);

	count++;
	assert(req->work_req.type != WT_BAR);
	free(req);
}

static void after_work_cb(pool_work_t *req)
{
	unsigned int i;
	int rc;
	pool_work_t *work;
	unsigned int wt;

	for (i = 0; i <= WORK_ITEMS_NR + 1 /*for BAR*/; i++) {
		work = malloc(sizeof(*work));

		if (i < WORK_ITEMS_NR / 2)
		    wt = WT_ORD1;
		else if (i == WORK_ITEMS_NR / 2)
		    wt = WT_BAR;
		else
		    wt = WT_ORD2;

		work->work_req.type = i % 2 == 0 ? wt : WT_UNORD;
		rc = pool_queue_work(req->loop, work, i, bottom_work_cb,
				     bottom_after_work_cb);
		munit_assert_int(rc, ==, 0);
	}
}

static void work_cb(pool_work_t *req)
{
	pool_loop_t *pl = uv_to_pool_loop(req->work_req.loop);
	munit_assert_uint(req->work_req.thread_idx, ==, pool_loop_thread_id(pl));
}

static void threadpool_tear_down(void *data)
{
	int rc;
	struct fixture *f = data;

	pool_loop_fini(&f->ploop);
	rc = uv_loop_close(&f->ploop.loop);
	munit_assert_int(rc, ==, 0);
	free(f);
}

static void *threadpool_setup(const MunitParameter params[] UNUSED,
			      void *user_data UNUSED)
{
	struct fixture *f = calloc(1, sizeof *f);
	loop_setup(f);
	return f;
}

TEST_SUITE(threadpool);
TEST_SETUP(threadpool, threadpool_setup);
TEST_TEAR_DOWN(threadpool, threadpool_tear_down);
TEST_CASE(threadpool, sync, NULL)
{
	(void)params;
	struct fixture *f = data;
	int rc;

	f->work_req.work_req.type = WT_UNORD;
	rc = pool_queue_work(&f->ploop.loop, &f->work_req, 0,
			     work_cb, after_work_cb);
	munit_assert_int(rc, ==, 0);

	rc = uv_run(&f->ploop.loop, UV_RUN_DEFAULT);
	munit_assert_int(rc, ==, 0);

	return MUNIT_OK;
}
