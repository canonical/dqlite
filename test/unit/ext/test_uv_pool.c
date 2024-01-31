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

enum { WORK_ITEMS_NR = 5000 };

struct fixture
{
	xx_loop_t xx_loop;
	xx_work_t work_req;
};

static void loop_setup(struct fixture *f)
{
	int rc;

	rc = uv_loop_init(&f->xx_loop.loop);
	munit_assert_int(rc, ==, 0);

	rc = xx_loop_init(&f->xx_loop);
	munit_assert_int(rc, ==, 0);
}

static void bottom_work_cb(xx_work_t *req)
{
	munit_assert_uint(req->work_req.thread_idx, ==, xx__thread_id());
}

static void bottom_after_work_cb(xx_work_t *req, int status UNUSED)
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
		xx_loop_async_close(req->loop);

	count++;
	free(req);
}

static void after_work_cb(xx_work_t *req, int status UNUSED)
{
	unsigned int i;
	int rc;
	xx_work_t *work;

	for (i = 0; i <= WORK_ITEMS_NR; i++) {
		work = malloc(sizeof(*work));
		work->work_req.type = i % 2 == 0 ? WT_ORD1 : WT_UNORD;
		rc = xx_queue_work(req->loop, work, i, bottom_work_cb,
				   bottom_after_work_cb);
		munit_assert_int(rc, ==, 0);
	}
}

static void work_cb(xx_work_t *req)
{
	munit_assert_uint(req->work_req.thread_idx, ==, xx__thread_id());
}

static void threadpool_tear_down(void *data)
{
	int rc;
	struct fixture *f = data;

	xx_loop_close(&f->xx_loop);
	rc = uv_loop_close(&f->xx_loop.loop);
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
	rc = xx_queue_work(&f->xx_loop.loop, &f->work_req, 0,
			   work_cb, after_work_cb);
	munit_assert_int(rc, ==, 0);

	rc = uv_run(&f->xx_loop.loop, UV_RUN_DEFAULT);
	munit_assert_int(rc, ==, 0);

	return MUNIT_OK;
}
