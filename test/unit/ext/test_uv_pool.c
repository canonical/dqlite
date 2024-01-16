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

enum { WORK_ITEMS_NR = 50 };

static xx_loop_t default_loop_struct;
static xx_loop_t *default_loop_ptr;
static xx_work_t work_req;

static xx_loop_t* xx_default_loop(void)
{
    int rc;

    if (default_loop_ptr != NULL)
	return default_loop_ptr;

    rc = uv_loop_init(&default_loop_struct.loop);
    munit_assert_int(rc, ==, 0);

    rc = xx_loop_init(&default_loop_struct);
    munit_assert_int(rc, ==, 0);

    default_loop_ptr = &default_loop_struct;
    return default_loop_ptr;
}

static void bottom_work_cb(xx_work_t* req UNUSED)
{
    munit_logf(MUNIT_LOG_INFO, "bottom_work_cb() tid=%d", gettid());
}


static void bottom_after_work_cb(xx_work_t* req, int status UNUSED)
{
  static int count = 0;
  munit_logf(MUNIT_LOG_INFO, "bottom_after_work_cb() tid=%d req=%p count=%d",
	     gettid(), req, count);

  /*
   * XXX: This is a very strange way to close the uv_loop.  The alternative is
   *      proctological and can be seen in libuv v1.46 around special handling
   *      for initialization:
   *      uv__handle_unref(&loop->wq_async);
   *      loop->wq_async.flags |= UV_HANDLE_INTERNAL;
   *      and for finalization:
   *      uv_walk() ... if (h->flags & UV_HANDLE_INTERNAL) continue;
   */
  if (count == WORK_ITEMS_NR)
      xx_loop_async_close(req->loop);

  count++;
  free(req);
}

static void after_work_cb(xx_work_t *req UNUSED, int status UNUSED) {
    int i;
    int rc;
    xx_work_t* work;

    for (i = 0; i <= WORK_ITEMS_NR; i++) {
	work = malloc(sizeof(*work));
	munit_logf(MUNIT_LOG_INFO, "after_work_cb() tid=%d", gettid());
	rc = xx_queue_work(&xx_default_loop()->loop,
			   work,
			   bottom_work_cb,
			   bottom_after_work_cb);
	munit_assert_int(rc, ==, 0);
    }
}

static void work_cb(xx_work_t *req UNUSED)
{
    munit_logf(MUNIT_LOG_INFO, "work_cb()tid=%d", gettid());
}

static void threadpool_tear_down(void *data UNUSED)
{
    int rc;

    xx__threadpool_cleanup();
    xx_loop_close(xx_default_loop());
    rc = uv_loop_close(&xx_default_loop()->loop);
    munit_assert_int(rc, ==, 0);
}

static void *threadpool_setup(const MunitParameter params[] UNUSED,
			      void *user_data UNUSED)
{
    xx_default_loop();
    return NULL;
}

TEST_SUITE(threadpool);
TEST_SETUP(threadpool, threadpool_setup);
TEST_TEAR_DOWN(threadpool, threadpool_tear_down);

TEST_CASE(threadpool, sync, NULL)
{
    (void)params;
    (void)data;
    int rc;

    rc = xx_queue_work(&xx_default_loop()->loop, &work_req,
		       work_cb, after_work_cb);
    munit_assert_int(rc, ==, 0);

    rc = uv_run(&xx_default_loop()->loop, UV_RUN_DEFAULT);
    munit_assert_int(rc, ==, 0);

    return MUNIT_OK;
}
