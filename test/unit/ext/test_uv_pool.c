#include "../../../src/lib/threadpool.h"
#include "../../../src/utils.h"
#include "../../lib/runner.h"
#include "../../lib/uv.h"

TEST_MODULE(ext_uv_pool);

/******************************************************************************
 *
 * threadpool
 *
 ******************************************************************************/

enum { WORK_ITEMS_NR = 50000 };

struct fixture {
	pool_work_t w;
	uv_loop_t loop;
	pool_t pool;
};

static void loop_setup(struct fixture *f)
{
	int rc;

	rc = uv_loop_init(&f->loop);
	munit_assert_int(rc, ==, 0);

	rc = pool_init(&f->pool, &f->loop, 4, POOL_QOS_PRIO_FAIR);
	munit_assert_int(rc, ==, 0);
}

static void bottom_work_cb(pool_work_t *w)
{
	(void)w;
}

static void bottom_after_work_cb(pool_work_t *w)
{
	static int count = 0;

	if (count == WORK_ITEMS_NR)
		pool_close(w->pool);

	count++;
	assert(w->type != WT_BAR);
	free(w);
}

static void after_work_cb(pool_work_t *w)
{
	enum pool_work_type pwt;
	pool_work_t *work;
	unsigned int wt;
	unsigned int i;

	for (i = 0; i <= WORK_ITEMS_NR + 1 /* +WT_BAR */; i++) {
		work = calloc(1, sizeof(*work));

		if (i < WORK_ITEMS_NR / 2)
			wt = WT_ORD1;
		else if (i == WORK_ITEMS_NR / 2)
			wt = WT_BAR;
		else
			wt = WT_ORD2;

		pwt = i % 2 == 0 ? wt : WT_UNORD;
		pool_queue_work(w->pool, work, i, pwt, bottom_work_cb,
				bottom_after_work_cb);
	}
}

static void work_cb(pool_work_t *w)
{
	(void)w;
}

static void threadpool_tear_down(void *data)
{
	int rc;
	struct fixture *f = data;

	pool_fini(&f->pool);
	rc = uv_loop_close(&f->loop);
	munit_assert_int(rc, ==, 0);
	free(f);
}

static void *threadpool_setup(const MunitParameter params[], void *user_data)
{
	(void)params;
	(void)user_data;
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

	pool_queue_work(&f->pool, &f->w, 0, WT_UNORD, work_cb, after_work_cb);

	rc = uv_run(&f->loop, UV_RUN_DEFAULT);
	munit_assert_int(rc, ==, 0);

	return MUNIT_OK;
}
