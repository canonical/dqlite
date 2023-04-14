#include "uv.h"

#define TEST_UV_MAX_LOOP_RUN 10 /* Max n. of loop iterations upon teardown */

void test_uv_setup(const MunitParameter params[], struct uv_loop_s *l)
{
	int rv;

	(void)params;

	rv = uv_loop_init(l);
	munit_assert_int(rv, ==, 0);
}

int test_uv_run(struct uv_loop_s *l, unsigned n)
{
	unsigned i;
	int rv;

	munit_assert_int(n, >, 0);

	for (i = 0; i < n; i++) {
		rv = uv_run(l, UV_RUN_ONCE);
		if (rv < 0) {
			munit_errorf("uv_run: %s (%d)", uv_strerror(rv), rv);
		}
		if (rv == 0) {
			break;
		}
	}

	return rv;
}

void test_uv_stop(struct uv_loop_s *l)
{
	unsigned n_handles;

	/* Spin a few times to trigger pending callbacks. */
	n_handles = test_uv_run(l, TEST_UV_MAX_LOOP_RUN);
	if (n_handles > 0) {
		munit_errorf("loop has still %d pending active handles",
			     n_handles);
	}
}

static void test_uv__walk_cb(uv_handle_t *handle, void *arg)
{
	(void)arg;

	munit_logf(MUNIT_LOG_INFO, "handle %d", handle->type);
}

void test_uv_tear_down(struct uv_loop_s *l)
{
	int rv;

	rv = uv_loop_close(l);
	if (rv != 0) {
		uv_walk(l, test_uv__walk_cb, NULL);
		munit_errorf("uv_loop_close: %s (%d)", uv_strerror(rv), rv);
	}

	rv = uv_replace_allocator(malloc, realloc, calloc, free);
	munit_assert_int(rv, ==, 0);
}
