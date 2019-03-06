/**
 * Add support for using the libuv loop in tests.
 */

#ifndef TEST_UV_H
#define TEST_UV_H

#include <uv.h>

#include "munit.h"

/* Max n. of loop iterations ran by a single function call */
#define TEST_UV_MAX_LOOP_RUN 10

/**
 * Initialize the given libuv loop.
 */
void test_uv_setup(const MunitParameter params[], struct uv_loop_s *l);

/**
 * Run the loop until there are no pending active handles.
 *
 * If there are still pending active handles after 10 loop iterations, the test
 * will fail.
 *
 * This is meant to be used in tear down functions.
 */
void test_uv_stop(struct uv_loop_s *l);

/**
 * Tear down the loop making sure no active handles are left.
 */
void test_uv_tear_down(struct uv_loop_s *l);

/**
 * Run the loop until there are no pending active handles or the given amount of
 * iterations is reached.
 *
 * Return non-zero if there are pending handles.
 */
int test_uv_run(struct uv_loop_s *l, unsigned n);

/**
 * Run the loop until the given function returns true.
 *
 * If the loop exhausts all active handles or if #TEST_UV_MAX_LOOP_RUN is
 * reached without @f returning #true, the test fails.
 */
#define test_uv_run_until(DATA, FUNC)                                        \
	{                                                                    \
		unsigned i;                                                  \
		int rv;                                                      \
		for (i = 0; i < TEST_UV_MAX_LOOP_RUN; i++) {                 \
			if (FUNC(DATA)) {                                    \
				break;                                       \
			}                                                    \
			rv = uv_run(&f->loop, UV_RUN_ONCE);                  \
			if (rv < 0) {                                        \
				munit_errorf("uv_run: %s", uv_strerror(rv)); \
			}                                                    \
			if (rv == 0) {                                       \
				if (FUNC(DATA)) {                            \
					break;                               \
				}                                            \
				munit_errorf(                                \
				    "uv_run: stopped after %u iterations",   \
				    i + 1);                                  \
			}                                                    \
		}                                                            \
		if (i == TEST_UV_MAX_LOOP_RUN) {                             \
			munit_errorf(                                        \
			    "uv_run: condition not met in %d iterations",    \
			    TEST_UV_MAX_LOOP_RUN);                           \
		}                                                            \
	}

#endif /* TEST_UV_H */
