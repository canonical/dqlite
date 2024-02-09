/* Add support for using the libuv loop in tests. */

#ifndef TEST_LOOP_H
#define TEST_LOOP_H

#include <uv.h>

#include "../../../src/raft.h"
#include "munit.h"

/* Max n. of loop iterations ran by a single function call */
#define LOOP_MAX_RUN 20

#define FIXTURE_LOOP struct uv_loop_s loop

/* Older libuv versions might try to free() memory that was not allocated. */
#if HAVE_DECL_UV_FS_O_CREAT
#define LOOP_REPLACE_ALLOCATOR                                         \
    _rv = uv_replace_allocator(raft_malloc, raft_realloc, raft_calloc, \
                               raft_free);                             \
    munit_assert_int(_rv, ==, 0)
#else
#define LOOP_REPLACE_ALLOCATOR
#endif

#define SETUP_LOOP                    \
    {                                 \
        int _rv;                      \
        LOOP_REPLACE_ALLOCATOR;       \
        _rv = uv_loop_init(&f->loop); \
        munit_assert_int(_rv, ==, 0); \
    }

#define TEAR_DOWN_LOOP                                                     \
    {                                                                      \
        int rv_;                                                           \
        int alive_ = uv_loop_alive(&f->loop);                              \
        if (alive_ != 0) {                                                 \
            LOOP_STOP;                                                     \
        }                                                                  \
        rv_ = uv_loop_close(&f->loop);                                     \
        if (rv_ != 0) {                                                    \
            uv_walk(&f->loop, test_loop_walk_cb, NULL);                    \
            munit_errorf("uv_loop_close: %s (%d)", uv_strerror(rv_), rv_); \
        }                                                                  \
        rv_ = uv_replace_allocator(malloc, realloc, calloc, free);         \
        munit_assert_int(rv_, ==, 0);                                      \
    }

/* Run the loop until there are no pending active handles or the given amount of
 * iterations is reached. */
#define LOOP_RUN(N)                                                       \
    {                                                                     \
        unsigned i__;                                                     \
        int rv__;                                                         \
        for (i__ = 0; i__ < N; i__++) {                                   \
            rv__ = uv_run(&f->loop, UV_RUN_ONCE);                         \
            if (rv__ < 0) {                                               \
                munit_errorf("uv_run: %s (%d)", uv_strerror(rv__), rv__); \
            }                                                             \
            if (rv__ == 0) {                                              \
                break;                                                    \
            }                                                             \
        }                                                                 \
    }

/* Run the loop until the value stored through the given boolean pointer is
 * true.
 *
 * If the loop exhausts all active handles or if #LOOP_MAX_RUN is reached, the
 * test fails. */
#define LOOP_RUN_UNTIL(CONDITION)                                             \
    {                                                                         \
        unsigned __i;                                                         \
        int __rv;                                                             \
        for (__i = 0; __i < LOOP_MAX_RUN; __i++) {                            \
            if (*(CONDITION)) {                                               \
                break;                                                        \
            }                                                                 \
            __rv = uv_run(&f->loop, UV_RUN_ONCE);                             \
            if (__rv < 0) {                                                   \
                munit_errorf("uv_run: %s (%d)", uv_strerror(__rv), __rv);     \
            }                                                                 \
            if (__rv == 0) {                                                  \
                if (*(CONDITION)) {                                           \
                    break;                                                    \
                }                                                             \
                munit_errorf("uv_run: stopped after %u iterations", __i + 1); \
            }                                                                 \
        }                                                                     \
        if (!*(CONDITION)) {                                                  \
            munit_errorf("uv_run: condition not met in %d iterations",        \
                         LOOP_MAX_RUN);                                       \
        }                                                                     \
    }

/* Run the loop until there are no pending active handles.
 *
 * If there are still pending active handles after LOOP_MAX_RUN iterations, the
 * test will fail.
 *
 * This is meant to be used in tear down functions. */
#define LOOP_STOP                                                 \
    {                                                             \
        int alive__;                                              \
        LOOP_RUN(LOOP_MAX_RUN);                                   \
        alive__ = uv_loop_alive(&f->loop);                        \
        if (alive__ != 0) {                                       \
            munit_error("loop has still pending active handles"); \
        }                                                         \
    }

void test_loop_walk_cb(uv_handle_t *handle, void *arg);

#endif /* TEST_LOOP_H */
