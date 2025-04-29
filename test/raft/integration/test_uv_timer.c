#include "../../../src/raft/uv.h"
#include "../lib/runner.h"
#include "../lib/uv.h"

#include <unistd.h>

/******************************************************************************
 *
 * Fixture with a libuv-based raft_io instance.
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_UV_DEPS;
    FIXTURE_UV;
};

/******************************************************************************
 *
 * Set up and tear down.
 *
 *****************************************************************************/

static void *setUp(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_UV_DEPS;
    SETUP_UV;
    return f;
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    if (f == NULL) {
        return;
    }
    TEAR_DOWN_UV;
    TEAR_DOWN_UV_DEPS;
    free(f);
}

SUITE(timer)

struct test_timer {
    struct raft_timer timer;
    int target, count;
    bool success;
};

void callback(struct raft_timer *t) {
    struct test_timer *timer = (struct test_timer *)t;
    timer->count++;
    if (timer->count >= timer->target) {
        timer->success = true;
    }
}

TEST(timer, once, setUp, tearDown, 0, NULL)
{
    int rv;
    struct fixture *f = data;
    struct test_timer timer = {
        .target = 1,
    };

    rv = UvTimerStart(&f->io, &timer.timer, 100, 0, callback);
    munit_assert_int(rv, ==, 0);
    LOOP_RUN_UNTIL(&timer.success);

    rv = UvTimerStop(&f->io, &timer.timer);
    munit_assert_int(rv, ==, 0);

    return MUNIT_OK;
}

TEST(timer, repeated, setUp, tearDown, 0, NULL)
{
    int rv;
    struct fixture *f = data;
    struct test_timer timer = {
        .target = 5,
    };
    rv = UvTimerStart(&f->io, &timer.timer, 100, 100, callback);
    munit_assert_int(rv, ==, 0);
    LOOP_RUN_UNTIL(&timer.success);

    rv = UvTimerStop(&f->io, &timer.timer);
    munit_assert_int(rv, ==, 0);

    return MUNIT_OK;
}

TEST(timer, stop, setUp, tearDown, 0, NULL)
{
    int rv;
    struct fixture *f = data;
    struct test_timer timer = {
        .target = 2,
    };
    rv = UvTimerStart(&f->io, &timer.timer, 100, 100, callback);
    munit_assert_int(rv, ==, 0);
    LOOP_RUN_UNTIL(&timer.success);

    rv = UvTimerStop(&f->io, &timer.timer);
    munit_assert_int(rv, ==, 0);

    rv = uv_run(&f->loop, UV_RUN_ONCE);
    if (rv < 0) {
        munit_errorf("uv_run: %s (%d)", uv_strerror(rv), rv);
    }
    munit_assert_int(rv, ==, 0);
    return MUNIT_OK;
}
