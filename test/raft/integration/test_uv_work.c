#include <unistd.h>

#include "../../../src/raft/uv.h"
#include "../lib/dir.h"
#include "../lib/loop.h"
#include "../lib/runner.h"
#include "../lib/uv.h"

/******************************************************************************
 *
 * Fixture
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_UV_DEPS;
    FIXTURE_UV;
};

struct result
{
    int rv;      /* Indicate success or failure of the work */
    int counter; /* Proof that work was performed */
    bool done;   /* To check test termination */
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

static void tearDownDeps(void *data)
{
    struct fixture *f = data;
    if (f == NULL) {
        return;
    }
    TEAR_DOWN_UV_DEPS;
    free(f);
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    if (f == NULL) {
        return;
    }
    TEAR_DOWN_UV;
    tearDownDeps(f);
}

/******************************************************************************
 *
 * UvAsyncWork
 *
 *****************************************************************************/

static void asyncWorkCbAssertResult(struct raft_io_async_work *req, int status)
{
    struct result *r = req->data;
    munit_assert_int(status, ==, r->rv);
    munit_assert_int(r->counter, ==, 1);
    r->done = true;
}

static int asyncWorkFn(struct raft_io_async_work *req)
{
    struct result *r = req->data;
    sleep(1);
    r->counter = 1;
    return r->rv;
}

SUITE(UvAsyncWork)

static char *rvs[] = {"-1", "0", "1", "37", NULL};
static MunitParameterEnum rvs_params[] = {
    {"rv", rvs},
    {NULL, NULL},
};

TEST(UvAsyncWork, work, setUp, tearDown, 0, rvs_params)
{
    struct fixture *f = data;
    struct result res = {0};
    struct raft_io_async_work req = {0};
    res.rv = (int)strtol(munit_parameters_get(params, "rv"), NULL, 0);
    req.data = &res;
    req.work = asyncWorkFn;
    UvAsyncWork(&f->io, &req, asyncWorkCbAssertResult);
    LOOP_RUN_UNTIL(&res.done);
    return MUNIT_OK;
}
