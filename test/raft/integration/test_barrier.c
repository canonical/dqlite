#include "../lib/cluster.h"
#include "../lib/runner.h"

/******************************************************************************
 *
 * Fixture
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_CLUSTER;
};

static void *setUp(const MunitParameter params[], MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_CLUSTER(2);
    CLUSTER_BOOTSTRAP;
    CLUSTER_START;
    CLUSTER_ELECT(0);
    return f;
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_CLUSTER;
    free(f);
}

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

struct result
{
    int status;
    bool done;
};

static void barrierCbAssertResult(struct raft_barrier *req, int status)
{
    struct result *result = req->data;
    munit_assert_int(status, ==, result->status);
    result->done = true;
}

static bool barrierCbHasFired(struct raft_fixture *f, void *arg)
{
    struct result *result = arg;
    (void)f;
    return result->done;
}

/* Submit a barrier request. */
#define BARRIER_SUBMIT(I)                                              \
    struct raft_barrier _req;                                          \
    struct result _result = {0, false};                                \
    int _rv;                                                           \
    _req.data = &_result;                                              \
    _rv = raft_barrier(CLUSTER_RAFT(I), &_req, barrierCbAssertResult); \
    munit_assert_int(_rv, ==, 0);

/* Expect the barrier callback to fire with the given status. */
#define BARRIER_EXPECT(STATUS) _result.status = STATUS

/* Wait until the barrier request completes. */
#define BARRIER_WAIT CLUSTER_STEP_UNTIL(barrierCbHasFired, &_result, 2000)

/* Submit to the I'th server a barrier request and wait for the operation to
 * succeed. */
#define BARRIER(I)         \
    do {                   \
        BARRIER_SUBMIT(I); \
        BARRIER_WAIT;      \
    } while (0)

/******************************************************************************
 *
 * Success scenarios
 *
 *****************************************************************************/

SUITE(raft_barrier)

TEST(raft_barrier, cb, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    BARRIER(0);
    return MUNIT_OK;
}
