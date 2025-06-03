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
    int pending;
};

static void barrierCbAssertResult(struct raft_barrier *req, int status)
{
    struct result *result = req->data;
    munit_assert_int(status, ==, RAFT_OK);
    munit_assert_int(result->pending, >, 0);
    result->pending--;
}

static bool barrierDone(struct raft_fixture *f, void *arg)
{
    struct result *result = arg;
    (void)f;
    return result->pending == 0;
}

SUITE(raft_barrier)

TEST(raft_barrier, cb, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    struct result result = { 1 };
    struct raft_barrier req = {
        .data = &result,
    };
    int _rv = raft_barrier(CLUSTER_RAFT(0), &req, barrierCbAssertResult);
    munit_assert_int(_rv, ==, 0);
    CLUSTER_STEP_UNTIL(barrierDone, &(result), 2000);

    return MUNIT_OK;
}

TEST(raft_barrier, multiple, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
    struct result result = {};

    #define REQ_N 100
    struct raft_barrier reqs[REQ_N] = {};
    for (int i = 0; i < REQ_N; i++) {
        reqs[i] = (struct raft_barrier) {
            .data = &result,
        };
        result.pending++;
        int rv = raft_barrier(CLUSTER_RAFT(0), &reqs[i], barrierCbAssertResult);
        munit_assert_int(rv, ==, 0);
        munit_assert_int(result.pending, ==, i+1);
    }
    CLUSTER_STEP_UNTIL(barrierDone, &(result), 2000);
    return MUNIT_OK;
}
