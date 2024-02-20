#include "../lib/cluster.h"
#include "../lib/runner.h"

/******************************************************************************
 *
 * Fixture holding a pristine raft instance.
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_CLUSTER;
};

static void *setUp(const MunitParameter params[], MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_CLUSTER(1);
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
 * Bootstrap tests.
 *
 *****************************************************************************/

SUITE(raft_bootstrap)

/* Attempting to bootstrap an instance that's already started results in
 * RAFT_BUSY. */
TEST(raft_bootstrap, busy, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft *raft;
    struct raft_configuration configuration;
    int rv;

    /* Bootstrap and the first server. */
    CLUSTER_BOOTSTRAP_N_VOTING(1);
    CLUSTER_START;

    raft = CLUSTER_RAFT(0);
    CLUSTER_CONFIGURATION(&configuration);
    rv = raft_bootstrap(raft, &configuration);
    munit_assert_int(rv, ==, RAFT_BUSY);
    raft_configuration_close(&configuration);

    return MUNIT_OK;
}
