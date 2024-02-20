#include "../lib/cluster.h"
#include "../lib/runner.h"

/******************************************************************************
 *
 * Fixture holding a bootstrapped raft cluster.
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_CLUSTER;
};

static void *setUp(const MunitParameter params[], MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_CLUSTER(3);
    CLUSTER_BOOTSTRAP;
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
 * Recover tests.
 *
 *****************************************************************************/

SUITE(raft_recover)

/* Attempting to recover a running instance results in RAFT_BUSY. */
TEST(raft_recover, busy, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft *raft;
    struct raft_configuration configuration;
    int rv;

    /* Start all servers. */
    CLUSTER_START;

    raft = CLUSTER_RAFT(0);
    CLUSTER_CONFIGURATION(&configuration);
    rv = raft_recover(raft, &configuration);
    munit_assert_int(rv, ==, RAFT_BUSY);
    raft_configuration_close(&configuration);

    return MUNIT_OK;
}
