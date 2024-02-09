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
    struct raft_change req;
};

static char *cluster_n[] = {"3", "4", "5", NULL};

static MunitParameterEnum _params[] = {
    {CLUSTER_N_PARAM, cluster_n},
    {NULL, NULL},
};

static void *setup(const MunitParameter params[], MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_CLUSTER(0);
    CLUSTER_BOOTSTRAP;
    CLUSTER_RANDOMIZE;
    CLUSTER_START;
    CLUSTER_STEP_UNTIL_HAS_LEADER(10000);
    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_CLUSTER;
    free(f);
}

/******************************************************************************
 *
 * Tests
 *
 *****************************************************************************/

SUITE(membership)

TEST(membership, addNonVoting, setup, tear_down, 0, _params)
{
    struct fixture *f = data;
    const struct raft_server *server;
    struct raft *raft;

    CLUSTER_ADD(&f->req);
    CLUSTER_STEP_UNTIL_APPLIED(CLUSTER_LEADER, 3, 2000);

    /* Then promote it. */
    CLUSTER_ASSIGN(&f->req, RAFT_STANDBY);

    CLUSTER_STEP_UNTIL_APPLIED(CLUSTER_N, 4, 2000);

    raft = CLUSTER_RAFT(CLUSTER_LEADER);

    server = &raft->configuration.servers[CLUSTER_N - 1];
    munit_assert_int(server->id, ==, CLUSTER_N);

    return MUNIT_OK;
}

TEST(membership, addVoting, setup, tear_down, 0, _params)
{
    struct fixture *f = data;
    const struct raft_server *server;
    struct raft *raft;

    (void)params;

    CLUSTER_ADD(&f->req);
    CLUSTER_STEP_UNTIL_APPLIED(CLUSTER_LEADER, 3, 2000);

    /* Then promote it. */
    CLUSTER_ASSIGN(&f->req, RAFT_VOTER);

    CLUSTER_STEP_UNTIL_APPLIED(CLUSTER_N, 4, 2000);

    raft = CLUSTER_RAFT(CLUSTER_LEADER);

    server = &raft->configuration.servers[CLUSTER_N - 1];
    munit_assert_int(server->role, ==, RAFT_VOTER);

    return MUNIT_OK;
}

TEST(membership, removeVoting, setup, tear_down, 0, _params)
{
    struct fixture *f = data;
    struct raft *raft;
    int rv;

    (void)params;

    raft = CLUSTER_RAFT(CLUSTER_LEADER);

    rv = raft_remove(raft, &f->req, CLUSTER_LEADER % CLUSTER_N + 1, NULL);
    munit_assert_int(rv, ==, 0);

    CLUSTER_STEP_UNTIL_APPLIED(CLUSTER_LEADER, 3, 2000);

    munit_assert_int(raft->configuration.n, ==, CLUSTER_N - 1);

    return 0;
}
