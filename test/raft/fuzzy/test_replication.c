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

static char *cluster_n[] = {"3", "5", "7", NULL};

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
 * Helper macros
 *
 *****************************************************************************/

#define APPLY_ADD_ONE(REQ) CLUSTER_APPLY_ADD_X(CLUSTER_LEADER, REQ, 1, NULL)

/******************************************************************************
 *
 * Tests
 *
 *****************************************************************************/

SUITE(replication)

/* New entries on the leader are eventually replicated to followers. */
TEST(replication, appendEntries, setup, tear_down, 0, _params)
{
    struct fixture *f = data;
    struct raft_apply *req = munit_malloc(sizeof *req);
    (void)params;
    APPLY_ADD_ONE(req);
    CLUSTER_STEP_UNTIL_APPLIED(CLUSTER_N, 3, 2000);
    free(req);
    return MUNIT_OK;
}

/* The cluster remains available even if the current leader dies and a new
 * leader gets elected. */
TEST(replication, availability, setup, tear_down, 0, _params)
{
    struct fixture *f = data;
    struct raft_apply *req1 = munit_malloc(sizeof *req1);
    struct raft_apply *req2 = munit_malloc(sizeof *req2);

    (void)params;

    APPLY_ADD_ONE(req1);
    CLUSTER_STEP_UNTIL_APPLIED(CLUSTER_N, 3, 2000);

    CLUSTER_KILL_LEADER;
    CLUSTER_STEP_UNTIL_HAS_NO_LEADER(10000);
    CLUSTER_STEP_UNTIL_HAS_LEADER(10000);

    APPLY_ADD_ONE(req2);
    /* Index 3 -> 5 = APPLY entry + BARRIER entry after becoming leader */
    CLUSTER_STEP_UNTIL_APPLIED(CLUSTER_LEADER, 5, 2000);

    free(req1);
    free(req2);

    return MUNIT_OK;
}

static void apply_cb(struct raft_apply *req, int status, void *result)
{
    (void)status;
    (void)result;
    free(req);
}

/* If no quorum is available, entries don't get committed. */
TEST(replication, noQuorum, setup, tear_down, 0, _params)
{
    struct fixture *f = data;
    struct raft_apply *req = munit_malloc(sizeof *req);
    unsigned i;

    (void)params;

    CLUSTER_APPLY_ADD_X(CLUSTER_LEADER, req, 1, apply_cb);
    CLUSTER_KILL_MAJORITY;

    CLUSTER_STEP_UNTIL_ELAPSED(10000);

    for (i = 0; i < CLUSTER_N; i++) {
        munit_assert_int(CLUSTER_LAST_APPLIED(i), ==, 1);
    }

    return MUNIT_OK;
}

/* If the cluster is partitioned, entries don't get committed. */
TEST(replication, partitioned, setup, tear_down, 0, _params)
{
    struct fixture *f = data;
    struct raft_apply *req1 = munit_malloc(sizeof *req1);
    struct raft_apply *req2 = munit_malloc(sizeof *req2);
    unsigned leader_id;
    size_t i;
    size_t n;

    (void)params;

    leader_id = CLUSTER_LEADER + 1;

    /* Disconnect the leader from a majority of servers */
    n = 0;
    for (i = 0; n < (CLUSTER_N / 2) + 1; i++) {
        struct raft *raft = CLUSTER_RAFT(i);
        if (raft->id == leader_id) {
            continue;
        }
        raft_fixture_saturate(&f->cluster, leader_id - 1, raft->id - 1);
        raft_fixture_saturate(&f->cluster, raft->id - 1, leader_id - 1);
        n++;
    }

    /* Try to append a new entry using the disconnected leader. */
    CLUSTER_APPLY_ADD_X(CLUSTER_LEADER, req1, 1, apply_cb);

    /* The leader gets deposed. */
    CLUSTER_STEP_UNTIL_HAS_NO_LEADER(10000);

    /* The entry does not get committed. */
    CLUSTER_STEP_UNTIL_ELAPSED(5000);

    /* Reconnect the old leader */
    for (i = 0; i < CLUSTER_N; i++) {
        struct raft *raft = CLUSTER_RAFT(i);
        if (raft->id == leader_id) {
            continue;
        }
        raft_fixture_desaturate(&f->cluster, leader_id - 1, raft->id - 1);
    }

    // TODO this fails with seed 0x3914306f
    CLUSTER_STEP_UNTIL_HAS_LEADER(30000);

    /* Re-try now to append the entry. */
    CLUSTER_APPLY_ADD_X(CLUSTER_LEADER, req2, 1, apply_cb);
    CLUSTER_STEP_UNTIL_APPLIED(CLUSTER_LEADER, 2, 10000);

    return MUNIT_OK;
}
