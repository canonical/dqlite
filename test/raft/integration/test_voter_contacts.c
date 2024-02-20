#include "../lib/cluster.h"
#include "../lib/runner.h"

#define N_SERVERS 3

/******************************************************************************
 *
 * Fixture with a test raft cluster.
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_CLUSTER;
};

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

#define STEP_N(N) raft_fixture_step_n(&f->cluster, N)

/******************************************************************************
 *
 * Set up a cluster with a three servers.
 *
 *****************************************************************************/

static void *setUp(const MunitParameter params[], MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_CLUSTER(N_SERVERS);
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
 * raft_voter_contacts
 *
 *****************************************************************************/

SUITE(raft_voter_contacts)

TEST(raft_voter_contacts, upToDate, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    CLUSTER_STEP_UNTIL_HAS_LEADER(1000);
    CLUSTER_STEP_N(1000);

    /* N node cluster with leader */
    for (unsigned int i = 0; i < N_SERVERS; i++) {
        int count = raft_voter_contacts(CLUSTER_RAFT(i));
        if (i == CLUSTER_LEADER) {
            munit_assert_int(count, ==, N_SERVERS);
        } else {
            munit_assert_int(count, ==, -1);
        }
    }

    /* Kill the cluster leader, so a new leader is elected and the number of
     * voters should be decreased */
    unsigned int leader = CLUSTER_LEADER;
    CLUSTER_KILL(leader);
    CLUSTER_STEP_UNTIL_HAS_LEADER(1000);
    CLUSTER_STEP_N(1000);

    for (unsigned int i = 0; i < N_SERVERS; i++) {
        if (i == leader) {
            continue;
        }
        int count = raft_voter_contacts(CLUSTER_RAFT(i));
        if (i == CLUSTER_LEADER) {
            munit_assert_int(count, ==, N_SERVERS - 1);
        } else {
            munit_assert_int(count, ==, -1);
        }
    }

    /* Revive the old leader, so the count should go back up */
    CLUSTER_REVIVE(leader);
    CLUSTER_STEP_N(1000);
    for (unsigned int i = 0; i < N_SERVERS; i++) {
        int count = raft_voter_contacts(CLUSTER_RAFT(i));
        if (i == CLUSTER_LEADER) {
            munit_assert_int(count, ==, N_SERVERS);
        } else {
            munit_assert_int(count, ==, -1);
        }
    }

    return MUNIT_OK;
}
