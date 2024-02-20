#include "../lib/cluster.h"
#include "../lib/runner.h"

/******************************************************************************
 *
 * Fixture
 *
 *****************************************************************************/

/* Maximum number of cluster loop iterations each test should perform. */
#define MAX_ITERATIONS 25000

/* Maximum number of cluster loop iterations a pair of servers should stay
 * disconnected. */
#define MAX_DISCONNECT 150

struct disconnection
{
    unsigned id1;
    unsigned id2;
    int start;
    int duration;
};

struct fixture
{
    FIXTURE_CLUSTER;
    struct disconnection *disconnections;
};

static char *cluster_n[] = {"3", "4", NULL};
static char *cluster_pre_vote[] = {"0", "1", NULL};

static MunitParameterEnum _params[] = {
    {CLUSTER_N_PARAM, cluster_n},
    {CLUSTER_PRE_VOTE_PARAM, cluster_pre_vote},
    {NULL, NULL},
};

/* Return the number of distinct server pairs in the cluster. */
static int __server_pairs(struct fixture *f)
{
    return CLUSTER_N * (CLUSTER_N - 1) / 2;
}

/* Update the cluster connectivity for the given iteration. */
static void __update_connectivity(struct fixture *f, int i)
{
    int p;
    int pairs = __server_pairs(f);

    for (p = 0; p < pairs; p++) {
        struct disconnection *disconnection = &f->disconnections[p];
        unsigned id1 = disconnection->id1;
        unsigned id2 = disconnection->id2;

        if (disconnection->start == 0) {
            /* Decide whether to disconnect this pair. */
            if (munit_rand_int_range(1, 10) <= 1) {
                disconnection->start = i;
                disconnection->duration =
                    munit_rand_int_range(50, MAX_DISCONNECT);
                raft_fixture_saturate(&f->cluster, id1 - 1, id2 - 1);
                raft_fixture_saturate(&f->cluster, id2 - 1, id1 - 1);
            }
        } else {
            /* Decide whether to reconnect this pair. */
            if (i - disconnection->start > disconnection->duration) {
                raft_fixture_desaturate(&f->cluster, id1 - 1, id2 - 1);
                raft_fixture_desaturate(&f->cluster, id2 - 1, id1 - 1);
                disconnection->start = 0;
            }
        }
    }
}

static void *setup(const MunitParameter params[], MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    int pairs;
    size_t i, j, k;
    SETUP_CLUSTER(0);
    CLUSTER_BOOTSTRAP;
    CLUSTER_RANDOMIZE;
    CLUSTER_START;

    /* Number of distinct pairs of servers. */
    pairs = __server_pairs(f);

    f->disconnections = munit_malloc(pairs * sizeof *f->disconnections);

    k = 0;
    for (i = 0; i < CLUSTER_N; i++) {
        for (j = i + 1; j < CLUSTER_N; j++) {
            struct disconnection *disconnection = &f->disconnections[k];
            disconnection->id1 = i + 1;
            disconnection->id2 = j + 1;
            disconnection->start = 0;
            disconnection->duration = 0;
            k++;
        }
    }

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_CLUSTER;
    free(f->disconnections);
    free(f);
}

/******************************************************************************
 *
 * Tests
 *
 *****************************************************************************/

SUITE(liveness)

static void apply_cb(struct raft_apply *req, int status, void *result)
{
    (void)status;
    (void)result;
    free(req);
}

/* The system makes progress even in case of network disruptions. */
TEST(liveness, networkDisconnect, setup, tear_down, 0, _params)
{
    struct fixture *f = data;
    int i = 0;

    (void)params;

    for (i = 0; i < MAX_ITERATIONS; i++) {
        __update_connectivity(f, i);
        raft_fixture_step(&f->cluster);

        if (CLUSTER_LEADER != CLUSTER_N) {
            struct raft_apply *req = munit_malloc(sizeof *req);
            CLUSTER_APPLY_ADD_X(CLUSTER_LEADER, req, 1, apply_cb);
            if (CLUSTER_LAST_APPLIED(CLUSTER_LEADER) >= 2) {
                break;
            }
        }
    }

    // munit_assert_int(CLUSTER_LAST_APPLIED(CLUSTER_LEADER), >=, 2);

    return MUNIT_OK;
}
