#include "../lib/cluster.h"
#include "../lib/runner.h"

/******************************************************************************
 *
 * Fixture with a fake raft_io instance.
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

/* Bootstrap the I'th server. */
#define BOOTSTRAP(I)                                  \
    do {                                              \
        struct raft_configuration _configuration;     \
        int _rv;                                      \
        struct raft *_raft;                           \
        CLUSTER_CONFIGURATION(&_configuration);       \
        _raft = CLUSTER_RAFT(I);                      \
        _rv = raft_bootstrap(_raft, &_configuration); \
        munit_assert_int(_rv, ==, 0);                 \
        raft_configuration_close(&_configuration);    \
    } while (0)

/******************************************************************************
 *
 * Set up a cluster with a single server.
 *
 *****************************************************************************/

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
 * raft_start
 *
 *****************************************************************************/

SUITE(raft_start)

/* There are two servers. The first has a snapshot present and no other
 * entries. */
TEST(raft_start, oneSnapshotAndNoEntries, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW;
    CLUSTER_SET_SNAPSHOT(0 /* server index                                  */,
                         6 /* last index                                    */,
                         2 /* last term                                     */,
                         1 /* conf index                                    */,
                         5 /* x                                             */,
                         7 /* y                                             */);
    CLUSTER_SET_TERM(0, 2);
    BOOTSTRAP(1);
    CLUSTER_START;
    CLUSTER_MAKE_PROGRESS;
    return MUNIT_OK;
}

/* There are two servers. The first has a snapshot along with some follow-up
 * entries. */
TEST(raft_start, oneSnapshotAndSomeFollowUpEntries, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry entries[2];
    struct raft_fsm *fsm;

    CLUSTER_GROW;
    BOOTSTRAP(1);

    entries[0].type = RAFT_COMMAND;
    entries[0].term = 2;
    FsmEncodeSetX(6, &entries[0].buf);

    entries[1].type = RAFT_COMMAND;
    entries[1].term = 2;
    FsmEncodeAddY(2, &entries[1].buf);

    CLUSTER_SET_SNAPSHOT(0 /*                                               */,
                         6 /* last index                                    */,
                         2 /* last term                                     */,
                         1 /* conf index                                    */,
                         5 /* x                                             */,
                         7 /* y                                             */);
    CLUSTER_ADD_ENTRY(0, &entries[0]);
    CLUSTER_ADD_ENTRY(1, &entries[1]);
    CLUSTER_SET_TERM(0, 2);

    CLUSTER_START;
    CLUSTER_MAKE_PROGRESS;

    fsm = CLUSTER_FSM(0);
    munit_assert_int(FsmGetX(fsm), ==, 7);

    return MUNIT_OK;
}

/******************************************************************************
 *
 * Start with entries present on disk.
 *
 *****************************************************************************/

/* There are 3 servers. The first has no entries are present at all */
TEST(raft_start, noEntries, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW;
    CLUSTER_GROW;
    BOOTSTRAP(1);
    BOOTSTRAP(2);
    CLUSTER_START;
    CLUSTER_MAKE_PROGRESS;
    return MUNIT_OK;
}

/* There are 3 servers, the first has some entries, the others don't. */
TEST(raft_start, twoEntries, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_configuration configuration;
    struct raft_entry entry;
    struct raft_fsm *fsm;
    unsigned i;
    int rv;

    CLUSTER_GROW;
    CLUSTER_GROW;

    CLUSTER_CONFIGURATION(&configuration);
    rv = raft_bootstrap(CLUSTER_RAFT(0), &configuration);
    munit_assert_int(rv, ==, 0);
    raft_configuration_close(&configuration);

    entry.type = RAFT_COMMAND;
    entry.term = 3;
    FsmEncodeSetX(123, &entry.buf);

    CLUSTER_ADD_ENTRY(0, &entry);
    CLUSTER_SET_TERM(0, 3);

    BOOTSTRAP(1);
    BOOTSTRAP(2);

    CLUSTER_START;
    CLUSTER_ELECT(0);
    CLUSTER_MAKE_PROGRESS;

    CLUSTER_STEP_UNTIL_APPLIED(CLUSTER_N, 3, 3000);

    for (i = 0; i < CLUSTER_N; i++) {
        fsm = CLUSTER_FSM(i);
        munit_assert_int(FsmGetX(fsm), ==, 124);
    }

    return MUNIT_OK;
}

/* There is a single voting server in the cluster, which immediately elects
 * itself when starting. */
TEST(raft_start, singleVotingSelfElect, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_BOOTSTRAP;
    CLUSTER_START;
    munit_assert_int(CLUSTER_STATE(0), ==, RAFT_LEADER);
    CLUSTER_MAKE_PROGRESS;
    return MUNIT_OK;
}

/* There are two servers in the cluster, one is voting and the other is
 * not. When started, the non-voting server does not elects itself. */
TEST(raft_start, singleVotingNotUs, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW;
    CLUSTER_BOOTSTRAP_N_VOTING(1);
    CLUSTER_START;
    munit_assert_int(CLUSTER_STATE(1), ==, RAFT_FOLLOWER);
    CLUSTER_MAKE_PROGRESS;
    return MUNIT_OK;
}

static void state_cb(struct raft *r, unsigned short old, unsigned short new)
{
    munit_assert_ushort(old, !=, new);
    r->data = (void *)(uintptr_t)0xFEEDBEEF;
}

/* There is a single voting server in the cluster, register a state_cb and
 * assert that it's called because the node will progress to leader.  */
TEST(raft_start, singleVotingWithStateCb, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_BOOTSTRAP;
    struct raft *r = CLUSTER_RAFT(0);
    r->data = (void *)(uintptr_t)0;
    raft_register_state_cb(r, state_cb);
    CLUSTER_START;
    munit_assert_uint((uintptr_t)r->data, ==, 0xFEEDBEEF);
    return MUNIT_OK;
}
