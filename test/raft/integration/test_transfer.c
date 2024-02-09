#include "../lib/cluster.h"
#include "../lib/runner.h"

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

static void transferCb(struct raft_transfer *req)
{
    bool *done = req->data;
    munit_assert_false(*done);
    *done = true;
}

static bool transferCbHasFired(struct raft_fixture *f, void *arg)
{
    bool *done = arg;
    (void)f;
    return *done;
}

/* Submit a transfer leadership request against the I'th server. */
#define TRANSFER_SUBMIT(I, ID)                         \
    struct raft *_raft = CLUSTER_RAFT(I);              \
    struct raft_transfer _req;                         \
    bool _done = false;                                \
    int _rv;                                           \
    _req.data = &_done;                                \
    _rv = raft_transfer(_raft, &_req, ID, transferCb); \
    munit_assert_int(_rv, ==, 0);

/* Wait until the transfer leadership request completes. */
#define TRANSFER_WAIT CLUSTER_STEP_UNTIL(transferCbHasFired, &_done, 2000)

/* Submit a transfer leadership request and wait for it to complete. */
#define TRANSFER(I, ID)         \
    do {                        \
        TRANSFER_SUBMIT(I, ID); \
        TRANSFER_WAIT;          \
    } while (0)

/* Submit a transfer leadership request against the I'th server and assert that
 * the given error is returned. */
#define TRANSFER_ERROR(I, ID, RV, ERRMSG)                        \
    do {                                                         \
        struct raft_transfer __req;                              \
        int __rv;                                                \
        __rv = raft_transfer(CLUSTER_RAFT(I), &__req, ID, NULL); \
        munit_assert_int(__rv, ==, RV);                          \
        munit_assert_string_equal(CLUSTER_ERRMSG(I), ERRMSG);    \
    } while (0)

/******************************************************************************
 *
 * Set up a cluster with a three servers.
 *
 *****************************************************************************/

static void *setUp(const MunitParameter params[], MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_CLUSTER(3);
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
 * raft_transfer
 *
 *****************************************************************************/

SUITE(raft_transfer)

/* The follower we ask to transfer leadership to is up-to-date. */
TEST(raft_transfer, upToDate, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    TRANSFER(0, 2);
    CLUSTER_STEP_UNTIL_HAS_LEADER(1000);
    munit_assert_int(CLUSTER_LEADER, ==, 1);
    return MUNIT_OK;
}

/* The follower we ask to transfer leadership to needs to catch up. */
TEST(raft_transfer, catchUp, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_apply req;
    CLUSTER_APPLY_ADD_X(CLUSTER_LEADER, &req, 1, NULL);
    TRANSFER(0, 2);
    CLUSTER_STEP_UNTIL_HAS_LEADER(1000);
    munit_assert_int(CLUSTER_LEADER, ==, 1);
    return MUNIT_OK;
}

/* The follower we ask to transfer leadership to is down and the leadership
 * transfer does not succeed. */
TEST(raft_transfer, expire, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_apply req;
    CLUSTER_APPLY_ADD_X(CLUSTER_LEADER, &req, 1, NULL);
    CLUSTER_KILL(1);
    TRANSFER(0, 2);
    munit_assert_int(CLUSTER_LEADER, ==, 0);
    return MUNIT_OK;
}

/* The given ID doesn't match any server in the current configuration. */
TEST(raft_transfer, unknownServer, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    TRANSFER_ERROR(0, 4, RAFT_BADID, "server ID is not valid");
    return MUNIT_OK;
}

/* Submitting a transfer request twice is an error. */
TEST(raft_transfer, twice, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    TRANSFER_SUBMIT(0, 2);
    TRANSFER_ERROR(0, 3, RAFT_NOTLEADER, "server is not the leader");
    TRANSFER_WAIT;
    return MUNIT_OK;
}

/* If the given ID is zero, the target is selected automatically. */
TEST(raft_transfer, autoSelect, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    TRANSFER(0, 0);
    CLUSTER_STEP_UNTIL_HAS_LEADER(1000);
    munit_assert_int(CLUSTER_LEADER, !=, 0);
    return MUNIT_OK;
}

/* If the given ID is zero, the target is selected automatically. Followers that
 * are up-to-date are preferred. */
TEST(raft_transfer, autoSelectUpToDate, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_KILL(1);
    CLUSTER_MAKE_PROGRESS;
    TRANSFER(0, 0);
    CLUSTER_STEP_UNTIL_HAS_LEADER(1000);
    munit_assert_int(CLUSTER_LEADER, ==, 2);
    return MUNIT_OK;
}

/* It's not possible to transfer leadership after the server has been
 * demoted. */
TEST(raft_transfer, afterDemotion, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_change req;
    struct raft *raft = CLUSTER_RAFT(0);
    int rv;
    CLUSTER_ADD(&req);
    CLUSTER_STEP_UNTIL_APPLIED(0, 3, 1000);
    CLUSTER_ASSIGN(&req, RAFT_VOTER);
    CLUSTER_STEP_UNTIL_APPLIED(0, 4, 1000);
    rv = raft_assign(raft, &req, raft->id, RAFT_SPARE, NULL);
    munit_assert_int(rv, ==, 0);
    CLUSTER_STEP_UNTIL_APPLIED(0, 5, 1000);
    TRANSFER_ERROR(0, 2, RAFT_NOTLEADER, "server is not the leader");
    return MUNIT_OK;
}

static char *cluster_pre_vote[] = {"0", "1", NULL};
static char *cluster_heartbeat[] = {"1", "100", NULL};

static MunitParameterEnum _params[] = {
    {CLUSTER_PRE_VOTE_PARAM, cluster_pre_vote},
    {CLUSTER_HEARTBEAT_PARAM, cluster_heartbeat},
    {NULL, NULL},
};

/* It's possible to transfer leadership also when pre-vote is active */
TEST(raft_transfer, preVote, setUp, tearDown, 0, _params)
{
    struct fixture *f = data;
    TRANSFER(0, 2);
    CLUSTER_STEP_UNTIL_HAS_LEADER(1000);
    munit_assert_int(CLUSTER_LEADER, ==, 1);
    return MUNIT_OK;
}
