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
    const char *n_voting_param = munit_parameters_get(params, "n_voting");
    unsigned n = 3;
    unsigned n_voting = n;
    if (n_voting_param != NULL) {
        n_voting = atoi(n_voting_param);
    }
    SETUP_CLUSTER(n);
    CLUSTER_BOOTSTRAP_N_VOTING(n_voting);
    CLUSTER_START;
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
 * Assertions
 *
 *****************************************************************************/

/* Assert the current value of the timer of the I'th raft instance */
#define ASSERT_ELECTION_TIMER(I, MSECS)                                   \
    {                                                                     \
        struct raft *raft_ = CLUSTER_RAFT(I);                             \
        munit_assert_int(                                                 \
            raft_->io->time(raft_->io) - raft_->election_timer_start, ==, \
            MSECS);                                                       \
    }

/* Assert the current state of the I'th raft instance.  */
#define ASSERT_STATE(I, STATE) munit_assert_int(CLUSTER_STATE(I), ==, STATE);

/******************************************************************************
 *
 * Tick callback
 *
 *****************************************************************************/

SUITE(tick)

/* Internal timers are updated according to the given time delta. */
TEST(tick, electionTimer, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    (void)params;

    CLUSTER_STEP;
    ASSERT_ELECTION_TIMER(0, 100);

    CLUSTER_STEP;
    ASSERT_ELECTION_TIMER(1, 100);

    CLUSTER_STEP;
    ASSERT_ELECTION_TIMER(2, 100);

    CLUSTER_STEP;
    ASSERT_ELECTION_TIMER(0, 200);

    return MUNIT_OK;
}

/* If the election timeout expires, the follower is a voting server, and it
 * hasn't voted yet in this term, then become candidate and start a new
 * election. */
TEST(tick, candidate, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft *raft = CLUSTER_RAFT(0);
    (void)params;

    CLUSTER_STEP_UNTIL_ELAPSED(
        raft->follower_state.randomized_election_timeout);

    /* The term has been incremented. */
    munit_assert_int(raft->current_term, ==, 2);

    /* We have voted for ourselves. */
    munit_assert_int(raft->voted_for, ==, 1);

    /* We are candidate */
    ASSERT_STATE(0, RAFT_CANDIDATE);

    /* The votes array is initialized */
    munit_assert_ptr_not_null(raft->candidate_state.votes);
    munit_assert_true(raft->candidate_state.votes[0]);
    munit_assert_false(raft->candidate_state.votes[1]);

    return MUNIT_OK;
}

/* If the election timeout has not elapsed, stay follower. */
TEST(tick, electionTimerNotExpired, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft *raft = CLUSTER_RAFT(0);
    (void)params;

    CLUSTER_STEP_UNTIL_ELAPSED(
        raft->follower_state.randomized_election_timeout - 100);
    ASSERT_STATE(0, RAFT_FOLLOWER);

    return MUNIT_OK;
}

static char *elapse_non_voter_n_voting[] = {"1", NULL};

static MunitParameterEnum elapse_non_voter_params[] = {
    {"n_voting", elapse_non_voter_n_voting},
    {NULL, NULL},
};

/* If the election timeout has elapsed, but we're not voters, stay follower. */
TEST(tick, not_voter, setUp, tearDown, 0, elapse_non_voter_params)
{
    struct fixture *f = data;
    struct raft *raft = CLUSTER_RAFT(1);
    (void)params;

    /* Prevent the timer of the first server from expiring. */
    raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 2000);
    raft_set_election_timeout(CLUSTER_RAFT(0), 2000);

    CLUSTER_STEP_UNTIL_ELAPSED(
        raft->follower_state.randomized_election_timeout + 100);
    ASSERT_STATE(1, RAFT_FOLLOWER);

    return MUNIT_OK;
}

/* If we're leader election timeout elapses without hearing from a majority of
 * the cluster, step down. */
TEST(tick, no_contact, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    (void)params;

    CLUSTER_ELECT(0);
    CLUSTER_SATURATE_BOTHWAYS(0, 1);
    CLUSTER_SATURATE_BOTHWAYS(0, 2);

    /* Wait for the leader to step down. */
    CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_FOLLOWER, 2000);

    return MUNIT_OK;
}

/* If we're candidate and the election timeout has elapsed, start a new
 * election. */
TEST(tick, new_election, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft *raft = CLUSTER_RAFT(0);

    (void)params;

    CLUSTER_SATURATE_BOTHWAYS(0, 1);
    CLUSTER_SATURATE_BOTHWAYS(0, 2);

    /* Become candidate */
    CLUSTER_STEP_UNTIL_ELAPSED(
        raft->follower_state.randomized_election_timeout);

    /* Expire the election timeout */
    CLUSTER_STEP_UNTIL_ELAPSED(
        raft->candidate_state.randomized_election_timeout);

    /* The term has been incremented and saved to stable store. */
    munit_assert_int(raft->current_term, ==, 3);

    /* We have voted for ourselves. */
    munit_assert_int(raft->voted_for, ==, 1);

    /* We are still candidate */
    ASSERT_STATE(0, RAFT_CANDIDATE);

    /* The votes array is initialized */
    munit_assert_ptr_not_null(raft->candidate_state.votes);
    munit_assert_true(raft->candidate_state.votes[0]);
    munit_assert_false(raft->candidate_state.votes[1]);

    return MUNIT_OK;
}

/* If the election timeout has not elapsed, stay candidate. */
TEST(tick, during_election, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft *raft = CLUSTER_RAFT(0);
    (void)params;

    CLUSTER_SATURATE_BOTHWAYS(0, 1);
    CLUSTER_SATURATE_BOTHWAYS(0, 2);

    /* Become candidate */
    CLUSTER_STEP_UNTIL_ELAPSED(
        raft->follower_state.randomized_election_timeout);

    /* Make some time elapse, but not enough to trigger the timeout */
    CLUSTER_STEP_UNTIL_ELAPSED(
        raft->candidate_state.randomized_election_timeout - 100);

    /* We are still candidate at the same term */
    ASSERT_STATE(0, RAFT_CANDIDATE);
    munit_assert_int(raft->current_term, ==, 2);

    return MUNIT_OK;
}

static char *elapse_request_vote_only_to_voters_n_voting[] = {"2", NULL};

static MunitParameterEnum elapse_request_vote_only_to_voters_params[] = {
    {"n_voting", elapse_request_vote_only_to_voters_n_voting},
    {NULL, NULL},
};

/* Vote requests are sent only to voting servers. */
TEST(tick,
     request_vote_only_to_voters,
     setUp,
     tearDown,
     0,
     elapse_request_vote_only_to_voters_params)
{
    struct fixture *f = data;
    struct raft *raft = CLUSTER_RAFT(0);
    (void)params;

    CLUSTER_SATURATE_BOTHWAYS(0, 1);
    CLUSTER_SATURATE_BOTHWAYS(0, 2);

    /* Become candidate */
    CLUSTER_STEP_UNTIL_ELAPSED(
        raft->follower_state.randomized_election_timeout);

    /* We have sent vote requests only to the voting server */
    //__assert_request_vote(f, 2, 2, 1, 1);

    return MUNIT_OK;
}
