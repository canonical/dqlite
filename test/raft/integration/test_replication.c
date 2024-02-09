#include "../../../src/raft/configuration.h"
#include "../../../src/raft/flags.h"
#include "../../../src/raft/progress.h"
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

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

/* Standard startup sequence, bootstrapping the cluster and electing server 0 */
#define BOOTSTRAP_START_AND_ELECT \
    CLUSTER_BOOTSTRAP;            \
    CLUSTER_START;                \
    CLUSTER_ELECT(0);             \
    ASSERT_TIME(1045)

/******************************************************************************
 *
 * Set up a cluster with a two servers.
 *
 *****************************************************************************/

static void *setUp(const MunitParameter params[], MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_CLUSTER(2);
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

/* Assert that the I'th server is in follower state. */
#define ASSERT_FOLLOWER(I) munit_assert_int(CLUSTER_STATE(I), ==, RAFT_FOLLOWER)

/* Assert that the I'th server is in candidate state. */
#define ASSERT_CANDIDATE(I) \
    munit_assert_int(CLUSTER_STATE(I), ==, RAFT_CANDIDATE)

/* Assert that the I'th server is in leader state. */
#define ASSERT_LEADER(I) munit_assert_int(CLUSTER_STATE(I), ==, RAFT_LEADER)

/* Assert that the fixture time matches the given value */
#define ASSERT_TIME(TIME) munit_assert_int(CLUSTER_TIME, ==, TIME)

/* Assert that the configuration of the I'th server matches the given one */
#define ASSERT_CONFIGURATION(I, EXPECTED)                                    \
    do {                                                                     \
        struct raft *_raft = CLUSTER_RAFT(I);                                \
        struct raft_configuration *_actual = &_raft->configuration;          \
        unsigned _i;                                                         \
                                                                             \
        munit_assert_uint(_actual->n, ==, (EXPECTED)->n);                    \
        for (_i = 0; _i < _actual->n; _i++) {                                \
            struct raft_server *_server1 = &_actual->servers[_i];            \
            struct raft_server *_server2 = &(EXPECTED)->servers[_i];         \
            munit_assert_ulong(_server1->id, ==, _server2->id);              \
            munit_assert_int(_server1->role, ==, _server2->role);            \
            munit_assert_string_equal(_server1->address, _server2->address); \
        }                                                                    \
    } while (0)

/******************************************************************************
 *
 * Log replication.
 *
 *****************************************************************************/

SUITE(replication)

/* A leader sends a heartbeat message as soon as it gets elected. */
TEST(replication, sendInitialHeartbeat, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft *raft;
    CLUSTER_BOOTSTRAP;
    CLUSTER_START;

    /* Server 0 becomes candidate and sends vote requests after the election
     * timeout. */
    CLUSTER_STEP_N(19);
    ASSERT_TIME(1000);
    ASSERT_CANDIDATE(0);

    /* Server 0 receives the vote result, becomes leader and sends
     * heartbeats. */
    CLUSTER_STEP_N(6);
    ASSERT_LEADER(0);
    ASSERT_TIME(1030);
    raft = CLUSTER_RAFT(0);
    munit_assert_int(raft->leader_state.progress[1].last_send, ==, 1030);

    /* Server 1 receives the heartbeat from server 0 and resets its election
     * timer. */
    raft = CLUSTER_RAFT(1);
    munit_assert_int(raft->election_timer_start, ==, 1015);
    CLUSTER_STEP_N(2);
    munit_assert_int(raft->election_timer_start, ==, 1045);

    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_APPEND_ENTRIES), ==, 1);
    munit_assert_int(CLUSTER_N_RECV(1, RAFT_IO_APPEND_ENTRIES), ==, 1);

    return MUNIT_OK;
}

/* After receiving an AppendEntriesResult, a leader has set the feature flags of
 * a node. */
TEST(replication, receiveFlags, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft *raft;
    CLUSTER_BOOTSTRAP;
    CLUSTER_START;

    /* Server 0 becomes leader and sends the initial heartbeat. */
    CLUSTER_STEP_N(24);
    ASSERT_LEADER(0);
    ASSERT_TIME(1030);

    /* Flags is empty */
    raft = CLUSTER_RAFT(0);
    munit_assert_ullong(raft->leader_state.progress[1].features, ==, 0);

    raft = CLUSTER_RAFT(1);
    /* Server 1 receives the first heartbeat. */
    CLUSTER_STEP_N(4);
    munit_assert_int(raft->election_timer_start, ==, 1045);
    munit_assert_int(CLUSTER_N_RECV(1, RAFT_IO_APPEND_ENTRIES), ==, 1);

    /* Server 0 receives the reply to the heartbeat. */
    CLUSTER_STEP_N(2);
    munit_assert_int(CLUSTER_N_RECV(0, RAFT_IO_APPEND_ENTRIES_RESULT), ==, 1);
    raft = CLUSTER_RAFT(0);
    munit_assert_ullong(raft->leader_state.progress[1].features, ==,
                        RAFT_DEFAULT_FEATURE_FLAGS);

    return MUNIT_OK;
}

/* A leader keeps sending heartbeat messages at regular intervals to
 * maintain leadership. */
TEST(replication, sendFollowupHeartbeat, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft *raft;
    CLUSTER_BOOTSTRAP;
    CLUSTER_START;

    /* Server 0 becomes leader and sends the initial heartbeat. */
    CLUSTER_STEP_N(24);
    ASSERT_LEADER(0);
    ASSERT_TIME(1030);

    raft = CLUSTER_RAFT(1);

    /* Server 1 receives the first heartbeat. */
    CLUSTER_STEP_N(4);
    munit_assert_int(raft->election_timer_start, ==, 1045);
    munit_assert_int(CLUSTER_N_RECV(1, RAFT_IO_APPEND_ENTRIES), ==, 1);

    /* Server 1 receives the second heartbeat. */
    CLUSTER_STEP_N(8);
    munit_assert_int(raft->election_timer_start, ==, 1215);
    munit_assert_int(CLUSTER_N_RECV(1, RAFT_IO_APPEND_ENTRIES), ==, 2);

    /* Server 1 receives the third heartbeat. */
    CLUSTER_STEP_N(7);
    munit_assert_int(raft->election_timer_start, ==, 1315);
    munit_assert_int(CLUSTER_N_RECV(1, RAFT_IO_APPEND_ENTRIES), ==, 3);

    /* Server 1 receives the fourth heartbeat. */
    CLUSTER_STEP_N(7);
    munit_assert_int(raft->election_timer_start, ==, 1415);

    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_APPEND_ENTRIES), ==, 4);
    munit_assert_int(CLUSTER_N_RECV(0, RAFT_IO_APPEND_ENTRIES_RESULT), ==, 4);
    munit_assert_int(CLUSTER_N_RECV(1, RAFT_IO_APPEND_ENTRIES), ==, 4);
    munit_assert_int(CLUSTER_N_SEND(1, RAFT_IO_APPEND_ENTRIES_RESULT), ==, 4);

    return MUNIT_OK;
}

/* If a leader replicates some entries during a given heartbeat interval, it
 * skips sending the heartbeat for that interval. */
TEST(replication, sendSkipHeartbeat, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft *raft;
    struct raft_apply req;
    CLUSTER_BOOTSTRAP;
    CLUSTER_START;

    raft = CLUSTER_RAFT(0);

    /* Server 0 becomes leader and sends the first two heartbeats. */
    CLUSTER_STEP_UNTIL_ELAPSED(1215);
    ASSERT_LEADER(0);
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_APPEND_ENTRIES), ==, 2);
    munit_assert_int(CLUSTER_N_RECV(1, RAFT_IO_APPEND_ENTRIES), ==, 2);

    /* Server 0 starts replicating a new entry after 15 milliseconds. */
    CLUSTER_STEP_UNTIL_ELAPSED(15);
    ASSERT_TIME(1230);
    CLUSTER_APPLY_ADD_X(0, &req, 1, NULL);
    CLUSTER_STEP_N(1);
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_APPEND_ENTRIES), ==, 3);
    munit_assert_int(raft->leader_state.progress[1].last_send, ==, 1230);

    /* When the heartbeat timeout expires, server 0 does not send an empty
     * append entries. */
    CLUSTER_STEP_UNTIL_ELAPSED(70);
    ASSERT_TIME(1300);
    CLUSTER_STEP_N(1);
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_APPEND_ENTRIES), ==, 3);
    munit_assert_int(raft->leader_state.progress[1].last_send, ==, 1230);

    return MUNIT_OK;
}

/* The leader doesn't send replication messages to idle servers. */
TEST(replication, skipIdle, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_change req1;
    struct raft_apply req2;
    BOOTSTRAP_START_AND_ELECT;
    CLUSTER_ADD(&req1);
    CLUSTER_STEP_UNTIL_APPLIED(0, 3, 1000);
    CLUSTER_APPLY_ADD_X(CLUSTER_LEADER, &req2, 1, NULL);
    CLUSTER_STEP_UNTIL_ELAPSED(1000);
    munit_assert_int(CLUSTER_LAST_APPLIED(0), ==, 4);
    munit_assert_int(CLUSTER_LAST_APPLIED(1), ==, 4);
    munit_assert_int(CLUSTER_LAST_APPLIED(2), ==, 0);
    return MUNIT_OK;
}

/* A follower remains in probe mode until the leader receives a successful
 * AppendEntries response. */
TEST(replication, sendProbe, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_apply req1;
    struct raft_apply req2;
    CLUSTER_BOOTSTRAP;
    CLUSTER_START;

    /* Server 0 becomes leader and sends the initial heartbeat. */
    CLUSTER_STEP_N(25);
    ASSERT_LEADER(0);
    ASSERT_TIME(1030);
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_APPEND_ENTRIES), ==, 1);

    /* Set a very high network latency for server 1, so server 0 will send a
     * second probe AppendEntries without transitioning to pipeline mode. */
    munit_assert_int(CLUSTER_N_RECV(1, RAFT_IO_APPEND_ENTRIES), ==, 0);
    CLUSTER_SET_NETWORK_LATENCY(1, 250);

    /* Server 0 receives a new entry after 15 milliseconds. Since the follower
     * is still in probe mode and since an AppendEntries message was already
     * sent recently, it does not send the new entry immediately. */
    CLUSTER_STEP_UNTIL_ELAPSED(15);
    CLUSTER_APPLY_ADD_X(0, &req1, 1, NULL);
    CLUSTER_STEP;
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_APPEND_ENTRIES), ==, 1);

    /* A heartbeat timeout elapses without receiving a response, so server 0
     * sends an new AppendEntries to server 1. */
    CLUSTER_STEP_UNTIL_ELAPSED(85);
    CLUSTER_STEP;
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_APPEND_ENTRIES), ==, 2);

    /* Server 0 receives a second entry after 15 milliseconds. Since the
     * follower is still in probe mode and since an AppendEntries message was
     * already sent recently, it does not send the new entry immediately. */
    CLUSTER_STEP_UNTIL_ELAPSED(15);
    CLUSTER_APPLY_ADD_X(0, &req2, 1, NULL);
    CLUSTER_STEP;
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_APPEND_ENTRIES), ==, 2);

    /* Eventually server 0 receives AppendEntries results for both entries. */
    CLUSTER_STEP_UNTIL_APPLIED(0, 4, 1000);

    return MUNIT_OK;
}

static bool indices_updated(struct raft_fixture *f, void *data)
{
    (void)f;
    const struct raft *r = data;
    return r->last_stored == 4 && r->leader_state.progress[1].match_index == 3;
}

/* A follower transitions to pipeline mode after the leader receives a
 * successful AppendEntries response from it. */
TEST(replication, sendPipeline, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft *raft;
    struct raft_apply req1;
    struct raft_apply req2;
    CLUSTER_BOOTSTRAP;
    CLUSTER_START;

    raft = CLUSTER_RAFT(0);

    /* Server 0 becomes leader and sends the initial heartbeat, receiving a
     * successful response. */
    CLUSTER_STEP_UNTIL_ELAPSED(1070);
    ASSERT_LEADER(0);
    ASSERT_TIME(1070);
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_APPEND_ENTRIES), ==, 1);

    /* Server 0 receives a new entry after 15 milliseconds. Since the follower
     * has transitioned to pipeline mode the new entry is sent immediately and
     * the next index is optimistically increased. */
    CLUSTER_STEP_UNTIL_ELAPSED(15);
    CLUSTER_APPLY_ADD_X(0, &req1, 1, NULL);
    CLUSTER_STEP;
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_APPEND_ENTRIES), ==, 2);
    munit_assert_int(raft->leader_state.progress[1].next_index, ==, 4);

    /* After another 15 milliseconds server 0 receives a second apply request,
     * which is also sent out immediately */
    CLUSTER_STEP_UNTIL_ELAPSED(15);
    CLUSTER_APPLY_ADD_X(0, &req2, 1, NULL);
    CLUSTER_STEP;
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_APPEND_ENTRIES), ==, 3);
    munit_assert_int(raft->leader_state.progress[1].next_index, ==, 5);

    /* Wait until the leader has stored entry 4 and the follower has matched
     * entry 3. Expect the commit index to have been updated to 3. */
    CLUSTER_STEP_UNTIL(indices_updated, CLUSTER_RAFT(0), 2000);
    munit_assert_ulong(raft->commit_index, ==, 3);

    /* Eventually server 0 receives AppendEntries results for both entries. */
    CLUSTER_STEP_UNTIL_APPLIED(0, 4, 1000);

    return MUNIT_OK;
}

/* A follower disconnects while in probe mode. */
TEST(replication, sendDisconnect, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_BOOTSTRAP;
    CLUSTER_START;

    /* Server 0 becomes leader and sends the initial heartbeat, however they
     * fail because server 1 has disconnected. */
    CLUSTER_STEP_N(24);
    ASSERT_LEADER(0);
    CLUSTER_DISCONNECT(0, 1);
    CLUSTER_STEP;
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_APPEND_ENTRIES), ==, 0);

    /* After the heartbeat timeout server 0 retries, but still fails. */
    CLUSTER_STEP_UNTIL_ELAPSED(100);
    CLUSTER_STEP;
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_APPEND_ENTRIES), ==, 0);

    /* After another heartbeat timeout server 0 retries and this time
     * succeeds. */
    CLUSTER_STEP_UNTIL_ELAPSED(100);
    CLUSTER_RECONNECT(0, 1);
    CLUSTER_STEP;
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_APPEND_ENTRIES), ==, 1);

    return MUNIT_OK;
}

/* A follower disconnects while in pipeline mode. */
TEST(replication, sendDisconnectPipeline, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_apply req1;
    struct raft_apply req2;
    CLUSTER_BOOTSTRAP;
    CLUSTER_START;

    /* Server 0 becomes leader and sends a couple of heartbeats. */
    CLUSTER_STEP_UNTIL_ELAPSED(1215);
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_APPEND_ENTRIES), ==, 2);

    /* It then starts to replicate a few entries, however the follower
     * disconnects before delivering results. */
    CLUSTER_APPLY_ADD_X(0, &req1, 1, NULL);
    CLUSTER_STEP;
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_APPEND_ENTRIES), ==, 3);
    CLUSTER_APPLY_ADD_X(0, &req2, 1, NULL);
    CLUSTER_STEP;
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_APPEND_ENTRIES), ==, 4);

    CLUSTER_DISCONNECT(0, 1);

    /* The next heartbeat fails, transitioning the follower back to probe
     * mode. */
    CLUSTER_STEP_UNTIL_ELAPSED(115);
    munit_assert_int(CLUSTER_N_SEND(0, RAFT_IO_APPEND_ENTRIES), ==, 4);

    /* After reconnection the follower eventually replicates the entries and
     * reports back. */
    CLUSTER_RECONNECT(0, 1);

    CLUSTER_STEP_UNTIL_APPLIED(0, 3, 1000);

    return MUNIT_OK;
}

static char *send_oom_heap_fault_delay[] = {"5", NULL};
static char *send_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum send_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, send_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, send_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory failures. */
TEST(replication, sendOom, setUp, tearDown, 0, send_oom_params)
{
    struct fixture *f = data;
    return MUNIT_SKIP;
    struct raft_apply req;
    BOOTSTRAP_START_AND_ELECT;

    HEAP_FAULT_ENABLE;

    CLUSTER_APPLY_ADD_X(0, &req, 1, NULL);
    CLUSTER_STEP;

    return MUNIT_OK;
}

/* A failure occurs upon submitting the I/O request. */
TEST(replication, persistError, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_apply req;
    BOOTSTRAP_START_AND_ELECT;

    raft_fixture_append_fault(&f->cluster, 0, 0);

    CLUSTER_APPLY_ADD_X(0, &req, 1, NULL);
    CLUSTER_STEP;

    return MUNIT_OK;
}

/* Receive the same entry a second time, before the first has been persisted. */
TEST(replication, recvTwice, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_apply *req = munit_malloc(sizeof *req);
    BOOTSTRAP_START_AND_ELECT;

    CLUSTER_APPLY_ADD_X(CLUSTER_LEADER, req, 1, NULL);

    /* Set a high disk latency for server 1, so server 0 won't receive an
     * AppendEntries result within the heartbeat and will re-send the same
     * entries */
    CLUSTER_SET_DISK_LATENCY(1, 300);

    CLUSTER_STEP_UNTIL_DELIVERED(0, 1, 100); /* First AppendEntries */
    CLUSTER_STEP_UNTIL_ELAPSED(110);         /* Heartbeat timeout */
    CLUSTER_STEP_UNTIL_DELIVERED(0, 1, 100); /* Second AppendEntries */

    CLUSTER_STEP_UNTIL_APPLIED(0, req->index, 500);

    free(req);

    return MUNIT_OK;
}

/* If the term in the request is stale, the server rejects it. */
TEST(replication, recvStaleTerm, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW;
    BOOTSTRAP_START_AND_ELECT;

    /* Set a very high election timeout and the disconnect the leader so it will
     * keep sending heartbeats. */
    raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 5000);
    raft_set_election_timeout(CLUSTER_RAFT(0), 5000);
    CLUSTER_SATURATE_BOTHWAYS(0, 1);
    CLUSTER_SATURATE_BOTHWAYS(0, 2);

    /* Eventually a new leader gets elected. */
    CLUSTER_STEP_UNTIL_HAS_NO_LEADER(5000);
    CLUSTER_STEP_UNTIL_HAS_LEADER(10000);
    munit_assert_int(CLUSTER_LEADER, ==, 1);

    /* Reconnect the old leader to the current follower. */
    CLUSTER_DESATURATE_BOTHWAYS(0, 2);

    /* Step a few times, so the old leader sends heartbeats to the follower,
     * which rejects them. */
    CLUSTER_STEP_UNTIL_ELAPSED(200);

    return MUNIT_OK;
}

/* If server's log is shorter than prevLogIndex, the request is rejected . */
TEST(replication, recvMissingEntries, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry;
    CLUSTER_BOOTSTRAP;

    /* Server 0 has an entry that server 1 doesn't have */
    entry.type = RAFT_COMMAND;
    entry.term = 1;
    FsmEncodeSetX(1, &entry.buf);
    CLUSTER_ADD_ENTRY(0, &entry);

    /* Server 0 wins the election because it has a longer log. */
    CLUSTER_START;
    CLUSTER_STEP_UNTIL_HAS_LEADER(5000);
    munit_assert_int(CLUSTER_LEADER, ==, 0);

    /* The first server replicates missing entries to the second. */
    CLUSTER_STEP_UNTIL_APPLIED(1, 3, 3000);

    return MUNIT_OK;
}

/* If the term of the last log entry on the server is different from the one
 * prevLogTerm, and value of prevLogIndex is greater than server's commit commit
 * index (i.e. this is a normal inconsistency), we reject the request. */
TEST(replication, recvPrevLogTermMismatch, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry1;
    struct raft_entry entry2;
    CLUSTER_BOOTSTRAP;

    /* The servers have an entry with a conflicting term. */
    entry1.type = RAFT_COMMAND;
    entry1.term = 2;
    FsmEncodeSetX(1, &entry1.buf);
    CLUSTER_ADD_ENTRY(0, &entry1);

    entry2.type = RAFT_COMMAND;
    entry2.term = 1;
    FsmEncodeSetX(2, &entry2.buf);
    CLUSTER_ADD_ENTRY(1, &entry2);

    CLUSTER_START;
    CLUSTER_ELECT(0);

    /* The follower eventually replicates the entry */
    CLUSTER_STEP_UNTIL_APPLIED(1, 2, 3000);

    return MUNIT_OK;
}

/* The follower has an uncommitted log entry that conflicts with a new one sent
 * by the leader (same index but different term). The follower's conflicting log
 * entry happens to be a configuration change. In that case the follower
 * discards the conflicting entry from its log and rolls back its configuration
 * to the initial one contained in the log entry at index 1. */
TEST(replication, recvRollbackConfigurationToInitial, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry1;
    struct raft_entry entry2;
    struct raft_configuration base; /* Committed configuration at index 1 */
    struct raft_configuration conf; /* Uncommitted configuration at index 2 */
    CLUSTER_BOOTSTRAP;
    CLUSTER_CONFIGURATION(&base);

    /* Both servers have an entry at index 2, but with conflicting terms. The
     * entry of the second server is a configuration change. */
    entry1.type = RAFT_COMMAND;
    entry1.term = 2;
    FsmEncodeSetX(1, &entry1.buf);
    CLUSTER_ADD_ENTRY(0, &entry1);

    entry2.type = RAFT_CHANGE;
    entry2.term = 1;
    CLUSTER_CONFIGURATION(&conf);
    raft_configuration_add(&conf, 3, "3", 2);
    raft_configuration_encode(&conf, &entry2.buf);
    CLUSTER_ADD_ENTRY(1, &entry2);

    /* At startup the second server uses the most recent configuration, i.e. the
     * one contained in the entry that we just added. The server can't know yet
     * if it's committed or not, and regards it as pending configuration
     * change. */
    CLUSTER_START;
    ASSERT_CONFIGURATION(1, &conf);

    /* The first server gets elected. */
    CLUSTER_ELECT(0);

    /* The second server eventually replicates the first server's log entry at
     * index 2, truncating its own log and rolling back to the configuration
     * contained in the log entry at index 1. */
    CLUSTER_STEP_UNTIL_APPLIED(1, 2, 3000);
    ASSERT_CONFIGURATION(0, &base);
    ASSERT_CONFIGURATION(1, &base);

    raft_configuration_close(&base);
    raft_configuration_close(&conf);

    return MUNIT_OK;
}

/* The follower has an uncommitted log entry that conflicts with a new one sent
 * by the leader (same index but different term). The follower's conflicting log
 * entry happens to be a configuration change. There's also an older committed
 * configuration entry present. In that case the follower discards the
 * conflicting entry from its log and rolls back its configuration to the
 * committed one in the older configuration entry. */
TEST(replication, recvRollbackConfigurationToPrevious, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry1;
    struct raft_entry entry2;
    struct raft_entry entry3;
    struct raft_entry entry4;
    struct raft_configuration base; /* Committed configuration at index 2 */
    struct raft_configuration conf; /* Uncommitted configuration at index 3 */
    CLUSTER_BOOTSTRAP;
    CLUSTER_CONFIGURATION(&base);

    /* Both servers have a matching configuration entry at index 2. */
    CLUSTER_CONFIGURATION(&conf);

    entry1.type = RAFT_CHANGE;
    entry1.term = 1;
    raft_configuration_encode(&conf, &entry1.buf);
    CLUSTER_ADD_ENTRY(0, &entry1);

    entry2.type = RAFT_CHANGE;
    entry2.term = 1;
    raft_configuration_encode(&conf, &entry2.buf);
    CLUSTER_ADD_ENTRY(1, &entry2);

    /* Both servers have an entry at index 3, but with conflicting terms. The
     * entry of the second server is a configuration change. */
    entry3.type = RAFT_COMMAND;
    entry3.term = 2;
    FsmEncodeSetX(1, &entry3.buf);
    CLUSTER_ADD_ENTRY(0, &entry3);

    entry4.type = RAFT_CHANGE;
    entry4.term = 1;
    raft_configuration_add(&conf, 3, "3", 2);
    raft_configuration_encode(&conf, &entry4.buf);
    CLUSTER_ADD_ENTRY(1, &entry4);

    /* At startup the second server uses the most recent configuration, i.e. the
     * one contained in the log entry at index 3. The server can't know yet if
     * it's committed or not, and regards it as pending configuration change. */
    CLUSTER_START;
    ASSERT_CONFIGURATION(1, &conf);

    /* The first server gets elected. */
    CLUSTER_ELECT(0);

    /* The second server eventually replicates the first server's log entry at
     * index 3, truncating its own log and rolling back to the configuration
     * contained in the log entry at index 2. */
    CLUSTER_STEP_UNTIL_APPLIED(1, 3, 3000);
    ASSERT_CONFIGURATION(0, &base);
    ASSERT_CONFIGURATION(1, &base);

    raft_configuration_close(&base);
    raft_configuration_close(&conf);

    return MUNIT_OK;
}

/* The follower has an uncommitted log entry that conflicts with a new one sent
 * by the leader (same index but different term). The follower's conflicting log
 * entry happens to be a configuration change. The follower's log has been
 * truncated after a snashot and does not contain the previous committed
 * configuration anymore. In that case the follower discards the conflicting
 * entry from its log and rolls back its configuration to the previous committed
 * one, which was cached when the snapshot was restored. */
TEST(replication, recvRollbackConfigurationToSnapshot, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry1;
    struct raft_entry entry2;
    struct raft_configuration base; /* Committed configuration at index 1 */
    struct raft_configuration conf; /* Uncommitted configuration at index 2 */
    int rv;

    CLUSTER_CONFIGURATION(&conf);
    CLUSTER_CONFIGURATION(&base);

    /* Bootstrap the first server. This creates a log entry at index 1
     * containing the initial configuration. */
    rv = raft_bootstrap(CLUSTER_RAFT(0), &conf);
    munit_assert_int(rv, ==, 0);

    /* The second server has a snapshot up to entry 1. Entry 1 is not present in
     * the log. */
    CLUSTER_SET_SNAPSHOT(1 /*                                               */,
                         1 /* last index                                    */,
                         1 /* last term                                     */,
                         1 /* conf index                                    */,
                         5 /* x                                             */,
                         0 /* y                                             */);
    CLUSTER_SET_TERM(1, 1);

    /* Both servers have an entry at index 2, but with conflicting terms. The
     * entry of the second server is a configuration change and gets appended to
     * the truncated log. */
    entry1.type = RAFT_COMMAND;
    entry1.term = 3;
    FsmEncodeSetX(1, &entry1.buf);
    CLUSTER_ADD_ENTRY(0, &entry1);

    entry2.type = RAFT_CHANGE;
    entry2.term = 2;
    raft_configuration_add(&conf, 3, "3", 2);
    raft_configuration_encode(&conf, &entry2.buf);
    CLUSTER_ADD_ENTRY(1, &entry2);

    /* At startup the second server uses the most recent configuration, i.e. the
     * one contained in the log entry at index 2. The server can't know yet if
     * it's committed or not, and regards it as pending configuration change. */
    CLUSTER_START;
    ASSERT_CONFIGURATION(1, &conf);

    CLUSTER_ELECT(0);

    /* The second server eventually replicates the first server's log entry at
     * index 3, truncating its own log and rolling back to the configuration
     * contained in the snapshot, which is not present in the log anymore but
     * was cached at startup. */
    CLUSTER_STEP_UNTIL_APPLIED(1, 3, 3000);
    ASSERT_CONFIGURATION(0, &base);
    ASSERT_CONFIGURATION(1, &base);

    raft_configuration_close(&base);
    raft_configuration_close(&conf);

    return MUNIT_OK;
}

/* If any of the new entry has the same index of an existing entry in our log,
 * but different term, and that entry index is already committed, we bail out
 * with an error. */
TEST(replication, recvPrevIndexConflict, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry1;
    struct raft_entry entry2;
    CLUSTER_BOOTSTRAP;

    /* The servers have an entry with a conflicting term. */
    entry1.type = RAFT_COMMAND;
    entry1.term = 2;
    FsmEncodeSetX(1, &entry1.buf);
    CLUSTER_ADD_ENTRY(0, &entry1);

    entry2.type = RAFT_COMMAND;
    entry2.term = 1;
    FsmEncodeSetX(2, &entry2.buf);
    CLUSTER_ADD_ENTRY(1, &entry2);

    CLUSTER_START;
    CLUSTER_ELECT(0);

    /* Artificially bump the commit index on the second server */
    CLUSTER_RAFT(1)->commit_index = 2;
    CLUSTER_STEP;
    CLUSTER_STEP;

    return MUNIT_OK;
}

/* A write log request is submitted for outstanding log entries. If some entries
 * are already existing in the log, they will be skipped. */
TEST(replication, recvSkip, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_apply *req = munit_malloc(sizeof *req);
    BOOTSTRAP_START_AND_ELECT;

    /* Submit an entry */
    CLUSTER_APPLY_ADD_X(0, req, 1, NULL);

    /* The leader replicates the entry to the follower however it does not get
     * notified about the result, so it sends the entry again. */
    CLUSTER_STEP;
    CLUSTER_SATURATE_BOTHWAYS(0, 1);
    CLUSTER_STEP_UNTIL_ELAPSED(150);

    /* The follower reconnects and receives again the same entry. This time the
     * leader receives the notification. */
    CLUSTER_DESATURATE_BOTHWAYS(0, 1);
    CLUSTER_STEP_UNTIL_APPLIED(0, req->index, 2000);

    free(req);

    return MUNIT_OK;
}

/* If the index and term of the last snapshot on the server match prevLogIndex
 * and prevLogTerm the request is accepted. */
TEST(replication, recvMatch_last_snapshot, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry;
    struct raft_configuration configuration;
    int rv;

    CLUSTER_CONFIGURATION(&configuration);
    rv = raft_bootstrap(CLUSTER_RAFT(0), &configuration);
    munit_assert_int(rv, ==, 0);
    raft_configuration_close(&configuration);

    /* The first server has entry 2 */
    entry.type = RAFT_COMMAND;
    entry.term = 2;
    FsmEncodeSetX(5, &entry.buf);
    CLUSTER_ADD_ENTRY(0, &entry);

    /* The second server has a snapshot up to entry 2 */
    CLUSTER_SET_SNAPSHOT(1 /*                                               */,
                         2 /* last index                                    */,
                         2 /* last term                                     */,
                         1 /* conf index                                    */,
                         5 /* x                                             */,
                         0 /* y                                             */);
    CLUSTER_SET_TERM(1, 2);

    CLUSTER_START;
    CLUSTER_ELECT(0);

    /* Apply an additional entry and check that it gets replicated on the
     * follower. */
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_STEP_UNTIL_APPLIED(1, 3, 3000);

    return MUNIT_OK;
}

/* If a candidate server receives a request containing the same term as its
 * own, it it steps down to follower and accept the request . */
TEST(replication, recvCandidateSameTerm, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW;
    CLUSTER_BOOTSTRAP;

    /* Disconnect server 2 from the other two and set a low election timeout on
     * it, so it will immediately start an election. */
    CLUSTER_SATURATE_BOTHWAYS(2, 0);
    CLUSTER_SATURATE_BOTHWAYS(2, 1);
    raft_fixture_set_randomized_election_timeout(&f->cluster, 2, 800);
    raft_set_election_timeout(CLUSTER_RAFT(2), 800);

    /* Server 2 becomes candidate. */
    CLUSTER_START;
    CLUSTER_STEP_UNTIL_STATE_IS(2, RAFT_CANDIDATE, 1000);
    munit_assert_int(CLUSTER_TERM(2), ==, 2);

    /* Server 0 wins the election and replicates an entry. */
    CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_LEADER, 2000);
    munit_assert_int(CLUSTER_TERM(0), ==, 2);
    munit_assert_int(CLUSTER_TERM(1), ==, 2);
    munit_assert_int(CLUSTER_TERM(2), ==, 2);
    CLUSTER_MAKE_PROGRESS;

    /* Now reconnect the third server, which eventually steps down and
     * replicates the entry. */
    munit_assert_int(CLUSTER_STATE(2), ==, RAFT_CANDIDATE);
    munit_assert_int(CLUSTER_TERM(2), ==, 2);
    CLUSTER_DESATURATE_BOTHWAYS(2, 0);
    CLUSTER_DESATURATE_BOTHWAYS(2, 1);
    CLUSTER_STEP_UNTIL_STATE_IS(2, RAFT_FOLLOWER, 2000);
    CLUSTER_STEP_UNTIL_APPLIED(2, 2, 2000);

    return MUNIT_OK;
}

/* If a candidate server receives a request containing an higher term as its
 * own, it it steps down to follower and accept the request . */
TEST(replication, recvCandidateHigherTerm, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW;
    CLUSTER_BOOTSTRAP;

    /* Set a high election timeout on server 1, so it won't become candidate */
    raft_fixture_set_randomized_election_timeout(&f->cluster, 1, 2000);
    raft_set_election_timeout(CLUSTER_RAFT(1), 2000);

    /* Disconnect server 2 from the other two. */
    CLUSTER_SATURATE_BOTHWAYS(2, 0);
    CLUSTER_SATURATE_BOTHWAYS(2, 1);

    /* Set a low election timeout on server 0, and disconnect it from server 1,
     * so by the time it wins the second round, server 2 will have turned
     * candidate */
    raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 800);
    raft_set_election_timeout(CLUSTER_RAFT(0), 800);
    CLUSTER_SATURATE_BOTHWAYS(0, 1);

    CLUSTER_START;

    /* Server 2 becomes candidate, and server 0 already is candidate. */
    CLUSTER_STEP_UNTIL_STATE_IS(2, RAFT_CANDIDATE, 1500);
    munit_assert_int(CLUSTER_TERM(2), ==, 2);
    munit_assert_int(CLUSTER_STATE(0), ==, RAFT_CANDIDATE);
    munit_assert_int(CLUSTER_TERM(0), ==, 2);

    /* Server 0 starts a new election, while server 2 is still candidate */
    CLUSTER_STEP_UNTIL_TERM_IS(0, 3, 2000);
    munit_assert_int(CLUSTER_TERM(2), ==, 2);
    munit_assert_int(CLUSTER_STATE(2), ==, RAFT_CANDIDATE);

    /* Reconnect the first and second server and let the election succeed and
     * replicate an entry. */
    CLUSTER_DESATURATE_BOTHWAYS(0, 1);
    CLUSTER_STEP_UNTIL_HAS_LEADER(1000);
    CLUSTER_MAKE_PROGRESS;

    /* Now reconnect the third server, which eventually steps down and
     * replicates the entry. */
    munit_assert_int(CLUSTER_STATE(2), ==, RAFT_CANDIDATE);
    munit_assert_int(CLUSTER_TERM(2), ==, 2);
    CLUSTER_DESATURATE_BOTHWAYS(2, 0);
    CLUSTER_DESATURATE_BOTHWAYS(2, 1);
    CLUSTER_STEP_UNTIL_STATE_IS(2, RAFT_FOLLOWER, 2000);
    CLUSTER_STEP_UNTIL_APPLIED(2, 2, 2000);

    return MUNIT_OK;
}

/* If the server handling the response is not the leader, the result
 * is ignored. */
TEST(replication, resultNotLeader, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    BOOTSTRAP_START_AND_ELECT;

    /* Set a very high-latency for the second server's outgoing messages, so the
     * first server won't get notified about the results for a while. */
    CLUSTER_SET_NETWORK_LATENCY(1, 400);

    /* Set a low election timeout on the first server so it will step down very
     * soon. */
    raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 200);
    raft_set_election_timeout(CLUSTER_RAFT(0), 200);

    /* Eventually leader steps down and becomes candidate. */
    CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_CANDIDATE, 2000);

    /* The AppendEntries result eventually gets delivered, but the candidate
     * ignores it. */
    CLUSTER_STEP_UNTIL_ELAPSED(400);

    return MUNIT_OK;
}

/* If the response has a term which is lower than the server's one, it's
 * ignored. */
TEST(replication, resultLowerTerm, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW;
    BOOTSTRAP_START_AND_ELECT;

    /* Set a very high-latency for the second server's outgoing messages, so the
     * first server won't get notified about the results for a while. */
    CLUSTER_SET_NETWORK_LATENCY(1, 2000);

    /* Set a high election timeout on server 1, so it won't become candidate */
    raft_fixture_set_randomized_election_timeout(&f->cluster, 1, 2000);
    raft_set_election_timeout(CLUSTER_RAFT(1), 2000);

    /* Disconnect server 0 and set a low election timeout on it so it will step
     * down very soon. */
    CLUSTER_SATURATE_BOTHWAYS(0, 2);
    raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 200);
    raft_set_election_timeout(CLUSTER_RAFT(0), 200);
    CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_FOLLOWER, 2000);

    /* Make server 0 become leader again. */
    CLUSTER_DESATURATE_BOTHWAYS(0, 2);
    CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_LEADER, 4000);

    /* Eventually deliver the result message. */
    CLUSTER_STEP_UNTIL_ELAPSED(2500);

    return MUNIT_OK;
}

/* If the response has a term which is higher than the server's one, step down
 * to follower. */
TEST(replication, resultHigherTerm, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW;
    BOOTSTRAP_START_AND_ELECT;

    /* Set a very high election timeout for server 0 so it won't step down. */
    raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 5000);
    raft_set_election_timeout(CLUSTER_RAFT(0), 5000);

    /* Disconnect the server 0 from the rest of the cluster. */
    CLUSTER_SATURATE_BOTHWAYS(0, 1);
    CLUSTER_SATURATE_BOTHWAYS(0, 2);

    /* Eventually a new leader gets elected */
    CLUSTER_STEP_UNTIL_HAS_NO_LEADER(2000);
    CLUSTER_STEP_UNTIL_HAS_LEADER(4000);
    munit_assert_int(CLUSTER_LEADER, ==, 1);

    /* Reconnect the old leader to the current follower, which eventually
     * replies with an AppendEntries result containing an higher term. */
    CLUSTER_DESATURATE_BOTHWAYS(0, 2);
    CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_FOLLOWER, 2000);

    return MUNIT_OK;
}

/* If the response fails because a log mismatch, the nextIndex for the server is
 * updated and the relevant older entries are resent. */
TEST(replication, resultRetry, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry;
    CLUSTER_BOOTSTRAP;

    /* Add an additional entry to the first server that the second server does
     * not have. */
    entry.type = RAFT_COMMAND;
    entry.term = 1;
    FsmEncodeSetX(5, &entry.buf);
    CLUSTER_ADD_ENTRY(0, &entry);

    CLUSTER_START;
    CLUSTER_ELECT(0);

    /* The first server receives an AppendEntries result from the second server
     * indicating that its log does not have the entry at index 2, so it will
     * resend it. */
    CLUSTER_STEP_UNTIL_APPLIED(1, 3, 2000);

    return MUNIT_OK;
}

static void applyAssertStatusCb(struct raft_apply *req,
                                int status,
                                void *result)
{
    (void)result;
    int status_expected = (int)(intptr_t)(req->data);
    munit_assert_int(status_expected, ==, status);
}

/* When the leader fails to write some new entries to disk, it steps down. */
TEST(replication, diskWriteFailure, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_apply *req = munit_malloc(sizeof(*req));
    req->data = (void *)(intptr_t)RAFT_IOERR;
    BOOTSTRAP_START_AND_ELECT;

    raft_fixture_append_fault(&f->cluster, 0, 0);
    CLUSTER_APPLY_ADD_X(0, req, 1, applyAssertStatusCb);
    /* The leader steps down when its disk write fails. */
    CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_FOLLOWER, 2000);
    free(req);

    return MUNIT_OK;
}

/* A follower updates its term number while persisting entries. */
TEST(replication, newTermWhileAppending, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_apply *req = munit_malloc(sizeof(*req));
    raft_term term;
    CLUSTER_GROW;

    /* Make sure that persisting entries will take a long time */
    CLUSTER_SET_DISK_LATENCY(2, 3000);

    BOOTSTRAP_START_AND_ELECT;
    CLUSTER_APPLY_ADD_X(0, req, 1, NULL);

    /* Wait for the leader to replicate the entry */
    CLUSTER_STEP_UNTIL_ELAPSED(500);

    /* Force a new term */
    term = CLUSTER_RAFT(2)->current_term;
    CLUSTER_DEPOSE;
    CLUSTER_ELECT(1);

    CLUSTER_STEP_UNTIL_ELAPSED(500);
    munit_assert_ullong(CLUSTER_RAFT(2)->current_term, ==, term + 1);

    /* Wait for the long disk write to complete */
    CLUSTER_STEP_UNTIL_ELAPSED(3000);

    free(req);

    return MUNIT_OK;
}

/* A leader with slow disk commits an entry that it hasn't persisted yet,
 * because enough followers to have a majority have aknowledged that they have
 * appended the entry. The leader's last_stored field hence lags behind its
 * commit_index. A new leader gets elected, with a higher commit index and sends
 * first a new entry than a heartbeat to the old leader, that needs to update
 * its commit_index taking into account its lagging last_stored. */
TEST(replication, lastStoredLaggingBehindCommitIndex, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW;

    /* Server 0 takes a long time to persist entry 2 (the barrier) */
    CLUSTER_SET_DISK_LATENCY(0, 10000);

    /* Server 0 gets elected and creates a barrier entry at index 2 */
    BOOTSTRAP_START_AND_ELECT;

    /* Server 0 commits and applies barrier entry 2 even if it not persist it
     * yet. */
    CLUSTER_STEP_UNTIL_APPLIED(0, 2, 2000);

    munit_assert_int(CLUSTER_RAFT(0)->last_stored, ==, 1);
    munit_assert_int(CLUSTER_RAFT(0)->commit_index, ==, 2);
    munit_assert_int(CLUSTER_RAFT(0)->last_applied, ==, 2);

    /* Server 1 stored barrier entry 2, but did not yet receive a notification
     * from server 0 about the new commit index. */
    munit_assert_int(CLUSTER_RAFT(1)->last_stored, ==, 2);
    munit_assert_int(CLUSTER_RAFT(1)->commit_index, ==, 1);
    munit_assert_int(CLUSTER_RAFT(1)->last_applied, ==, 1);

    /* Disconnect server 0 from server 1 and 2. */
    CLUSTER_DISCONNECT(0, 1);
    CLUSTER_DISCONNECT(0, 2);

    /* Set a very high election timeout on server 0, so it won't step down for a
     * while, even if disconnected. */
    raft_fixture_set_randomized_election_timeout(&f->cluster, 0, 10000);
    raft_set_election_timeout(CLUSTER_RAFT(0), 10000);

    /* Server 1 and 2 eventually timeout and start an election, server 1
     * wins. */
    CLUSTER_STEP_UNTIL_HAS_NO_LEADER(4000);
    CLUSTER_STEP_UNTIL_HAS_LEADER(2000);
    munit_assert_int(CLUSTER_LEADER, ==, 1);

    /* Server 1 commits the barrier entry at index 3 that it created at the
     * start of its term. */
    CLUSTER_STEP_UNTIL_APPLIED(1, 3, 2000);

    /* Reconnect server 0 to server 1, which will start replicating entry 3 to
     * it. */
    CLUSTER_RECONNECT(0, 1);
    CLUSTER_STEP_UNTIL_APPLIED(0, 3, 20000);

    return MUNIT_OK;
}

/* A leader with faulty disk fails to persist the barrier entry upon election.
 */
TEST(replication, failPersistBarrier, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW;

    /* Server 0 will fail to persist entry 2, a barrier */
    raft_fixture_append_fault(&f->cluster, 0, 0);

    /* Server 0 gets elected and creates a barrier entry at index 2 */
    CLUSTER_BOOTSTRAP;
    CLUSTER_START;
    CLUSTER_START_ELECT(0);

    /* Cluster recovers. */
    CLUSTER_STEP_UNTIL_HAS_LEADER(20000);

    return MUNIT_OK;
}

/* All servers fail to persist the barrier entry upon election of the first
 * leader. Ensure the cluster is able to make progress afterwards.
 */
TEST(replication, failPersistBarrierFollower, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_GROW;

    /* The servers will fail to persist entry 2, a barrier */
    raft_fixture_append_fault(&f->cluster, 1, 0);
    raft_fixture_append_fault(&f->cluster, 2, 0);

    /* Server 0 gets elected and creates a barrier entry at index 2 */
    CLUSTER_BOOTSTRAP;
    CLUSTER_START;
    CLUSTER_START_ELECT(0);

    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_MAKE_PROGRESS;

    return MUNIT_OK;
}

/* A leader originates a log entry, fails to persist it, and steps down.
 * A follower that received the entry wins the ensuing election and sends
 * the same entry back to the original leader, while the original leader
 * still has an outgoing pending message that references its copy of the
 * entry. This triggers the original leader to reinstate the entry in its
 * log. */
TEST(replication, receiveSameWithPendingSend, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_apply req;

    /* Three voters. */
    CLUSTER_GROW;
    /* Server 0 is the leader. */
    BOOTSTRAP_START_AND_ELECT;

    /* Server 1 never gets the entry. */
    raft_fixture_set_send_latency(&f->cluster, 0, 1, 10000);

    /* Disk write fails, but not before the entry gets to server 2. */
    CLUSTER_SET_DISK_LATENCY(0, 1000);
    raft_fixture_append_fault(&f->cluster, 0, 0);
    req.data = (void *)(intptr_t)RAFT_IOERR;
    CLUSTER_APPLY_ADD_X(0, &req, 1, NULL);
    /* Server 0 steps down. */
    CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_FOLLOWER, 1500);
    munit_assert_ullong(CLUSTER_RAFT(0)->current_term, ==, 2);
    ASSERT_FOLLOWER(1);
    ASSERT_FOLLOWER(2);
    /* Only server 2 has the new entry. */
    munit_assert_ullong(CLUSTER_RAFT(0)->last_stored, ==, 2);
    munit_assert_ullong(CLUSTER_RAFT(1)->last_stored, ==, 2);
    munit_assert_ullong(CLUSTER_RAFT(2)->last_stored, ==, 3);

    /* Server 2 times out first and wins the election. */
    raft_set_election_timeout(CLUSTER_RAFT(2), 500);
    raft_fixture_start_elect(&f->cluster, 2);
    CLUSTER_STEP_UNTIL_STATE_IS(2, RAFT_LEADER, 1000);
    munit_assert_ullong(CLUSTER_RAFT(2)->current_term, ==, 3);

    /* Server 0 gets the same entry back from server 2. */
    CLUSTER_STEP_UNTIL_APPLIED(2, 3, 1000);
    return MUNIT_OK;
}
