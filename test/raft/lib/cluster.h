/* Setup and drive a test raft cluster. */

#ifndef TEST_CLUSTER_H
#define TEST_CLUSTER_H

#include <stdlib.h>

#include "../../../src/raft.h"
#include "fsm.h"
#include "heap.h"
#include "munit.h"
#include "snapshot.h"

#define FIXTURE_CLUSTER                             \
    FIXTURE_HEAP;                                   \
    struct raft_fsm fsms[RAFT_FIXTURE_MAX_SERVERS]; \
    struct raft_fixture cluster

/* N is the default number of servers, but can be tweaked with the cluster-n
 * parameter. */
#define SETUP_CLUSTER(DEFAULT_N)                                               \
    SET_UP_HEAP;                                                               \
    do {                                                                       \
        unsigned _n = DEFAULT_N;                                               \
        bool _pre_vote = false;                                                \
        bool _ss_async = false;                                                \
        int _fsm_version = 3;                                                  \
        unsigned _hb = 0;                                                      \
        unsigned _i;                                                           \
        int _rv;                                                               \
        if (munit_parameters_get(params, CLUSTER_N_PARAM) != NULL) {           \
            _n = atoi(munit_parameters_get(params, CLUSTER_N_PARAM));          \
        }                                                                      \
        if (munit_parameters_get(params, CLUSTER_PRE_VOTE_PARAM) != NULL) {    \
            _pre_vote =                                                        \
                atoi(munit_parameters_get(params, CLUSTER_PRE_VOTE_PARAM));    \
        }                                                                      \
        if (munit_parameters_get(params, CLUSTER_HEARTBEAT_PARAM) != NULL) {   \
            _hb = atoi(munit_parameters_get(params, CLUSTER_HEARTBEAT_PARAM)); \
        }                                                                      \
        if (munit_parameters_get(params, CLUSTER_SS_ASYNC_PARAM) != NULL) {    \
            _ss_async =                                                        \
                atoi(munit_parameters_get(params, CLUSTER_SS_ASYNC_PARAM));    \
        }                                                                      \
        if (munit_parameters_get(params, CLUSTER_FSM_VERSION_PARAM) != NULL) { \
            _fsm_version =                                                     \
                atoi(munit_parameters_get(params, CLUSTER_FSM_VERSION_PARAM)); \
        }                                                                      \
        munit_assert_int(_n, >, 0);                                            \
        _rv = raft_fixture_init(&f->cluster);                                  \
        munit_assert_int(_rv, ==, 0);                                          \
        for (_i = 0; _i < _n; _i++) {                                          \
            if (!_ss_async || _fsm_version < 3) {                              \
                FsmInit(&f->fsms[_i], _fsm_version);                           \
            } else {                                                           \
                FsmInitAsync(&f->fsms[_i], _fsm_version);                      \
            }                                                                  \
            _rv = raft_fixture_grow(&f->cluster, &f->fsms[_i]);                \
            munit_assert_int(_rv, ==, 0);                                      \
        }                                                                      \
        for (_i = 0; _i < _n; _i++) {                                          \
            raft_set_pre_vote(raft_fixture_get(&f->cluster, _i), _pre_vote);   \
            if (_hb) {                                                         \
                raft_set_heartbeat_timeout(raft_fixture_get(&f->cluster, _i),  \
                                           _hb);                               \
            }                                                                  \
        }                                                                      \
    } while (0)

#define TEAR_DOWN_CLUSTER                 \
    do {                                  \
        unsigned i;                       \
        raft_fixture_close(&f->cluster);  \
        for (i = 0; i < CLUSTER_N; i++) { \
            FsmClose(&f->fsms[i]);        \
        }                                 \
    } while (0);                          \
    TEAR_DOWN_HEAP;

/* Munit parameter for setting the number of servers */
#define CLUSTER_N_PARAM "cluster-n"

/* Munit parameter for setting the number of voting servers */
#define CLUSTER_N_VOTING_PARAM "cluster-n-voting"

/* Munit parameter for enabling pre-vote */
#define CLUSTER_PRE_VOTE_PARAM "cluster-pre-vote"

/* Munit parameter for setting HeartBeat timeout */
#define CLUSTER_HEARTBEAT_PARAM "cluster-heartbeat"

/* Munit parameter for setting snapshot behaviour */
#define CLUSTER_SS_ASYNC_PARAM "cluster-snapshot-async"

/* Munit parameter for setting fsm version */
#define CLUSTER_FSM_VERSION_PARAM "fsm-version"

/* Get the number of servers in the cluster. */
#define CLUSTER_N raft_fixture_n(&f->cluster)

/* Get the cluster time. */
#define CLUSTER_TIME raft_fixture_time(&f->cluster)

/* Index of the current leader, or CLUSTER_N if there's no leader. */
#define CLUSTER_LEADER raft_fixture_leader_index(&f->cluster)

/* True if the cluster has a leader. */
#define CLUSTER_HAS_LEADER CLUSTER_LEADER < CLUSTER_N

/* Get the struct raft object of the I'th server. */
#define CLUSTER_RAFT(I) raft_fixture_get(&f->cluster, I)

/* Get the state of the I'th server. */
#define CLUSTER_STATE(I) raft_state(raft_fixture_get(&f->cluster, I))

/* Get the current term of the I'th server. */
#define CLUSTER_TERM(I) raft_fixture_get(&f->cluster, I)->current_term

/* Get the struct fsm object of the I'th server. */
#define CLUSTER_FSM(I) &f->fsms[I]

/* Return the last applied index on the I'th server. */
#define CLUSTER_LAST_APPLIED(I) \
    raft_last_applied(raft_fixture_get(&f->cluster, I))

/* Return the ID of the server the I'th server has voted for. */
#define CLUSTER_VOTED_FOR(I) raft_fixture_voted_for(&f->cluster, I)

/* Return a description of the last error occurred on the I'th server. */
#define CLUSTER_ERRMSG(I) raft_errmsg(CLUSTER_RAFT(I))

/* Populate the given configuration with all servers in the fixture. All servers
 * will be voting. */
#define CLUSTER_CONFIGURATION(CONF)                                     \
    {                                                                   \
        int rv_;                                                        \
        rv_ = raft_fixture_configuration(&f->cluster, CLUSTER_N, CONF); \
        munit_assert_int(rv_, ==, 0);                                   \
    }

/* Bootstrap all servers in the cluster. All servers will be voting, unless the
 * cluster-n-voting parameter is used. */
#define CLUSTER_BOOTSTRAP                                                    \
    {                                                                        \
        unsigned n_ = CLUSTER_N;                                             \
        int rv_;                                                             \
        struct raft_configuration configuration;                             \
        if (munit_parameters_get(params, CLUSTER_N_VOTING_PARAM) != NULL) {  \
            n_ = atoi(munit_parameters_get(params, CLUSTER_N_VOTING_PARAM)); \
        }                                                                    \
        rv_ = raft_fixture_configuration(&f->cluster, n_, &configuration);   \
        munit_assert_int(rv_, ==, 0);                                        \
        rv_ = raft_fixture_bootstrap(&f->cluster, &configuration);           \
        munit_assert_int(rv_, ==, 0);                                        \
        raft_configuration_close(&configuration);                            \
    }

/* Bootstrap all servers in the cluster. Only the first N servers will be
 * voting. */
#define CLUSTER_BOOTSTRAP_N_VOTING(N)                                      \
    {                                                                      \
        int rv_;                                                           \
        struct raft_configuration configuration_;                          \
        rv_ = raft_fixture_configuration(&f->cluster, N, &configuration_); \
        munit_assert_int(rv_, ==, 0);                                      \
        rv_ = raft_fixture_bootstrap(&f->cluster, &configuration_);        \
        munit_assert_int(rv_, ==, 0);                                      \
        raft_configuration_close(&configuration_);                         \
    }

/* Start all servers in the test cluster. */
#define CLUSTER_START                         \
    {                                         \
        int rc;                               \
        rc = raft_fixture_start(&f->cluster); \
        munit_assert_int(rc, ==, 0);          \
    }

/* Step the cluster. */
#define CLUSTER_STEP raft_fixture_step(&f->cluster);

/* Step the cluster N times. */
#define CLUSTER_STEP_N(N)                   \
    {                                       \
        unsigned i_;                        \
        for (i_ = 0; i_ < N; i_++) {        \
            raft_fixture_step(&f->cluster); \
        }                                   \
    }

/* Step until the given function becomes true. */
#define CLUSTER_STEP_UNTIL(FUNC, ARG, MSECS)                            \
    {                                                                   \
        bool done_;                                                     \
        done_ = raft_fixture_step_until(&f->cluster, FUNC, ARG, MSECS); \
        munit_assert_true(done_);                                       \
    }

/* Step the cluster until a leader is elected or #MAX_MSECS have elapsed. */
#define CLUSTER_STEP_UNTIL_ELAPSED(MSECS) \
    raft_fixture_step_until_elapsed(&f->cluster, MSECS)

/* Step the cluster until a leader is elected or #MAX_MSECS have elapsed. */
#define CLUSTER_STEP_UNTIL_HAS_LEADER(MAX_MSECS)                           \
    {                                                                      \
        bool done;                                                         \
        done = raft_fixture_step_until_has_leader(&f->cluster, MAX_MSECS); \
        munit_assert_true(done);                                           \
        munit_assert_true(CLUSTER_HAS_LEADER);                             \
    }

/* Step the cluster until there's no leader or #MAX_MSECS have elapsed. */
#define CLUSTER_STEP_UNTIL_HAS_NO_LEADER(MAX_MSECS)                           \
    {                                                                         \
        bool done;                                                            \
        done = raft_fixture_step_until_has_no_leader(&f->cluster, MAX_MSECS); \
        munit_assert_true(done);                                              \
        munit_assert_false(CLUSTER_HAS_LEADER);                               \
    }

/* Step the cluster until the given index was applied by the given server (or
 * all if N) or #MAX_MSECS have elapsed. */
#define CLUSTER_STEP_UNTIL_APPLIED(I, INDEX, MAX_MSECS)                        \
    {                                                                          \
        bool done;                                                             \
        done =                                                                 \
            raft_fixture_step_until_applied(&f->cluster, I, INDEX, MAX_MSECS); \
        munit_assert_true(done);                                               \
    }

/* Step the cluster until the state of the server with the given index matches
 * the given value, or #MAX_MSECS have elapsed. */
#define CLUSTER_STEP_UNTIL_STATE_IS(I, STATE, MAX_MSECS)               \
    {                                                                  \
        bool done;                                                     \
        done = raft_fixture_step_until_state_is(&f->cluster, I, STATE, \
                                                MAX_MSECS);            \
        munit_assert_true(done);                                       \
    }

/* Step the cluster until the term of the server with the given index matches
 * the given value, or #MAX_MSECS have elapsed. */
#define CLUSTER_STEP_UNTIL_TERM_IS(I, TERM, MAX_MSECS)                        \
    {                                                                         \
        bool done;                                                            \
        done =                                                                \
            raft_fixture_step_until_term_is(&f->cluster, I, TERM, MAX_MSECS); \
        munit_assert_true(done);                                              \
    }

/* Step the cluster until server I has voted for server J, or #MAX_MSECS have
 * elapsed. */
#define CLUSTER_STEP_UNTIL_VOTED_FOR(I, J, MAX_MSECS)                        \
    {                                                                        \
        bool done;                                                           \
        done =                                                               \
            raft_fixture_step_until_voted_for(&f->cluster, I, J, MAX_MSECS); \
        munit_assert_true(done);                                             \
    }

/* Step the cluster until all messages from server I to server J have been
 * delivered, or #MAX_MSECS elapse. */
#define CLUSTER_STEP_UNTIL_DELIVERED(I, J, MAX_MSECS)                        \
    {                                                                        \
        bool done;                                                           \
        done =                                                               \
            raft_fixture_step_until_delivered(&f->cluster, I, J, MAX_MSECS); \
        munit_assert_true(done);                                             \
    }

/* Request to apply an FSM command to add the given value to x. */
#define CLUSTER_APPLY_ADD_X(I, REQ, VALUE, CB)            \
    {                                                     \
        struct raft_buffer buf_;                          \
        struct raft *raft_;                               \
        int rv_;                                          \
        FsmEncodeAddX(VALUE, &buf_);                      \
        raft_ = raft_fixture_get(&f->cluster, I);         \
        rv_ = raft_apply(raft_, REQ, &buf_, 1, CB); \
        munit_assert_int(rv_, ==, 0);                     \
    }

/* Kill the I'th server. */
#define CLUSTER_KILL(I) raft_fixture_kill(&f->cluster, I);

/* Revive the I'th server */
#define CLUSTER_REVIVE(I) raft_fixture_revive(&f->cluster, I);

/* Kill the leader. */
#define CLUSTER_KILL_LEADER CLUSTER_KILL(CLUSTER_LEADER)

/* Kill a majority of servers, except the leader (if there is one). */
#define CLUSTER_KILL_MAJORITY                                \
    {                                                        \
        size_t i2;                                           \
        size_t n;                                            \
        for (i2 = 0, n = 0; n < (CLUSTER_N / 2) + 1; i2++) { \
            if (i2 == CLUSTER_LEADER) {                      \
                continue;                                    \
            }                                                \
            CLUSTER_KILL(i2)                                 \
            n++;                                             \
        }                                                    \
    }

/* Grow the cluster adding one server. */
#define CLUSTER_GROW                                               \
    {                                                              \
        int rv_;                                                   \
        FsmInit(&f->fsms[CLUSTER_N], 2);                           \
        rv_ = raft_fixture_grow(&f->cluster, &f->fsms[CLUSTER_N]); \
        munit_assert_int(rv_, ==, 0);                              \
    }

/* Add a new pristine server to the cluster, connected to all others. Then
 * submit a request to add it to the configuration as an idle server. */
#define CLUSTER_ADD(REQ)                                               \
    {                                                                  \
        int rc;                                                        \
        struct raft *new_raft;                                         \
        CLUSTER_GROW;                                                  \
        rc = raft_start(CLUSTER_RAFT(CLUSTER_N - 1));                  \
        munit_assert_int(rc, ==, 0);                                   \
        new_raft = CLUSTER_RAFT(CLUSTER_N - 1);                        \
        rc = raft_add(CLUSTER_RAFT(CLUSTER_LEADER), REQ, new_raft->id, \
                      new_raft->address, NULL);                        \
        munit_assert_int(rc, ==, 0);                                   \
    }

/* Assign the given role to the server that was added last. */
#define CLUSTER_ASSIGN(REQ, ROLE)                                              \
    do {                                                                       \
        unsigned _id;                                                          \
        int _rv;                                                               \
        _id = CLUSTER_N; /* Last server that was added. */                     \
        _rv = raft_assign(CLUSTER_RAFT(CLUSTER_LEADER), REQ, _id, ROLE, NULL); \
        munit_assert_int(_rv, ==, 0);                                          \
    } while (0)

/* Ensure that the cluster can make progress from the current state.
 *
 * - If no leader is present, wait for one to be elected.
 * - Submit a request to apply a new FSM command and wait for it to complete. */
#define CLUSTER_MAKE_PROGRESS                                          \
    {                                                                  \
        struct raft_apply *req_ = munit_malloc(sizeof *req_);          \
        if (!(CLUSTER_HAS_LEADER)) {                                   \
            CLUSTER_STEP_UNTIL_HAS_LEADER(10000);                      \
        }                                                              \
        CLUSTER_APPLY_ADD_X(CLUSTER_LEADER, req_, 1, NULL);            \
        CLUSTER_STEP_UNTIL_APPLIED(CLUSTER_LEADER, req_->index, 3000); \
        free(req_);                                                    \
    }

/* Elect the I'th server. */
#define CLUSTER_ELECT(I) raft_fixture_elect(&f->cluster, I)

/* Start to elect the I'th server. */
#define CLUSTER_START_ELECT(I) raft_fixture_start_elect(&f->cluster, I)

/* Depose the current leader */
#define CLUSTER_DEPOSE raft_fixture_depose(&f->cluster)

/* Disconnect I from J. */
#define CLUSTER_DISCONNECT(I, J) raft_fixture_disconnect(&f->cluster, I, J)

/* Reconnect I to J. */
#define CLUSTER_RECONNECT(I, J) raft_fixture_reconnect(&f->cluster, I, J)

/* Saturate the connection from I to J. */
#define CLUSTER_SATURATE(I, J) raft_fixture_saturate(&f->cluster, I, J)

/* Saturate the connection from I to J and from J to I, in both directions. */
#define CLUSTER_SATURATE_BOTHWAYS(I, J) \
    CLUSTER_SATURATE(I, J);             \
    CLUSTER_SATURATE(J, I)

/* Desaturate the connection between I and J, making messages flow again. */
#define CLUSTER_DESATURATE(I, J) raft_fixture_desaturate(&f->cluster, I, J)

/* Reconnect two servers. */
#define CLUSTER_DESATURATE_BOTHWAYS(I, J) \
    CLUSTER_DESATURATE(I, J);             \
    CLUSTER_DESATURATE(J, I)

/* Set the network latency of outgoing messages of server I. */
#define CLUSTER_SET_NETWORK_LATENCY(I, MSECS) \
    raft_fixture_set_network_latency(&f->cluster, I, MSECS)

/* Set the disk I/O latency of server I. */
#define CLUSTER_SET_DISK_LATENCY(I, MSECS) \
    raft_fixture_set_disk_latency(&f->cluster, I, MSECS)

/* Set the term persisted on the I'th server. This must be called before
 * starting the cluster. */
#define CLUSTER_SET_TERM(I, TERM) raft_fixture_set_term(&f->cluster, I, TERM)

/* Set the snapshot persisted on the I'th server. This must be called before
 * starting the cluster. */
#define CLUSTER_SET_SNAPSHOT(I, LAST_INDEX, LAST_TERM, CONF_INDEX, X, Y)  \
    {                                                                     \
        struct raft_configuration configuration_;                         \
        struct raft_snapshot *snapshot_;                                  \
        CLUSTER_CONFIGURATION(&configuration_);                           \
        CREATE_SNAPSHOT(snapshot_, LAST_INDEX, LAST_TERM, configuration_, \
                        CONF_INDEX, X, Y);                                \
        raft_fixture_set_snapshot(&f->cluster, I, snapshot_);             \
    }

/* Add a persisted entry to the I'th server. This must be called before
 * starting the cluster. */
#define CLUSTER_ADD_ENTRY(I, ENTRY) \
    raft_fixture_add_entry(&f->cluster, I, ENTRY)

/* Add an entry to the ones persisted on the I'th server. This must be called
 * before starting the cluster. */
#define CLUSTER_ADD_ENTRY(I, ENTRY) \
    raft_fixture_add_entry(&f->cluster, I, ENTRY)

/* Return the number of messages sent by the given server. */
#define CLUSTER_N_SEND(I, TYPE) raft_fixture_n_send(&f->cluster, I, TYPE)

/* Return the number of messages sent by the given server. */
#define CLUSTER_N_RECV(I, TYPE) raft_fixture_n_recv(&f->cluster, I, TYPE)

/* Set a fixture hook that randomizes election timeouts, disk latency and
 * network latency. */
#define CLUSTER_RANDOMIZE                \
    cluster_randomize_init(&f->cluster); \
    raft_fixture_hook(&f->cluster, cluster_randomize)

void cluster_randomize_init(struct raft_fixture *f);
void cluster_randomize(struct raft_fixture *f,
                       struct raft_fixture_event *event);

#endif /* TEST_CLUSTER_H */
