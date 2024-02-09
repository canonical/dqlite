#include "../lib/runner.h"
#include "../lib/tcp.h"
#include "../lib/uv.h"

/******************************************************************************
 *
 * Fixture with a libuv-based raft_io instance.
 *
 *****************************************************************************/

struct peer
{
    struct uv_loop_s loop;
    struct raft_uv_transport transport;
    struct raft_io io;
};

struct fixture
{
    FIXTURE_UV_DEPS;
    FIXTURE_TCP;
    FIXTURE_UV;
    struct peer peer;
    bool closed;
};

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

struct result
{
    struct raft_message *message;
    bool done;
};

static void recvCb(struct raft_io *io, struct raft_message *m1)
{
    struct result *result = io->data;
    struct raft_message *m2 = result->message;
    unsigned i;
    munit_assert_int(m1->type, ==, m2->type);
    switch (m1->type) {
        case RAFT_IO_REQUEST_VOTE:
            munit_assert_int(m1->request_vote.term, ==, m2->request_vote.term);
            munit_assert_int(m1->request_vote.candidate_id, ==,
                             m2->request_vote.candidate_id);
            munit_assert_int(m1->request_vote.last_log_index, ==,
                             m2->request_vote.last_log_index);
            munit_assert_int(m1->request_vote.last_log_term, ==,
                             m2->request_vote.last_log_term);
            munit_assert_int(m1->request_vote.disrupt_leader, ==,
                             m2->request_vote.disrupt_leader);
            break;
        case RAFT_IO_REQUEST_VOTE_RESULT:
            munit_assert_int(m1->request_vote_result.term, ==,
                             m2->request_vote_result.term);
            munit_assert_int(m1->request_vote_result.vote_granted, ==,
                             m2->request_vote_result.vote_granted);
            break;
        case RAFT_IO_APPEND_ENTRIES:
            munit_assert_int(m1->append_entries.n_entries, ==,
                             m2->append_entries.n_entries);
            for (i = 0; i < m1->append_entries.n_entries; i++) {
                struct raft_entry *entry1 = &m1->append_entries.entries[i];
                struct raft_entry *entry2 = &m2->append_entries.entries[i];
                munit_assert_int(entry1->term, ==, entry2->term);
                munit_assert_int(entry1->type, ==, entry2->type);
                munit_assert_int(entry1->buf.len, ==, entry2->buf.len);
                munit_assert_int(
                    memcmp(entry1->buf.base, entry2->buf.base, entry1->buf.len),
                    ==, 0);
            }
            if (m1->append_entries.n_entries > 0) {
                raft_free(m1->append_entries.entries[0].batch);
                raft_free(m1->append_entries.entries);
            }
            break;
        case RAFT_IO_APPEND_ENTRIES_RESULT:
            munit_assert_int(m1->append_entries_result.term, ==,
                             m2->append_entries_result.term);
            munit_assert_int(m1->append_entries_result.rejected, ==,
                             m2->append_entries_result.rejected);
            munit_assert_int(m1->append_entries_result.last_log_index, ==,
                             m2->append_entries_result.last_log_index);
            break;
        case RAFT_IO_INSTALL_SNAPSHOT:
            munit_assert_int(m1->install_snapshot.conf.n, ==,
                             m2->install_snapshot.conf.n);
            for (i = 0; i < m1->install_snapshot.conf.n; i++) {
                struct raft_server *s1 = &m1->install_snapshot.conf.servers[i];
                struct raft_server *s2 = &m2->install_snapshot.conf.servers[i];
                munit_assert_int(s1->id, ==, s2->id);
                munit_assert_string_equal(s1->address, s2->address);
                munit_assert_int(s1->role, ==, s2->role);
            }
            munit_assert_int(m1->install_snapshot.data.len, ==,
                             m2->install_snapshot.data.len);
            munit_assert_int(memcmp(m1->install_snapshot.data.base,
                                    m2->install_snapshot.data.base,
                                    m2->install_snapshot.data.len),
                             ==, 0);
            raft_configuration_close(&m1->install_snapshot.conf);
            raft_free(m1->install_snapshot.data.base);
            break;
        case RAFT_IO_TIMEOUT_NOW:
            munit_assert_int(m1->timeout_now.term, ==, m2->timeout_now.term);
            munit_assert_int(m1->timeout_now.last_log_index, ==,
                             m2->timeout_now.last_log_index);
            munit_assert_int(m1->timeout_now.last_log_term, ==,
                             m2->timeout_now.last_log_term);
            break;
    };
    result->done = true;
}

static void peerSendCb(struct raft_io_send *req, int status)
{
    bool *done = req->data;
    munit_assert_int(status, ==, 0);
    *done = true;
}

static void peerCloseCb(struct raft_io *io)
{
    bool *done = io->data;
    *done = true;
}

/* Set up the fixture's peer raft_io instance. */
#define PEER_SETUP                                                 \
    do {                                                           \
        struct uv_loop_s *_loop = &f->peer.loop;                   \
        struct raft_uv_transport *_transport = &f->peer.transport; \
        struct raft_io *_io = &f->peer.io;                         \
        int _rv;                                                   \
        _rv = uv_loop_init(_loop);                                 \
        munit_assert_int(_rv, ==, 0);                              \
        _transport->version = 1;                                   \
        _rv = raft_uv_tcp_init(_transport, _loop);                 \
        munit_assert_int(_rv, ==, 0);                              \
        _rv = raft_uv_init(_io, _loop, f->dir, _transport);        \
        munit_assert_int(_rv, ==, 0);                              \
        _rv = _io->init(_io, 2, "127.0.0.1:9002");                 \
        munit_assert_int(_rv, ==, 0);                              \
    } while (0)

/* Tear down the fixture's peer raft_io instance. */
#define PEER_TEAR_DOWN                                             \
    do {                                                           \
        struct uv_loop_s *_loop = &f->peer.loop;                   \
        struct raft_uv_transport *_transport = &f->peer.transport; \
        struct raft_io *_io = &f->peer.io;                         \
        bool _done = false;                                        \
        int _i;                                                    \
        _done = false;                                             \
        _io->data = &_done;                                        \
        _io->close(_io, peerCloseCb);                              \
        for (_i = 0; _i < 10; _i++) {                              \
            if (_done) {                                           \
                break;                                             \
            }                                                      \
            uv_run(_loop, UV_RUN_ONCE);                            \
        }                                                          \
        uv_run(_loop, UV_RUN_DEFAULT);                             \
        munit_assert_true(_done);                                  \
        raft_uv_close(_io);                                        \
        raft_uv_tcp_close(_transport);                             \
        uv_loop_close(_loop);                                      \
    } while (0)

/* Send a message to the main fixture's raft_io instance using the fixture's
 * peer instance. */
#define PEER_SEND(MESSAGE)                                \
    do {                                                  \
        struct uv_loop_s *_loop = &f->peer.loop;          \
        struct raft_io *_io = &f->peer.io;                \
        struct raft_io_send _req;                         \
        bool _done = false;                               \
        int _i;                                           \
        int _rv;                                          \
        (MESSAGE)->server_id = 1;                         \
        (MESSAGE)->server_address = "127.0.0.1:9001";     \
        _req.data = &_done;                               \
        _rv = _io->send(_io, &_req, MESSAGE, peerSendCb); \
        munit_assert_int(_rv, ==, 0);                     \
        for (_i = 0; _i < 10; _i++) {                     \
            if (_done) {                                  \
                break;                                    \
            }                                             \
            uv_run(_loop, UV_RUN_ONCE);                   \
        }                                                 \
        munit_assert_true(_done);                         \
    } while (0)

/* Establish a connection and send an handshake using plain TCP. */
#define PEER_HANDSHAKE                                             \
    do {                                                           \
        uint8_t _handshake[] = {                                   \
            6, 6, 6, 0, 0, 0, 0, 0, /* Protocol */                 \
            1, 0, 0, 0, 0, 0, 0, 0, /* Server ID */                \
            2, 0, 0, 0, 0, 0, 0, 0, /* Address length, in words */ \
            0, 0, 0, 0, 0, 0, 0, 0, /* First address word */       \
            0, 0, 0, 0, 0, 0, 0, 0  /* Second address word */      \
        };                                                         \
        sprintf((char *)&_handshake[24], "127.0.0.1:666");         \
        TCP_CLIENT_CONNECT(9001);                                  \
        TCP_CLIENT_SEND(_handshake, sizeof _handshake);            \
    } while (0);

/* Run the loop until a new message is received. Assert that the received
 * message matches the given one. */
#define RECV(MESSAGE)                             \
    do {                                          \
        struct result _result = {MESSAGE, false}; \
        f->io.data = &_result;                    \
        LOOP_RUN_UNTIL(&_result.done);            \
        f->io.data = NULL;                        \
    } while (0)

/******************************************************************************
 *
 * Set up and tear down.
 *
 *****************************************************************************/

static void *setUpDeps(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_UV_DEPS;
    SETUP_TCP;
    PEER_SETUP;
    f->io.data = f;
    f->closed = false;
    return f;
}

static void tearDownDeps(void *data)
{
    struct fixture *f = data;
    PEER_TEAR_DOWN;
    TEAR_DOWN_TCP;
    TEAR_DOWN_UV_DEPS;
    free(f);
}

static void *setUp(const MunitParameter params[], void *user_data)
{
    struct fixture *f = setUpDeps(params, user_data);
    int rv;
    SETUP_UV;
    f->io.data = f;
    rv = f->io.start(&f->io, 10000, NULL, recvCb);
    munit_assert_int(rv, ==, 0);
    return f;
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_UV;
    tearDownDeps(f);
}

/******************************************************************************
 *
 * raft_io_recv_cb
 *
 *****************************************************************************/

SUITE(recv)

/* Receive the very first message over the connection. */
TEST(recv, first, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_message message;
    message.type = RAFT_IO_REQUEST_VOTE;
    message.request_vote.candidate_id = 2;
    message.request_vote.last_log_index = 123;
    message.request_vote.last_log_term = 2;
    message.request_vote.disrupt_leader = false;
    PEER_SEND(&message);
    RECV(&message);
    return MUNIT_OK;
}

/* Receive the a first message then another one. */
TEST(recv, second, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_message message;
    message.type = RAFT_IO_REQUEST_VOTE;
    message.request_vote.candidate_id = 2;
    message.request_vote.last_log_index = 123;
    message.request_vote.last_log_term = 2;
    message.request_vote.disrupt_leader = true;
    PEER_SEND(&message);
    RECV(&message);
    PEER_SEND(&message);
    RECV(&message);
    return MUNIT_OK;
}

/* Receive a RequestVote result message. */
TEST(recv, requestVoteResult, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_message message;
    message.type = RAFT_IO_REQUEST_VOTE_RESULT;
    message.request_vote_result.term = 3;
    message.request_vote_result.vote_granted = true;
    message.request_vote_result.pre_vote = false;
    PEER_SEND(&message);
    RECV(&message);
    return MUNIT_OK;
}

/* Receive an AppendEntries message with two entries. */
TEST(recv, appendEntries, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry entries[2];
    struct raft_message message;
    uint8_t data1[8] = {1, 2, 3, 4, 5, 6, 7, 8};
    uint8_t data2[8] = {8, 7, 6, 5, 4, 3, 2, 1};

    entries[0].type = RAFT_COMMAND;
    entries[0].buf.base = data1;
    entries[0].buf.len = sizeof data1;

    entries[1].type = RAFT_COMMAND;
    entries[1].buf.base = data2;
    entries[1].buf.len = sizeof data2;

    message.type = RAFT_IO_APPEND_ENTRIES;
    message.append_entries.entries = entries;
    message.append_entries.n_entries = 2;

    PEER_SEND(&message);
    RECV(&message);

    return MUNIT_OK;
}

/* Receive an AppendEntries message with no entries (i.e. an heartbeat). */
TEST(recv, heartbeat, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_message message;
    message.type = RAFT_IO_APPEND_ENTRIES;
    message.append_entries.entries = NULL;
    message.append_entries.n_entries = 0;
    PEER_SEND(&message);
    RECV(&message);
    return MUNIT_OK;
}

/* Receive an AppendEntries result f->peer.message. */
TEST(recv, appendEntriesResult, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_message message;
    message.type = RAFT_IO_APPEND_ENTRIES_RESULT;
    message.append_entries_result.term = 3;
    message.append_entries_result.rejected = 0;
    message.append_entries_result.last_log_index = 123;
    PEER_SEND(&message);
    RECV(&message);
    return MUNIT_OK;
}

/* Receive an InstallSnapshot message. */
TEST(recv, installSnapshot, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_message message;
    uint8_t snapshot_data[8] = {1, 2, 3, 4, 5, 6, 7, 8};
    int rv;

    message.type = RAFT_IO_INSTALL_SNAPSHOT;
    message.install_snapshot.term = 2;
    message.install_snapshot.last_index = 123;
    message.install_snapshot.last_term = 1;
    raft_configuration_init(&message.install_snapshot.conf);
    rv = raft_configuration_add(&message.install_snapshot.conf, 1, "1",
                                RAFT_VOTER);
    munit_assert_int(rv, ==, 0);
    message.install_snapshot.data.len = sizeof snapshot_data;
    message.install_snapshot.data.base = snapshot_data;

    PEER_SEND(&message);
    RECV(&message);

    raft_configuration_close(&message.install_snapshot.conf);

    return MUNIT_OK;
}

/* Receive a TimeoutNow message. */
TEST(recv, timeoutNow, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_message message;
    message.type = RAFT_IO_TIMEOUT_NOW;
    message.timeout_now.term = 3;
    message.timeout_now.last_log_index = 123;
    message.timeout_now.last_log_term = 2;
    PEER_SEND(&message);
    RECV(&message);
    return MUNIT_OK;
}

/* The handshake fails because of an unexpected protocon version. */
TEST(recv, badProtocol, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    uint8_t handshake[] = {
        6, 6, 6, 0, 0, 0, 0, 0, /* Protocol */
        1, 0, 0, 0, 0, 0, 0, 0, /* Server ID */
        2, 0, 0, 0, 0, 0, 0, 0  /* Address length */
    };
    TCP_CLIENT_CONNECT(9001);
    TCP_CLIENT_SEND(handshake, sizeof handshake);
    LOOP_RUN(2);
    return MUNIT_OK;
}

/* A message can't have zero length. */
TEST(recv, badSize, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    uint8_t header[] = {
        1, 0, 0, 0, 0, 0, 0, 0, /* Message type */
        0, 0, 0, 0, 0, 0, 0, 0  /* Message size */
    };
    PEER_HANDSHAKE;
    TCP_CLIENT_SEND(header, sizeof header);
    LOOP_RUN(2);
    return MUNIT_OK;
}

/* A message with a bad type causes the connection to be aborted. */
TEST(recv, badType, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    uint8_t header[] = {
        1, 2, 3, 4, 5, 6, 7, 8, /* Message type */
        0, 0, 0, 0, 0, 0, 0, 0  /* Message size */
    };
    PEER_HANDSHAKE;
    TCP_CLIENT_SEND(header, sizeof header);
    LOOP_RUN(2);
    return MUNIT_OK;
}

/* The backend is closed just before accepting a new connection. */
TEST(recv, closeBeforeAccept, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    uint8_t header[] = {
        1, 2, 3, 4, 5, 6, 7, 8, /* Message type */
        0, 0, 0, 0, 0, 0, 0, 0  /* Message size */
    };
    PEER_HANDSHAKE;
    TCP_CLIENT_SEND(header, sizeof header);
    LOOP_RUN(1);
    TEAR_DOWN_UV;
    return MUNIT_OK;
}

/* The backend is closed after receiving the header of an AppendEntries
 * message. */
TEST(recv, closeAfterAppendEntriesHeader, setUp, tearDown, 0, NULL)
{
    /* TODO */
    return MUNIT_SKIP;
}
