#include "../../../src/raft.h"
#include "../../../src/raft/byte.h"
#include "../lib/addrinfo.h"
#include "../lib/heap.h"
#include "../lib/loop.h"
#include "../lib/runner.h"
#include "../lib/tcp.h"

/******************************************************************************
 *
 * Fixture with a TCP-based raft_uv_transport.
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_HEAP;
    FIXTURE_LOOP;
    FIXTURE_TCP;
    struct raft_uv_transport transport;
    bool accepted;
    bool closed;
    struct
    {
        uint8_t buf[sizeof(uint64_t) + /* Protocol version */
                    sizeof(uint64_t) + /* Server ID */
                    sizeof(uint64_t) + /* Length of address */
                    sizeof(uint64_t) * 2 /* Address */];
        size_t offset;
    } handshake;
};

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

#define PEER_ID 2
#define PEER_ADDRESS "127.0.0.1:666"

static void closeCb(struct raft_uv_transport *transport)
{
    struct fixture *f = transport->data;
    f->closed = true;
}

static void acceptCb(struct raft_uv_transport *t,
                     raft_id id,
                     const char *address,
                     struct uv_stream_s *stream)
{
    struct fixture *f = t->data;
    munit_assert_int(id, ==, PEER_ID);
    munit_assert_string_equal(address, PEER_ADDRESS);
    f->accepted = true;
    uv_close((struct uv_handle_s *)stream, (uv_close_cb)raft_free);
}

#define INIT                                                                  \
    do {                                                                      \
        int _rv;                                                              \
        f->transport.version = 1;                                             \
        _rv = raft_uv_tcp_init(&f->transport, &f->loop);                      \
        munit_assert_int(_rv, ==, 0);                                         \
        const char *bind_addr = munit_parameters_get(params, "bind-address"); \
        if (bind_addr && strlen(bind_addr)) {                                 \
            _rv = raft_uv_tcp_set_bind_address(&f->transport, bind_addr);     \
            munit_assert_int(_rv, ==, 0);                                     \
        }                                                                     \
        const char *address = munit_parameters_get(params, "address");        \
        if (!address) {                                                       \
            address = "127.0.0.1:9000";                                       \
        }                                                                     \
        _rv = f->transport.init(&f->transport, 1, address);                   \
        munit_assert_int(_rv, ==, 0);                                         \
        f->transport.data = f;                                                \
        f->closed = false;                                                    \
    } while (0)

#define CLOSE                                       \
    do {                                            \
        f->transport.close(&f->transport, closeCb); \
        LOOP_RUN_UNTIL(&f->closed);                 \
        raft_uv_tcp_close(&f->transport);           \
    } while (0)

/******************************************************************************
 *
 * Set up and tear down.
 *
 *****************************************************************************/

static void *setUpDeps(const MunitParameter params[],
                       MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SET_UP_ADDRINFO;
    SET_UP_HEAP;
    SETUP_LOOP;
    SETUP_TCP;
    return f;
}

static void tearDownDeps(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_TCP;
    TEAR_DOWN_LOOP;
    TEAR_DOWN_HEAP;
    TEAR_DOWN_ADDRINFO;
    free(f);
}

static void *setUp(const MunitParameter params[], void *user_data)
{
    struct fixture *f = setUpDeps(params, user_data);
    void *cursor;
    /* test_tcp_listen(&f->tcp); */
    INIT;
    f->accepted = false;
    f->handshake.offset = 0;

    cursor = f->handshake.buf;
    bytePut64(&cursor, 1);
    bytePut64(&cursor, PEER_ID);
    bytePut64(&cursor, 16);
    strcpy(cursor, PEER_ADDRESS);

    return f;
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    CLOSE;
    tearDownDeps(f);
}

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

#define LISTEN(EXPECTED_RV)                                \
    do {                                                   \
        int rv;                                            \
        rv = f->transport.listen(&f->transport, acceptCb); \
        munit_assert_int(rv, ==, EXPECTED_RV);             \
    } while (false)

/* Connect to the listening socket of the transport, creating a new connection
 * that is waiting to be accepted. */
#define PEER_CONNECT TCP_CLIENT_CONNECT(9000)

/* Make the peer close the connection. */
#define PEER_CLOSE TCP_CLIENT_CLOSE

/* Make the connected client send handshake data. */
#define PEER_HANDSHAKE                        \
    do {                                      \
        size_t n = sizeof f->handshake.buf;   \
        TCP_CLIENT_SEND(f->handshake.buf, n); \
    } while (0)

/* Make the connected client send partial handshake data: only N bytes will be
 * sent, starting from the offset of the last call. */
#define PEER_HANDSHAKE_PARTIAL(N)                                   \
    do {                                                            \
        TCP_CLIENT_SEND(f->handshake.buf + f->handshake.offset, N); \
    } while (0)

/* After a PEER_CONNECT() call, spin the event loop until the connected
 * callback of the listening TCP handle gets called. */
#define LOOP_RUN_UNTIL_CONNECTED LOOP_RUN(1);

/* After a PEER_HANDSHAKE_PARTIAL() call, spin the event loop until the read
 * callback gets called. */
#define LOOP_RUN_UNTIL_READ LOOP_RUN(1);

/* Spin the event loop until the accept callback gets eventually invoked. */
#define ACCEPT LOOP_RUN_UNTIL(&f->accepted);

/******************************************************************************
 *
 * Success scenarios.
 *
 *****************************************************************************/

SUITE(tcp_listen)

/* Check success with addrinfo resolve to mutiple IP and first one is used to
 * connect */
TEST(tcp_listen, firstOfTwo, setUp, tearDown, 0, NULL)
{
    const struct AddrinfoResult results[] = {{"127.0.0.1", 9000},
                                             {"127.0.0.2", 9000}};
    struct fixture *f = data;
    AddrinfoInjectSetResponse(0, 2, results);
    LISTEN(0);
    PEER_CONNECT;
    PEER_HANDSHAKE;
    ACCEPT;
    return MUNIT_OK;
}

/* Check success with addrinfo resolve to mutiple IP and second one is used to
 * connect */
TEST(tcp_listen, secondOfTwo, setUp, tearDown, 0, NULL)
{
    const struct AddrinfoResult results[] = {{"127.0.0.2", 9000},
                                             {"127.0.0.1", 9000}};
    struct fixture *f = data;
    AddrinfoInjectSetResponse(0, 2, results);

    LISTEN(0);
    PEER_CONNECT;
    PEER_HANDSHAKE;
    ACCEPT;
    return MUNIT_OK;
}

/* Simulate port already in use error by addrinfo response contain the same IP
 * twice */
TEST(tcp_listen, alreadyBound, setUp, tearDown, 0, NULL)
{
    /* We need to use the same endpoint three times as a simple duplicate will
     * be skipped due to a glib strange behavior
     * https://bugzilla.redhat.com/show_bug.cgi?id=496300  */
    const struct AddrinfoResult results[] = {
        {"127.0.0.1", 9000}, {"127.0.0.1", 9000}, {"127.0.0.1", 9000}};
    struct fixture *f = data;
    AddrinfoInjectSetResponse(0, 3, results);
    LISTEN(RAFT_IOERR);
    return MUNIT_OK;
}

/* Error in bind first IP address */
TEST(tcp_listen, cannotBindFirst, setUp, tearDown, 0, NULL)
{
    const struct AddrinfoResult results[] = {{"192.0.2.0", 9000},
                                             {"127.0.0.1", 9000}};
    struct fixture *f = data;
    AddrinfoInjectSetResponse(0, 2, results);
    LISTEN(RAFT_IOERR);
    return MUNIT_OK;
}

/* Error in bind of second IP address */
TEST(tcp_listen, cannotBindSecond, setUp, tearDown, 0, NULL)
{
    const struct AddrinfoResult results[] = {{"127.0.0.1", 9000},
                                             {"192.0.2.0", 9000}};
    struct fixture *f = data;
    AddrinfoInjectSetResponse(0, 2, results);
    LISTEN(RAFT_IOERR);
    return MUNIT_OK;
}

/* Check error on general dns server failure */
TEST(tcp_listen, resolveFailure, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    AddrinfoInjectSetResponse(EAI_FAIL, 0, NULL);
    LISTEN(RAFT_IOERR);
    return MUNIT_OK;
}
