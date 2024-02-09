#include <unistd.h>

#include "../lib/runner.h"
#include "../lib/tcp.h"
#include "../lib/uv.h"

/******************************************************************************
 *
 * Fixture with a libuv-based raft_io instance and some pre-set messages.
 *
 *****************************************************************************/

#define N_MESSAGES 5

struct fixture
{
    FIXTURE_UV_DEPS;
    FIXTURE_TCP_SERVER;
    FIXTURE_UV;
    struct raft_message messages[N_MESSAGES];
};

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

struct result
{
    int status;
    bool done;
};

static void sendCbAssertResult(struct raft_io_send *req, int status)
{
    struct result *result = req->data;
    munit_assert_int(status, ==, result->status);
    result->done = true;
}

/* Get I'th fixture's message. */
#define MESSAGE(I) (&f->messages[I])

/* Submit a send request for the I'th fixture's message. */
#define SEND_SUBMIT(I, RV, STATUS)                                         \
    struct raft_io_send _req##I;                                           \
    struct result _result##I = {STATUS, false};                            \
    int _rv##I;                                                            \
    _req##I.data = &_result##I;                                            \
    _rv##I =                                                               \
        f->io.send(&f->io, &_req##I, &f->messages[I], sendCbAssertResult); \
    munit_assert_int(_rv##I, ==, RV)

/* Wait for the submit request of the I'th message to finish. */
#define SEND_WAIT(I) LOOP_RUN_UNTIL(&_result##I.done)

/* Submit a send request for the I'th fixture's message and wait for the
 * operation to successfully complete. */
#define SEND(I)                                     \
    do {                                            \
        SEND_SUBMIT(I, 0 /* rv */, 0 /* status */); \
        SEND_WAIT(I);                               \
    } while (0)

/* Submit a send request and assert that it fails synchronously with the
 * given error code and message. */
#define SEND_ERROR(I, RV, ERRMSG)                                    \
    do {                                                             \
        SEND_SUBMIT(I, RV, 0 /* status */);                          \
        /* munit_assert_string_equal(f->transport.errmsg, ERRMSG);*/ \
    } while (0)

/* Submit a send request and wait for the operation to fail with the given code
 * and message. */
#define SEND_FAILURE(I, STATUS, ERRMSG)                             \
    do {                                                            \
        SEND_SUBMIT(I, 0 /* rv */, STATUS);                         \
        SEND_WAIT(I);                                               \
        /*munit_assert_string_equal(f->transport.errmsg, ERRMSG);*/ \
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
    SETUP_TCP_SERVER;
    f->io.data = f;
    return f;
}

static void tearDownDeps(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_TCP_SERVER;
    TEAR_DOWN_UV_DEPS;
    free(f);
}

static void *setUp(const MunitParameter params[], void *user_data)
{
    struct fixture *f = setUpDeps(params, user_data);
    unsigned i;
    SETUP_UV;
    raft_uv_set_connect_retry_delay(&f->io, 1);
    for (i = 0; i < N_MESSAGES; i++) {
        struct raft_message *message = &f->messages[i];
        message->type = RAFT_IO_REQUEST_VOTE;
        message->server_id = 1;
        message->server_address = f->server.address;
    }
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
 * raft_io->send()
 *
 *****************************************************************************/

SUITE(send)

/* The first time a request is sent to a server a connection attempt is
 * triggered. If the connection succeeds the request gets written out. */
TEST(send, first, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    SEND(0);
    return MUNIT_OK;
}

/* The second time a request is sent it re-uses the connection that was already
 * established */
TEST(send, second, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    SEND(0);
    SEND(0);
    return MUNIT_OK;
}

/* Submit a few send requests in parallel. */
TEST(send, parallel, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    SEND_SUBMIT(0 /* message */, 0 /* rv */, 0 /* status */);
    SEND_SUBMIT(1 /* message */, 0 /* rv */, 0 /* status */);
    SEND_WAIT(0);
    SEND_WAIT(1);
    return MUNIT_OK;
}

/* Send a request vote result message. */
TEST(send, voteResult, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    MESSAGE(0)->type = RAFT_IO_REQUEST_VOTE_RESULT;
    SEND(0);
    return MUNIT_OK;
}

/* Send an append entries message. */
TEST(send, appendEntries, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry entries[2];
    entries[0].buf.base = raft_malloc(16);
    entries[0].buf.len = 16;
    entries[1].buf.base = raft_malloc(8);
    entries[1].buf.len = 8;

    MESSAGE(0)->type = RAFT_IO_APPEND_ENTRIES;
    MESSAGE(0)->append_entries.entries = entries;
    MESSAGE(0)->append_entries.n_entries = 2;

    SEND(0);

    raft_free(entries[0].buf.base);
    raft_free(entries[1].buf.base);

    return MUNIT_OK;
}

/* Send an append entries message with zero entries (i.e. a heartbeat). */
TEST(send, heartbeat, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    MESSAGE(0)->type = RAFT_IO_APPEND_ENTRIES;
    MESSAGE(0)->append_entries.entries = NULL;
    MESSAGE(0)->append_entries.n_entries = 0;
    SEND(0);
    return MUNIT_OK;
}

/* Send an append entries result message. */
TEST(send, appendEntriesResult, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    MESSAGE(0)->type = RAFT_IO_APPEND_ENTRIES_RESULT;
    SEND(0);
    return MUNIT_OK;
}

/* Send an install snapshot message. */
TEST(send, installSnapshot, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_install_snapshot *p = &MESSAGE(0)->install_snapshot;
    int rv;

    MESSAGE(0)->type = RAFT_IO_INSTALL_SNAPSHOT;

    raft_configuration_init(&p->conf);
    rv = raft_configuration_add(&p->conf, 1, "1", RAFT_VOTER);
    munit_assert_int(rv, ==, 0);

    p->data.len = 8;
    p->data.base = raft_malloc(p->data.len);

    SEND(0);

    raft_configuration_close(&p->conf);
    raft_free(p->data.base);

    return MUNIT_OK;
}

/* A connection attempt fails asynchronously after the connect function
 * returns. */
TEST(send, noConnection, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    MESSAGE(0)->server_address = "127.0.0.1:123456";
    SEND_SUBMIT(0 /* message */, 0 /* rv */, RAFT_CANCELED /* status */);
    TEAR_DOWN_UV;
    return MUNIT_OK;
}

/* The message has an invalid IPv4 address. */
TEST(send, badAddress, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    MESSAGE(0)->server_address = "1";
    SEND_SUBMIT(0 /* message */, 0 /* rv */, RAFT_CANCELED /* status */);
    TEAR_DOWN_UV;
    return MUNIT_OK;
}

/* Make sure UvSend doesn't use a stale connection for a certain server id
 * by first sending a message to a valid address and then sending a message to
 * an invalid address, making sure the valid connection is not reused.
 * Afterwards assert that a send to the correct address still succeeds. */
TEST(send, changeToUnconnectedAddress, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;

    /* Send a message to a server and a connected address */
    SEND(0);

    /* Send a message to the same server, but update the address to an
     * unconnected address and assert it fails. */
    munit_assert_ullong(MESSAGE(0)->server_id, ==, MESSAGE(1)->server_id);
    MESSAGE(1)->server_address = "127.0.0.2:1";
    SEND_SUBMIT(1 /* message */, 0 /* rv */, RAFT_CANCELED /* status */);

    /* Send another message to the same server and connected address */
    munit_assert_ullong(MESSAGE(0)->server_id, ==, MESSAGE(2)->server_id);
    SEND(2);

    /* Send another message to the same server and connected address */
    munit_assert_ullong(MESSAGE(0)->server_id, ==, MESSAGE(3)->server_id);
    SEND(3);

    TEAR_DOWN_UV;
    return MUNIT_OK;
}

/* The message has an invalid type. */
TEST(send, badMessage, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    MESSAGE(0)->type = 666;
    SEND_ERROR(0, RAFT_MALFORMED, "");
    return MUNIT_OK;
}

/* Old send requests that have accumulated and could not yet be sent are
 * progressively evicted. */
TEST(send, evictOldPending, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    TCP_SERVER_STOP;
    SEND_SUBMIT(0 /* message */, 0 /* rv */, RAFT_NOCONNECTION /* status */);
    SEND_SUBMIT(1 /* message */, 0 /* rv */, RAFT_CANCELED /* status */);
    SEND_SUBMIT(2 /* message */, 0 /* rv */, RAFT_CANCELED /* status */);
    SEND_SUBMIT(3 /* message */, 0 /* rv */, RAFT_CANCELED /* status */);
    SEND_WAIT(0);
    TEAR_DOWN_UV;
    return MUNIT_OK;
}

/* After the connection is established the peer dies and then comes back a
 * little bit later. */
TEST(send, reconnectAfterWriteError, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    int socket;
    SEND(0);
    socket = TcpServerAccept(&f->server);
    close(socket);
    SEND_FAILURE(0, RAFT_IOERR, "");
    SEND(0);
    return MUNIT_OK;
}

/* After the connection is established the peer dies and then comes back a
 * little bit later. At the time the peer died there where several writes
 * pending. */
TEST(send, reconnectAfterMultipleWriteErrors, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    int socket;
    signal(SIGPIPE, SIG_IGN);
    SEND(0);
    socket = TcpServerAccept(&f->server);
    close(socket);
    SEND_SUBMIT(1 /* message */, 0 /* rv */, RAFT_IOERR /* status */);
    SEND_SUBMIT(2 /* message */, 0 /* rv */, RAFT_IOERR /* status */);
    SEND_WAIT(1);
    SEND_WAIT(2);
    SEND(3);
    return MUNIT_OK;
}

static char *oomHeapFaultDelay[] = {"0", "1", "2", "3", "4", NULL};
static char *oomHeapFaultRepeat[] = {"1", NULL};

static MunitParameterEnum oomParams[] = {
    {TEST_HEAP_FAULT_DELAY, oomHeapFaultDelay},
    {TEST_HEAP_FAULT_REPEAT, oomHeapFaultRepeat},
    {NULL, NULL},
};

/* Out of memory conditions. */
TEST(send, oom, setUp, tearDown, 0, oomParams)
{
    struct fixture *f = data;
    HEAP_FAULT_ENABLE;
    SEND_ERROR(0, RAFT_NOMEM, "");
    return MUNIT_OK;
}

static char *oomAsyncHeapFaultDelay[] = {"2", NULL};
static char *oomAsyncHeapFaultRepeat[] = {"1", NULL};

static MunitParameterEnum oomAsyncParams[] = {
    {TEST_HEAP_FAULT_DELAY, oomAsyncHeapFaultDelay},
    {TEST_HEAP_FAULT_REPEAT, oomAsyncHeapFaultRepeat},
    {NULL, NULL},
};

/* Transient out of memory error happening after send() has returned. */
TEST(send, oomAsync, setUp, tearDown, 0, oomAsyncParams)
{
    struct fixture *f = data;
    SEND(0);
    return MUNIT_OK;
}

/* The backend gets closed while there is a pending write. */
TEST(send, closeDuringWrite, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry;

    /* Set a very large message that is likely to fill the socket buffer.
     * TODO: figure a more deterministic way to choose the value. */
    entry.buf.len = 1024 * 1024 * 8;
    entry.buf.base = raft_malloc(entry.buf.len);

    MESSAGE(0)->type = RAFT_IO_APPEND_ENTRIES;
    MESSAGE(0)->append_entries.entries = &entry;
    MESSAGE(0)->append_entries.n_entries = 1;

    SEND_SUBMIT(0 /* message */, 0 /* rv */, RAFT_CANCELED /* status */);
    TEAR_DOWN_UV;

    raft_free(entry.buf.base);

    return MUNIT_OK;
}

/* The backend gets closed while there is a pending connect request. */
TEST(send, closeDuringConnection, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    SEND_SUBMIT(0 /* message */, 0 /* rv */, RAFT_CANCELED /* status */);
    TEAR_DOWN_UV;
    return MUNIT_OK;
}
