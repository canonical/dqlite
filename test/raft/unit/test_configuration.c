#include "../../../src/raft/byte.h"
#include "../../../src/raft/configuration.h"
#include "../lib/heap.h"
#include "../lib/runner.h"

/******************************************************************************
 *
 * Fixture
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_HEAP;
    struct raft_configuration configuration;
};

static void *setUp(const MunitParameter params[], MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SET_UP_HEAP;
    configurationInit(&f->configuration);
    return f;
}

static void tearDownNoClose(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_HEAP;
    free(f);
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    configurationClose(&f->configuration);
    tearDownNoClose(data);
}

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

/* Accessors */
#define VOTER_COUNT configurationVoterCount(&f->configuration)
#define INDEX_OF(ID) configurationIndexOf(&f->configuration, ID)
#define INDEX_OF_VOTER(ID) configurationIndexOfVoter(&f->configuration, ID)
#define GET(ID) configurationGet(&f->configuration, ID)

/* Add a server to the fixture's configuration. */
#define ADD_RV(ID, ADDRESS, ROLE) \
    configurationAdd(&f->configuration, ID, ADDRESS, ROLE)
#define ADD(...) munit_assert_int(ADD_RV(__VA_ARGS__), ==, 0)
#define ADD_ERROR(RV, ...) munit_assert_int(ADD_RV(__VA_ARGS__), ==, RV)

/* Remove a server from the fixture's configuration */
#define REMOVE_RV(ID) configurationRemove(&f->configuration, ID)
#define REMOVE(...) munit_assert_int(REMOVE_RV(__VA_ARGS__), ==, 0)
#define REMOVE_ERROR(RV, ...) munit_assert_int(REMOVE_RV(__VA_ARGS__), ==, RV)

/* Copy the fixture's configuration into the given one. */
#define COPY_RV(CONF) configurationCopy(&f->configuration, CONF)
#define COPY(...) munit_assert_int(COPY_RV(__VA_ARGS__), ==, 0)
#define COPY_ERROR(RV, ...) munit_assert_int(COPY_RV(__VA_ARGS__), ==, RV)

/* Encode the fixture's configuration into the given buffer. */
#define ENCODE_RV(BUF) configurationEncode(&f->configuration, BUF)
#define ENCODE(...) munit_assert_int(ENCODE_RV(__VA_ARGS__), ==, 0)
#define ENCODE_ERROR(RV, ...) munit_assert_int(ENCODE_RV(__VA_ARGS__), ==, RV)

/* Decode the given buffer into the fixture's configuration. */
#define DECODE_RV(BUF) configurationDecode(BUF, &f->configuration)
#define DECODE(...) munit_assert_int(DECODE_RV(__VA_ARGS__), ==, 0)
#define DECODE_ERROR(RV, ...) munit_assert_int(DECODE_RV(__VA_ARGS__), ==, RV)

/******************************************************************************
 *
 * Assertions
 *
 *****************************************************************************/

/* Assert that the fixture's configuration has n servers. */
#define ASSERT_N(N)                                              \
    {                                                            \
        munit_assert_int(f->configuration.n, ==, N);             \
        if (N == 0) {                                            \
            munit_assert_ptr_null(f->configuration.servers);     \
        } else {                                                 \
            munit_assert_ptr_not_null(f->configuration.servers); \
        }                                                        \
    }

/* Assert that the attributes of the I'th server in the fixture's configuration
 * match the given values. */
#define ASSERT_SERVER(I, ID, ADDRESS, ROLE)                  \
    {                                                        \
        struct raft_server *server;                          \
        munit_assert_int(I, <, f->configuration.n);          \
        server = &f->configuration.servers[I];               \
        munit_assert_int(server->id, ==, ID);                \
        munit_assert_string_equal(server->address, ADDRESS); \
        munit_assert_int(server->role, ==, ROLE);            \
    }

/******************************************************************************
 *
 * configurationVoterCount
 *
 *****************************************************************************/

SUITE(configurationVoterCount)

/* All servers are voting. */
TEST(configurationVoterCount, all_voters, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    ADD(1, "192.168.1.1:666", RAFT_VOTER);
    ADD(2, "192.168.1.2:666", RAFT_VOTER);
    munit_assert_int(VOTER_COUNT, ==, 2);
    return MUNIT_OK;
}

/* Return only voting servers. */
TEST(configurationVoterCount, filter, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    ADD(1, "192.168.1.1:666", RAFT_VOTER);
    ADD(2, "192.168.1.2:666", RAFT_STANDBY);
    munit_assert_int(VOTER_COUNT, ==, 1);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * configurationIndexOf
 *
 *****************************************************************************/

SUITE(configurationIndexOf)

/* If a matching server is found, it's index is returned. */
TEST(configurationIndexOf, match, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    ADD(1, "192.168.1.1:666", RAFT_VOTER);
    ADD(2, "192.168.1.2:666", RAFT_STANDBY);
    munit_assert_int(INDEX_OF(2), ==, 1);
    return MUNIT_OK;
}

/* If no matching server is found, the length of the configuration is
 * returned. */
TEST(configurationIndexOf, no_match, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    ADD(1, "127.0.0.1:666", RAFT_VOTER);
    munit_assert_int(INDEX_OF(3), ==, f->configuration.n);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * configurationIndexOfVoter
 *
 *****************************************************************************/

SUITE(configurationIndexOfVoter)

/* The index of the matching voting server (relative to the number of voting
   servers) is returned. */
TEST(configurationIndexOfVoter, match, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    ADD(1, "192.168.1.1:666", RAFT_STANDBY);
    ADD(2, "192.168.1.2:666", RAFT_VOTER);
    ADD(3, "192.168.1.3:666", RAFT_VOTER);
    munit_assert_int(INDEX_OF_VOTER(3), ==, 1);
    return MUNIT_OK;
}

/* If no matching server is found, the length of the configuration is
 * returned. */
TEST(configurationIndexOfVoter, no_match, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    ADD(1, "192.168.1.1:666", RAFT_VOTER);
    munit_assert_int(INDEX_OF_VOTER(3), ==, 1);
    return MUNIT_OK;
}

/* If the server exists but is non-voting, the length of the configuration is
 * returned. */
TEST(configurationIndexOfVoter, non_voting, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    ADD(1, "192.168.1.1:666", RAFT_STANDBY);
    munit_assert_int(INDEX_OF_VOTER(1), ==, 1);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * configurationGet
 *
 *****************************************************************************/

SUITE(configurationGet)

/* If a matching server is found, it's returned. */
TEST(configurationGet, match, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    const struct raft_server *server;
    ADD(1, "192.168.1.1:666", RAFT_VOTER);
    ADD(2, "192.168.1.2:666", RAFT_STANDBY);
    server = GET(2);
    munit_assert_ptr_not_null(server);
    munit_assert_int(server->id, ==, 2);
    munit_assert_string_equal(server->address, "192.168.1.2:666");
    return MUNIT_OK;
}

/* If no matching server is found, NULL is returned. */
TEST(configurationGet, no_match, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    ADD(1, "127.0.0.1:666", RAFT_VOTER);
    munit_assert_ptr_null(GET(3));
    return MUNIT_OK;
}

/******************************************************************************
 *
 * configurationCopy
 *
 *****************************************************************************/

SUITE(configurationCopy)

/* Copy a configuration containing two servers */
TEST(configurationCopy, two, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_configuration configuration;
    ADD(1, "192.168.1.1:666", RAFT_STANDBY);
    ADD(2, "192.168.1.2:666", RAFT_VOTER);
    COPY(&configuration);
    munit_assert_int(configuration.n, ==, 2);
    munit_assert_int(configuration.servers[0].id, ==, 1);
    munit_assert_int(configuration.servers[1].id, ==, 2);
    configurationClose(&configuration);
    return MUNIT_OK;
}

static char *copy_oom_heap_fault_delay[] = {"0", "1", "2", NULL};
static char *copy_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum copy_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, copy_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, copy_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory */
TEST(configurationCopy, oom, setUp, tearDown, 0, copy_oom_params)
{
    struct fixture *f = data;
    struct raft_configuration configuration;
    ADD(1, "192.168.1.1:666", RAFT_STANDBY);
    ADD(2, "192.168.1.2:666", RAFT_VOTER);
    HEAP_FAULT_ENABLE;
    COPY_ERROR(RAFT_NOMEM, &configuration);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * raft_configuration_add
 *
 *****************************************************************************/

SUITE(configurationAdd)

/* Add a server to the configuration. */
TEST(configurationAdd, one, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    ADD(1, "127.0.0.1:666", RAFT_VOTER);
    ASSERT_N(1);
    ASSERT_SERVER(0, 1, "127.0.0.1:666", RAFT_VOTER);
    return MUNIT_OK;
}

/* Add two servers to the configuration. */
TEST(configurationAdd, two, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    ADD(1, "127.0.0.1:666", RAFT_VOTER);
    ADD(2, "192.168.1.1:666", RAFT_STANDBY);
    ASSERT_N(2);
    ASSERT_SERVER(0, 1, "127.0.0.1:666", RAFT_VOTER);
    ASSERT_SERVER(1, 2, "192.168.1.1:666", RAFT_STANDBY);
    return MUNIT_OK;
}

/* Add a server with an ID which is already in use. */
TEST(configurationAdd, duplicateId, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    ADD(1, "127.0.0.1:666", RAFT_VOTER);
    ADD_ERROR(RAFT_DUPLICATEID, 1, "192.168.1.1:666", RAFT_STANDBY);
    return MUNIT_OK;
}

/* Add a server with an address which is already in use. */
TEST(configurationAdd, duplicateAddress, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    ADD(1, "127.0.0.1:666", RAFT_VOTER);
    ADD_ERROR(RAFT_DUPLICATEADDRESS, 2, "127.0.0.1:666", RAFT_STANDBY);
    return MUNIT_OK;
}

/* Add a server with an invalid role. */
TEST(configurationAdd, invalidRole, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    ADD_ERROR(RAFT_BADROLE, 2, "127.0.0.1:666", 666);
    return MUNIT_OK;
}

static char *add_oom_heap_fault_delay[] = {"0", "1", NULL};
static char *add_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum add_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, add_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, add_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory. */
TEST(configurationAdd, oom, setUp, tearDown, 0, add_oom_params)
{
    struct fixture *f = data;
    HeapFaultEnable(&f->heap);
    ADD_ERROR(RAFT_NOMEM, 1, "127.0.0.1:666", RAFT_VOTER);
    munit_assert_null(f->configuration.servers);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * configurationRemove
 *
 *****************************************************************************/

SUITE(configurationRemove)

/* Remove the last and only server. */
TEST(configurationRemove, last, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    ADD(1, "127.0.0.1:666", RAFT_VOTER);
    REMOVE(1);
    ASSERT_N(0);
    return MUNIT_OK;
}

/* Remove the first server. */
TEST(configurationRemove, first, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    ADD(1, "127.0.0.1:666", RAFT_VOTER);
    ADD(2, "192.168.1.1:666", RAFT_STANDBY);
    REMOVE(1);
    ASSERT_N(1);
    ASSERT_SERVER(0, 2, "192.168.1.1:666", RAFT_STANDBY);
    return MUNIT_OK;
}

/* Remove a server in the middle. */
TEST(configurationRemove, middle, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    ADD(1, "127.0.0.1:666", RAFT_VOTER);
    ADD(2, "192.168.1.1:666", RAFT_STANDBY);
    ADD(3, "10.0.1.1:666", RAFT_VOTER);
    REMOVE(2);
    ASSERT_N(2);
    ASSERT_SERVER(0, 1, "127.0.0.1:666", RAFT_VOTER);
    ASSERT_SERVER(1, 3, "10.0.1.1:666", RAFT_VOTER);
    return MUNIT_OK;
}

/* Attempts to remove a server with an unknown ID result in an error. */
TEST(configurationRemove, unknownId, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    REMOVE_ERROR(RAFT_BADID, 1);
    return MUNIT_OK;
}

/* Out of memory. */
TEST(configurationRemove, oom, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    ADD(1, "127.0.0.1:666", RAFT_VOTER);
    ADD(2, "192.168.1.1:666", RAFT_STANDBY);
    HeapFaultConfig(&f->heap, 0, 1);
    HeapFaultEnable(&f->heap);
    REMOVE_ERROR(RAFT_NOMEM, 1);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * configurationEncode
 *
 *****************************************************************************/

SUITE(configurationEncode)

/* Encode a configuration with one server. */
TEST(configurationEncode, one_server, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_buffer buf;
    size_t len;
    const void *cursor;
    const char *address = "127.0.0.1:666";
    ADD(1, address, RAFT_VOTER);
    ENCODE(&buf);

    len = 1 + 8 +                  /* Version and n of servers */
          8 + strlen(address) + 1; /* Server */
    len = bytePad64(len);

    munit_assert_int(buf.len, ==, len);

    cursor = buf.base;

    munit_assert_int(byteGet8(&cursor), ==, 1);
    munit_assert_int(byteGet64(&cursor), ==, 1);

    munit_assert_int(byteGet64(&cursor), ==, 1);
    munit_assert_string_equal(byteGetString(&cursor, strlen(address) + 1),
                              address);
    munit_assert_int(byteGet8(&cursor), ==, RAFT_VOTER);

    raft_free(buf.base);

    return MUNIT_OK;
}

/* Encode a configuration with two servers. */
TEST(configurationEncode, two_servers, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_buffer buf;
    size_t len;
    const void *cursor;
    const char *address1 = "127.0.0.1:666";
    const char *address2 = "192.168.1.1:666";

    ADD(1, address1, RAFT_STANDBY);
    ADD(2, address2, RAFT_VOTER);
    ENCODE(&buf);

    len = 1 + 8 +                        /* Version and n of servers */
          8 + strlen(address1) + 1 + 1 + /* Server 1 */
          8 + strlen(address2) + 1 + 1;  /* Server 2 */
    len = bytePad64(len);

    munit_assert_int(buf.len, ==, len);

    cursor = buf.base;

    munit_assert_int(byteGet8(&cursor), ==, 1);
    munit_assert_int(byteGet64(&cursor), ==, 2);

    munit_assert_int(byteGet64(&cursor), ==, 1);
    munit_assert_string_equal(byteGetString(&cursor, strlen(address1) + 1),
                              address1);
    munit_assert_int(byteGet8(&cursor), ==, RAFT_STANDBY);

    munit_assert_int(byteGet64(&cursor), ==, 2);
    munit_assert_string_equal(byteGetString(&cursor, strlen(address2) + 1),
                              address2);
    munit_assert_int(byteGet8(&cursor), ==, RAFT_VOTER);

    raft_free(buf.base);

    return MUNIT_OK;
}

/* Out of memory. */
TEST(configurationEncode, oom, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_buffer buf;
    HeapFaultConfig(&f->heap, 2, 1);
    HeapFaultEnable(&f->heap);
    ADD(1, "127.0.0.1:666", RAFT_VOTER);
    ENCODE_ERROR(RAFT_NOMEM, &buf);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * configurationDecode
 *
 *****************************************************************************/

SUITE(configurationDecode)

/* The decode a payload encoding a configuration with one server */
TEST(configurationDecode, one_server, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    uint8_t bytes[] = {1,                            /* Version */
                       1,   0,   0,   0, 0, 0, 0, 0, /* Number of servers */
                       5,   0,   0,   0, 0, 0, 0, 0, /* Server ID */
                       'x', '.', 'y', 0,             /* Server address */
                       1};                           /* Role code */
    struct raft_buffer buf;

    buf.base = bytes;
    buf.len = sizeof bytes;

    DECODE(&buf);

    ASSERT_N(1);
    ASSERT_SERVER(0, 5, "x.y", RAFT_VOTER);

    return MUNIT_OK;
}

/* The decode size is the size of a raft_server array plus the length of the
 * addresses. */
TEST(configurationDecode, two_servers, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    uint8_t bytes[] = {1,                                /* Version */
                       2,   0,   0,   0,   0,   0, 0, 0, /* Number of servers */
                       5,   0,   0,   0,   0,   0, 0, 0, /* Server ID */
                       'x', '.', 'y', 0,                 /* Server address */
                       1,                                /* Role code */
                       3,   0,   0,   0,   0,   0, 0, 0, /* Server ID */
                       '1', '9', '2', '.', '2', 0,       /* Server address */
                       0};                               /* Role code */
    struct raft_buffer buf;
    buf.base = bytes;
    buf.len = sizeof bytes;
    DECODE(&buf);
    ASSERT_N(2);
    ASSERT_SERVER(0, 5, "x.y", RAFT_VOTER);
    ASSERT_SERVER(1, 3, "192.2", RAFT_STANDBY);
    return MUNIT_OK;
}

static char *decode_oom_heap_fault_delay[] = {"0", "1", "2", "3", NULL};
static char *decode_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum decode_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, decode_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, decode_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Not enough memory for creating the decoded configuration object. */
TEST(configurationDecode, oom, setUp, tearDownNoClose, 0, decode_oom_params)
{
    struct fixture *f = data;
    uint8_t bytes[] = {1,                            /* Version */
                       2,   0,   0,   0, 0, 0, 0, 0, /* Number of servers */
                       5,   0,   0,   0, 0, 0, 0, 0, /* Server ID */
                       'x', '.', 'y', 0,             /* Server address */
                       1,                            /* Role code */
                       3,   0,   0,   0, 0, 0, 0, 0, /* Server ID */
                       'z', '.', 'w', 0,             /* Server address */
                       0};                           /* Role code */
    struct raft_buffer buf;
    HEAP_FAULT_ENABLE;
    buf.base = bytes;
    buf.len = sizeof bytes;
    DECODE_ERROR(RAFT_NOMEM, &buf);
    return MUNIT_OK;
}

/* If the encoding version is wrong, an error is returned. */
TEST(configurationDecode, badVersion, setUp, tearDownNoClose, 0, NULL)
{
    struct fixture *f = data;
    uint8_t bytes = 127;
    struct raft_buffer buf;
    buf.base = &bytes;
    buf.len = 1;
    DECODE_ERROR(RAFT_MALFORMED, &buf);
    return MUNIT_OK;
}

/* The address of a server is not a nul-terminated string. */
TEST(configurationDecode, badAddress, setUp, tearDownNoClose, 0, NULL)
{
    struct fixture *f = data;
    uint8_t bytes[] = {1,                            /* Version */
                       1,   0,   0,   0, 0, 0, 0, 0, /* Number of servers */
                       5,   0,   0,   0, 0, 0, 0, 0, /* Server ID */
                       'x', '.', 'y',                /* Server address */
                       1};                           /* Voting flag */
    struct raft_buffer buf;
    buf.base = bytes;
    buf.len = sizeof bytes;
    DECODE_ERROR(RAFT_MALFORMED, &buf);
    return MUNIT_OK;
}

/* The encoded configuration is invalid because it has a duplicated server
 * ID. In that case RAFT_MALFORMED is returned. */
TEST(configurationDecode, duplicatedID, setUp, tearDownNoClose, 0, NULL)
{
    struct fixture *f = data;
    uint8_t bytes[] = {1,                            /* Version */
                       2,   0,   0,   0, 0, 0, 0, 0, /* Number of servers */
                       5,   0,   0,   0, 0, 0, 0, 0, /* Server ID */
                       'x', '.', 'y', 0,             /* Server address */
                       1,                            /* Role code */
                       5,   0,   0,   0, 0, 0, 0, 0, /* Server ID */
                       'z', '.', 'w', 0,             /* Server address */
                       0};                           /* Role code */
    struct raft_buffer buf;
    buf.base = bytes;
    buf.len = sizeof bytes;
    DECODE_ERROR(RAFT_MALFORMED, &buf);
    return MUNIT_OK;
}
