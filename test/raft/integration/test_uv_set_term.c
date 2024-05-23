#include "../../../src/raft.h"
#include "../../../src/raft/byte.h"
#include "../../../src/raft/uv_encoding.h"
#include "../lib/runner.h"
#include "../lib/uv.h"

/******************************************************************************
 *
 * Fixture with a libuv-based raft_io instance.
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_UV_DEPS;
    FIXTURE_UV;
    bool closed;
};

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

static void closeCb(struct raft_io *io)
{
    struct fixture *f = io->data;
    f->closed = true;
}

/* Invoke raft_uv_init() and assert that no error occurs. */
#define INIT                                                         \
    do {                                                             \
        int _rv;                                                     \
        _rv = raft_uv_init(&f->io, &f->loop, f->dir, &f->transport); \
        munit_assert_int(_rv, ==, 0);                                \
        _rv = f->io.init(&f->io, 1, "1");                            \
        munit_assert_int(_rv, ==, 0);                                \
    } while (0)

/* Invoke raft_io->close(). */
#define CLOSE                         \
    do {                              \
        f->io.close(&f->io, closeCb); \
        LOOP_RUN_UNTIL(&f->closed);   \
        raft_uv_close(&f->io);        \
    } while (0)

/* Invoke f->io->set_term() and assert that no error occurs. */
#define SET_TERM(TERM)                      \
    do {                                    \
        int _rv;                            \
        _rv = f->io.set_term(&f->io, TERM); \
        munit_assert_int(_rv, ==, 0);       \
    } while (0)

/* Invoke f->io->set_term() and assert that the given error code is returned and
 * the given error message set. */
#define SET_TERM_ERROR(TERM, RV, ERRMSG)                          \
    do {                                                          \
        int _rv;                                                  \
        _rv = f->io.set_term(&f->io, TERM);                       \
        munit_assert_int(_rv, ==, RV);                            \
        munit_assert_string_equal(f->io.errmsg_(&f->io), ERRMSG); \
    } while (0)

/* Write either the metadata1 or metadata2 file, filling it with the given
 * values. */
#define WRITE_METADATA_FILE(N, FORMAT, VERSION, TERM, VOTED_FOR) \
    {                                                            \
        uint8_t buf[8 * 4];                                      \
        void *cursor = buf;                                      \
        char filename[strlen("metadataN") + 1];                  \
        sprintf(filename, "metadata%d", N);                      \
        bytePut64(&cursor, FORMAT);                              \
        bytePut64(&cursor, VERSION);                             \
        bytePut64(&cursor, TERM);                                \
        bytePut64(&cursor, VOTED_FOR);                           \
        DirWriteFile(f->dir, filename, buf, sizeof buf);         \
    }

/* Assert that the content of either the metadata1 or metadata2 file match the
 * given values. */
#define ASSERT_METADATA_FILE(N, VERSION, TERM, VOTED_FOR)    \
    {                                                        \
        uint8_t buf2[8 * 4];                                 \
        const void *cursor = buf2;                           \
        char filename[strlen("metadataN") + 1];              \
        sprintf(filename, "metadata%d", N);                  \
        DirReadFile(f->dir, filename, buf2, sizeof buf2);    \
        munit_assert_int(byteGet64(&cursor), ==, UV__DISK_FORMAT);         \
        munit_assert_int(byteGet64(&cursor), ==, VERSION);   \
        munit_assert_int(byteGet64(&cursor), ==, TERM);      \
        munit_assert_int(byteGet64(&cursor), ==, VOTED_FOR); \
    }

/******************************************************************************
 *
 * Set up and tear down.
 *
 *****************************************************************************/

static void *setUpDeps(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_UV_DEPS;
    f->io.data = f;
    f->closed = false;
    return f;
}

static void *setUp(const MunitParameter params[], void *user_data)
{
    struct fixture *f = setUpDeps(params, user_data);
    INIT;
    return f;
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    CLOSE;
    TEAR_DOWN_UV_DEPS;
    free(f);
}

/******************************************************************************
 *
 * raft_io->set_term()
 *
 *****************************************************************************/

SUITE(set_term)

/* The very first time set_term() is called, the metadata1 file gets written. */
TEST(set_term, first, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    SET_TERM(1);
    ASSERT_METADATA_FILE(1, 1, 1, 0);
    munit_assert_false(DirHasFile(f->dir, "metadata2"));
    return MUNIT_OK;
}

/* The second time set_term() is called, the metadata2 file gets written. */
TEST(set_term, second, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    SET_TERM(1);
    SET_TERM(2);
    ASSERT_METADATA_FILE(1, 1, 1, 0);
    ASSERT_METADATA_FILE(2, 2, 2, 0);
    return MUNIT_OK;
}

/* The third time set_term() is called, the metadata1 file gets overwritten. */
TEST(set_term, third, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    SET_TERM(1);
    SET_TERM(2);
    SET_TERM(3);
    ASSERT_METADATA_FILE(1, 3, 3, 0);
    ASSERT_METADATA_FILE(2, 2, 2, 0);
    return MUNIT_OK;
}

/* The fourth time set_term() is called, the metadata2 file gets overwritten. */
TEST(set_term, fourth, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    SET_TERM(1);
    SET_TERM(2);
    SET_TERM(3);
    SET_TERM(4);
    ASSERT_METADATA_FILE(1, 3, 3, 0);
    ASSERT_METADATA_FILE(2, 4, 4, 0);
    return MUNIT_OK;
}

/* If the data directory has a single metadata1 file, the first time set_data()
 * is called, the second metadata file gets created. */
TEST(set_term, metadataOneExists, setUpDeps, tearDown, 0, NULL)
{
    struct fixture *f = data;
    WRITE_METADATA_FILE(1, /* Metadata file index                  */
                        UV__DISK_FORMAT, /* Format                               */
                        1, /* Version                              */
                        1, /* Term                                 */
                        0 /* Voted for                            */);
    INIT;
    SET_TERM(2);
    ASSERT_METADATA_FILE(1, 1, 1, 0);
    ASSERT_METADATA_FILE(2, 2, 2, 0);
    return MUNIT_OK;
}

/* The data directory has both metadata files, but metadata1 is greater. */
TEST(set_term, metadataOneIsGreater, setUpDeps, tearDown, 0, NULL)
{
    struct fixture *f = data;
    WRITE_METADATA_FILE(1, /* Metadata file index                  */
                        UV__DISK_FORMAT, /* Format                               */
                        3, /* Version                              */
                        3, /* Term                                 */
                        0 /* Voted for                            */);
    WRITE_METADATA_FILE(2, /* Metadata file index                  */
                        UV__DISK_FORMAT, /* Format                               */
                        2, /* Version                              */
                        2, /* Term                                 */
                        0 /* Voted for                            */);
    INIT;
    SET_TERM(4);
    ASSERT_METADATA_FILE(1 /* n */, 3 /* version */, 3 /* term */,
                         0 /* voted for */);
    ASSERT_METADATA_FILE(2 /* n */, 4 /* version */, 4 /* term */,
                         0 /* voted for */);
    return MUNIT_OK;
}

/* The data directory has both metadata files, but metadata2 is greater. */
TEST(set_term, metadataTwoIsGreater, setUpDeps, tearDown, 0, NULL)
{
    struct fixture *f = data;
    WRITE_METADATA_FILE(1, /* Metadata file index                  */
                        UV__DISK_FORMAT, /* Format                               */
                        1, /* Version                              */
                        1, /* Term                                 */
                        0 /* Voted for                            */);
    WRITE_METADATA_FILE(2, /* Metadata file index                  */
                        UV__DISK_FORMAT, /* Format                               */
                        2, /* Version                              */
                        2, /* Term                                 */
                        0 /* Voted for                            */);
    INIT;
    SET_TERM(2);
    ASSERT_METADATA_FILE(1 /* n */, 3 /* version */, 2 /* term */,
                         0 /* voted for */);
    ASSERT_METADATA_FILE(2 /* n */, 2 /* version */, 2 /* term */,
                         0 /* voted for */);
    return MUNIT_OK;
}
