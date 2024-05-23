#include "../../../src/raft.h"
#include "../../../src/raft/byte.h"
#include "../../../src/raft/uv_encoding.h"
#include "../lib/runner.h"
#include "../lib/uv.h"

#include <linux/magic.h>
#include <sys/vfs.h>

#define BAD_FORMAT 3
#define BAD_FORMAT_STR "3"

/******************************************************************************
 *
 * Fixture with a non-initialized raft_io instance and uv dependencies.
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
#define INIT(DIR)                                                 \
    do {                                                          \
        int _rv;                                                  \
        _rv = raft_uv_init(&f->io, &f->loop, DIR, &f->transport); \
        munit_assert_int(_rv, ==, 0);                             \
        _rv = f->io.init(&f->io, 1, "1");                         \
        munit_assert_int(_rv, ==, 0);                             \
    } while (0)

/* Invoke raft_io->close(). */
#define CLOSE                         \
    do {                              \
        f->io.close(&f->io, closeCb); \
        LOOP_RUN_UNTIL(&f->closed);   \
        raft_uv_close(&f->io);        \
    } while (0)

/* Invoke raft_uv_init() and assert that the given error code is returned and
 * the given error message set. */
#define INIT_ERROR(DIR, RV, ERRMSG)                               \
    do {                                                          \
        int _rv;                                                  \
        _rv = raft_uv_init(&f->io, &f->loop, DIR, &f->transport); \
        munit_assert_int(_rv, ==, 0);                             \
        _rv = f->io.init(&f->io, 1, "1");                         \
        munit_assert_int(_rv, ==, RV);                            \
        munit_assert_string_equal(f->io.errmsg, ERRMSG);          \
        CLOSE;                                                    \
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

#define LONG_DIR                                                               \
    "/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" \
    "/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" \
    "/ccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc" \
    "/ddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd" \
    "/eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee" \
    "/fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff" \
    "/ggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg" \
    "/hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh" \
    "/iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii" \
    "/jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj" \
    "/kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk" \
    "/lllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll" \
    "/mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm"

static void *setUp(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_UV_DEPS;
    f->io.data = f;
    f->closed = false;
    return f;
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    if (f == NULL) {
        return;
    }
    TEAR_DOWN_UV_DEPS;
    free(f);
}

/******************************************************************************
 *
 * raft_io->init()
 *
 *****************************************************************************/

SUITE(init)

TEST(init, dirTooLong, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_io io = {0};
    int rv;
    rv = raft_uv_init(&io, &f->loop, LONG_DIR, &f->transport);
    munit_assert_int(rv, ==, RAFT_NAMETOOLONG);
    munit_assert_string_equal(io.errmsg, "directory path too long");
    return 0;
}

/* Out of memory conditions upon probing for direct I/O. */
TEST(init, probeDirectIoOom, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    /* XXX: tmpfs seems to not support O_DIRECT */
    struct statfs info;
    int rv;
    rv = statfs(f->dir, &info);
    munit_assert_int(rv, ==, 0);
    if (info.f_type == TMPFS_MAGIC) {
        return MUNIT_SKIP;
    }
#if defined(__powerpc64__)
    /* XXX: fails on ppc64el */
    return MUNIT_SKIP;
#endif
    HeapFaultConfig(&f->heap, 1 /* delay */, 1 /* repeat */);
    HEAP_FAULT_ENABLE;
    INIT_ERROR(f->dir, RAFT_NOMEM, "probe Direct I/O: out of memory");
    return 0;
}

/* Out of memory conditions upon probing for async I/O. */
TEST(init, probeAsyncIoOom, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    /* XXX: tmpfs seems to not support O_DIRECT */
    struct statfs info;
    int rv;
    rv = statfs(f->dir, &info);
    munit_assert_int(rv, ==, 0);
    if (info.f_type == TMPFS_MAGIC) {
        return MUNIT_SKIP;
    }
#if defined(__powerpc64__)
    /* XXX: fails on ppc64el */
    return MUNIT_SKIP;
#endif
    HeapFaultConfig(&f->heap, 2 /* delay */, 1 /* repeat */);
    HEAP_FAULT_ENABLE;
    INIT_ERROR(f->dir, RAFT_NOMEM, "probe Async I/O: out of memory");
    return 0;
}

/* The given directory does not exist. */
TEST(init, dirDoesNotExist, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    INIT_ERROR("/foo/bar/egg/baz", RAFT_NOTFOUND,
               "directory '/foo/bar/egg/baz' does not exist");
    return MUNIT_OK;
}

/* The given directory not accessible */
TEST(init, dirNotAccessible, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    char errmsg[RAFT_ERRMSG_BUF_SIZE];
    sprintf(errmsg, "directory '%s' is not writable", f->dir);
    DirMakeUnexecutable(f->dir);
    INIT_ERROR(f->dir, RAFT_INVALID, errmsg);
    return MUNIT_OK;
}

/* No space is left for probing I/O capabilities. */
TEST(init, noSpace, setUp, tearDown, 0, DirTmpfsParams)
{
    struct fixture *f = data;
    SKIP_IF_NO_FIXTURE;
    DirFill(f->dir, 4);
    INIT_ERROR(f->dir, RAFT_NOSPACE,
               "create I/O capabilities probe file: not enough space to "
               "allocate 4096 bytes");
    return MUNIT_OK;
}

/* The metadata1 file has not the expected number of bytes. In this case the
 * file is not considered at all, and the effect is as if this was a brand new
 * server. */
TEST(init, metadataOneTooShort, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    uint8_t buf[16] = {0};
    DirWriteFile(f->dir, "metadata1", buf, sizeof buf);
    INIT(f->dir);
    CLOSE;
    return MUNIT_OK;
}

/* The metadata1 file has not the expected format. */
TEST(init, metadataOneBadFormat, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    WRITE_METADATA_FILE(1, /* Metadata file index                  */
                        BAD_FORMAT, /* Format                               */
                        1, /* Version                              */
                        1, /* Term                                 */
                        0 /* Voted for                            */);
    INIT_ERROR(f->dir, RAFT_MALFORMED,
               "decode content of metadata1: bad format version " BAD_FORMAT_STR);
    return MUNIT_OK;
}

/* The metadata1 file has not a valid version. */
TEST(init, metadataOneBadVersion, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    WRITE_METADATA_FILE(1, /* Metadata file index                  */
                        UV__DISK_FORMAT, /* Format                               */
                        0, /* Version                              */
                        1, /* Term                                 */
                        0 /* Voted for                            */);
    INIT_ERROR(f->dir, RAFT_CORRUPT,
               "decode content of metadata1: version is set to zero");
    return MUNIT_OK;
}

/* The data directory has both metadata files, but they have the same
 * version. */
TEST(init, metadataOneAndTwoSameVersion, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    WRITE_METADATA_FILE(1, /* Metadata file index                  */
                        UV__DISK_FORMAT, /* Format                               */
                        2, /* Version                              */
                        3, /* Term                                 */
                        0 /* Voted for                            */);
    WRITE_METADATA_FILE(2, /* Metadata file index                  */
                        UV__DISK_FORMAT, /* Format                               */
                        2, /* Version                              */
                        2, /* Term                                 */
                        0 /* Voted for                            */);
    INIT_ERROR(f->dir, RAFT_CORRUPT,
               "metadata1 and metadata2 are both at version 2");
    return MUNIT_OK;
}
