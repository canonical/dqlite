#include "../../../src/raft/uv.h"
#include "../lib/aio.h"
#include "../lib/runner.h"
#include "../lib/uv.h"
#include "append_helpers.h"

#include <unistd.h>

/* Maximum number of blocks a segment can have */
#define MAX_SEGMENT_BLOCKS 4

/* This block size should work fine for all file systems. */
#define SEGMENT_BLOCK_SIZE 4096

/* Default segment size */
#define SEGMENT_SIZE 4096 * MAX_SEGMENT_BLOCKS

/******************************************************************************
 *
 * Fixture with a libuv-based raft_io instance.
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_UV_DEPS;
    FIXTURE_UV;
    int count; /* To generate deterministic entry data */
};

/******************************************************************************
 *
 * Set up and tear down.
 *
 *****************************************************************************/

static void *setUp(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_UV_DEPS;
    SETUP_UV;
    raft_uv_set_block_size(&f->io, SEGMENT_BLOCK_SIZE);
    raft_uv_set_segment_size(&f->io, SEGMENT_SIZE);
    f->count = 0;
    return f;
}

static void tearDownDeps(void *data)
{
    struct fixture *f = data;
    if (f == NULL) {
        return;
    }
    TEAR_DOWN_UV_DEPS;
    free(f);
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    if (f == NULL) {
        return;
    }
    TEAR_DOWN_UV;
    tearDownDeps(f);
}

/******************************************************************************
 *
 * Assertions
 *
 *****************************************************************************/

/* Shutdown the fixture's raft_io instance, then load all entries on disk using
 * a new raft_io instance, and assert that there are N entries with a total data
 * size of TOTAL_DATA_SIZE bytes. */
#define ASSERT_ENTRIES(N, TOTAL_DATA_SIZE)                                   \
    TEAR_DOWN_UV;                                                            \
    do {                                                                     \
        struct uv_loop_s _loop;                                              \
        struct raft_uv_transport _transport;                                 \
        struct raft_io _io;                                                  \
        raft_term _term;                                                     \
        raft_id _voted_for;                                                  \
        struct raft_snapshot *_snapshot;                                     \
        raft_index _start_index;                                             \
        struct raft_entry *_entries;                                         \
        size_t _i;                                                           \
        size_t _n;                                                           \
        void *_batch = NULL;                                                 \
        size_t _total_data_size = 0;                                         \
        int _rv;                                                             \
                                                                             \
        _rv = uv_loop_init(&_loop);                                          \
        munit_assert_int(_rv, ==, 0);                                        \
        _transport.version = 1;                                              \
        _rv = raft_uv_tcp_init(&_transport, &_loop);                         \
        munit_assert_int(_rv, ==, 0);                                        \
        _rv = raft_uv_init(&_io, &_loop, f->dir, &_transport);               \
        munit_assert_int(_rv, ==, 0);                                        \
        _rv = _io.init(&_io, 1, "1");                                        \
        if (_rv != 0) {                                                      \
            munit_errorf("io->init(): %s (%d)", _io.errmsg, _rv);            \
        }                                                                    \
        _rv = _io.load(&_io, &_term, &_voted_for, &_snapshot, &_start_index, \
                       &_entries, &_n);                                      \
        if (_rv != 0) {                                                      \
            munit_errorf("io->load(): %s (%d)", _io.errmsg, _rv);            \
        }                                                                    \
        _io.close(&_io, NULL);                                               \
        uv_run(&_loop, UV_RUN_NOWAIT);                                       \
        raft_uv_close(&_io);                                                 \
        raft_uv_tcp_close(&_transport);                                      \
        uv_loop_close(&_loop);                                               \
                                                                             \
        munit_assert_ptr_null(_snapshot);                                    \
        munit_assert_int(_n, ==, N);                                         \
        for (_i = 0; _i < _n; _i++) {                                        \
            struct raft_entry *_entry = &_entries[_i];                       \
            uint64_t _value = *(uint64_t *)_entry->buf.base;                 \
            munit_assert_int(_entry->term, ==, 1);                           \
            munit_assert_int(_entry->type, ==, RAFT_COMMAND);                \
            munit_assert_int(_value, ==, _i);                                \
            munit_assert_ptr_not_null(_entry->batch);                        \
        }                                                                    \
        for (_i = 0; _i < _n; _i++) {                                        \
            struct raft_entry *_entry = &_entries[_i];                       \
            if (_entry->batch != _batch) {                                   \
                _batch = _entry->batch;                                      \
                raft_free(_batch);                                           \
            }                                                                \
            _total_data_size += _entry->buf.len;                             \
        }                                                                    \
        raft_free(_entries);                                                 \
        munit_assert_int(_total_data_size, ==, TOTAL_DATA_SIZE);             \
    } while (0);

/******************************************************************************
 *
 * raft_io->append()
 *
 *****************************************************************************/

SUITE(append)

/* Append an entries array containing unaligned buffers. */
TEST(append, unaligned, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND_SUBMIT_CB_DATA(0, 1, 9, NULL, NULL, RAFT_INVALID);
    munit_assert_string_equal(f->io.errmsg,
                              "entry buffers must be 8-byte aligned");
    APPEND_SUBMIT_CB_DATA(1, 3, 63, NULL, NULL, RAFT_INVALID);
    munit_assert_string_equal(f->io.errmsg,
                              "entry buffers must be 8-byte aligned");
    return MUNIT_OK;
}

/* Append the very first batch of entries. */
TEST(append, first, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    APPEND(1, 64);
    ASSERT_ENTRIES(1, 64);
    return MUNIT_OK;
}

/* As soon as the backend starts writing the first open segment, a second one
 * and a third one get prepared. */
TEST(append, prepareSegments, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND(1, 64);
    while (!DirHasFile(f->dir, "open-3")) {
        LOOP_RUN(1);
    }
    munit_assert_true(DirHasFile(f->dir, "open-1"));
    munit_assert_true(DirHasFile(f->dir, "open-2"));
    munit_assert_true(DirHasFile(f->dir, "open-3"));
    return MUNIT_OK;
}

/* Once the first segment fills up, it gets finalized, and an additional one
 * gets prepared, to maintain the available segments pool size. */
TEST(append, finalizeSegment, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND(MAX_SEGMENT_BLOCKS, SEGMENT_BLOCK_SIZE);
    APPEND(1, 64);
    while (!DirHasFile(f->dir, "open-4")) {
        LOOP_RUN(1);
    }
    munit_assert_true(DirHasFile(f->dir, "0000000000000001-0000000000000004"));
    munit_assert_false(DirHasFile(f->dir, "open-1"));
    munit_assert_true(DirHasFile(f->dir, "open-4"));
    return MUNIT_OK;
}

/* The very first batch of entries to append is bigger than the regular open
 * segment size. */
TEST(append, firstBig, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    APPEND(MAX_SEGMENT_BLOCKS, SEGMENT_BLOCK_SIZE);
    ASSERT_ENTRIES(MAX_SEGMENT_BLOCKS, MAX_SEGMENT_BLOCKS * SEGMENT_BLOCK_SIZE);
    return MUNIT_OK;
}

/* The second batch of entries to append is bigger than the regular open
 * segment size. */
TEST(append, secondBig, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND(1, 64);
    APPEND(MAX_SEGMENT_BLOCKS, SEGMENT_BLOCK_SIZE);
    return MUNIT_OK;
}

/* Schedule multiple appends each one exceeding the segment size. */
TEST(append, severalBig, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    APPEND_SUBMIT(0, 2, MAX_SEGMENT_BLOCKS * SEGMENT_BLOCK_SIZE);
    APPEND_SUBMIT(1, 2, MAX_SEGMENT_BLOCKS * SEGMENT_BLOCK_SIZE);
    APPEND_SUBMIT(2, 2, MAX_SEGMENT_BLOCKS * SEGMENT_BLOCK_SIZE);
    APPEND_WAIT(0);
    APPEND_WAIT(1);
    APPEND_WAIT(2);
    ASSERT_ENTRIES(6, 6 * MAX_SEGMENT_BLOCKS * SEGMENT_BLOCK_SIZE);
    return MUNIT_OK;
}

/* Write the very first entry and then another one, both fitting in the same
 * block. */
TEST(append, fitBlock, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    APPEND(1, 64);
    APPEND(1, 64);
    ASSERT_ENTRIES(2, 128);
    return MUNIT_OK;
}

/* Write an entry that fills the first block exactly and then another one. */
TEST(append, matchBlock, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    size_t size;

    size = SEGMENT_BLOCK_SIZE;
    size -= sizeof(uint64_t) + /* Format */
            sizeof(uint64_t) + /* Checksums */
            8 + 16;            /* Header */

    APPEND(1, size);
    APPEND(1, 64);

    ASSERT_ENTRIES(2, size + 64);

    return MUNIT_OK;
}

/* Write an entry that exceeds the first block, then another one that fits in
 * the second block, then a third one that fills the rest of the second block
 * plus the whole third block exactly, and finally a fourth entry that fits in
 * the fourth block */
TEST(append, exceedBlock, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    size_t written;
    size_t size1;
    size_t size2;

    size1 = SEGMENT_BLOCK_SIZE;

    APPEND(1, size1);
    APPEND(1, 64);

    written = sizeof(uint64_t) +     /* Format version */
              2 * sizeof(uint32_t) + /* CRC sums of first batch */
              8 + 16 +               /* Header of first batch */
              size1 +                /* Size of first batch */
              2 * sizeof(uint32_t) + /* CRC of second batch */
              8 + 16 +               /* Header of second batch */
              64;                    /* Size of second batch */

    /* Write a third entry that fills the second block exactly */
    size2 = SEGMENT_BLOCK_SIZE - (written % SEGMENT_BLOCK_SIZE);
    size2 -= (2 * sizeof(uint32_t) + 8 + 16);
    size2 += SEGMENT_BLOCK_SIZE;

    APPEND(1, size2);

    /* Write a fourth entry */
    APPEND(1, 64);

    ASSERT_ENTRIES(4, size1 + 64 + size2 + 64);

    return MUNIT_OK;
}

/* If an append request is submitted before the write operation of the previous
 * append request is started, then a single write will be performed for both
 * requests. */
TEST(append, batch, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND_SUBMIT(0, 1, 64);
    APPEND_SUBMIT(1, 1, 64);
    APPEND_WAIT(0);
    APPEND_WAIT(1);
    return MUNIT_OK;
}

/* An append request submitted while a write operation is in progress gets
 * executed only when the write completes. */
TEST(append, wait, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND_SUBMIT(0, 1, 64);
    LOOP_RUN(1);
    APPEND_SUBMIT(1, 1, 64);
    APPEND_WAIT(0);
    APPEND_WAIT(1);
    return MUNIT_OK;
}

/* Several batches with different size gets appended in fast pace, forcing the
 * segment arena to grow. */
TEST(append, resizeArena, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    APPEND_SUBMIT(0, 2, 64);
    APPEND_SUBMIT(1, 1, SEGMENT_BLOCK_SIZE);
    APPEND_SUBMIT(2, 2, 64);
    APPEND_SUBMIT(3, 1, SEGMENT_BLOCK_SIZE);
    APPEND_SUBMIT(4, 1, SEGMENT_BLOCK_SIZE);
    APPEND_WAIT(0);
    APPEND_WAIT(1);
    APPEND_WAIT(2);
    APPEND_WAIT(3);
    APPEND_WAIT(4);
    ASSERT_ENTRIES(7, 64 * 4 + SEGMENT_BLOCK_SIZE * 3);
    return MUNIT_OK;
}

/* A few append requests get queued, then a truncate request comes in and other
 * append requests right after, before truncation is fully completed. */
TEST(append, truncate, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    int rv;

    return MUNIT_SKIP; /* FIXME: flaky */

    APPEND(2, 64);

    APPEND_SUBMIT(0, 2, 64);

    struct raft_io_truncate *trunc = munit_malloc(sizeof(*trunc));
    rv = f->io.truncate(&f->io, trunc, 2);
    munit_assert_int(rv, ==, 0);

    APPEND_SUBMIT(1, 2, 64);

    APPEND_WAIT(0);
    APPEND_WAIT(1);

    return MUNIT_OK;
}

/* A few append requests get queued, then a truncate request comes in and other
 * append requests right after, before truncation is fully completed. However
 * the backend is closed before the truncation request can be processed. */
TEST(append, truncateClosing, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    int rv;
    APPEND(2, 64);
    APPEND_SUBMIT(0, 2, 64);
    struct raft_io_truncate *trunc = munit_malloc(sizeof(*trunc));
    rv = f->io.truncate(&f->io, trunc, 2);
    munit_assert_int(rv, ==, 0);
    APPEND_SUBMIT(1, 2, 64);
    APPEND_EXPECT(1, RAFT_CANCELED);
    TEAR_DOWN_UV;
    return MUNIT_OK;
}

/* A few append requests get queued, however the backend is closed before
 * preparing the second segment completes. */
TEST(append, prepareClosing, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    APPEND_SUBMIT(0, 2, 64);
    LOOP_RUN(1);
    TEAR_DOWN_UV;
    return MUNIT_OK;
}

/* The counters of the open segments get increased as they are closed. */
TEST(append, counter, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    size_t size = SEGMENT_BLOCK_SIZE;
    int i;
    for (i = 0; i < 10; i++) {
        APPEND(1, size);
    }
    munit_assert_true(DirHasFile(f->dir, "0000000000000001-0000000000000003"));
    munit_assert_true(DirHasFile(f->dir, "0000000000000004-0000000000000006"));
    munit_assert_true(DirHasFile(f->dir, "open-4"));
    return MUNIT_OK;
}

/* If the I/O instance is closed, all pending append requests get canceled. */
TEST(append, cancel, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    APPEND_SUBMIT(0, 1, 64);
    APPEND_EXPECT(0, RAFT_CANCELED);
    TEAR_DOWN_UV;
    return MUNIT_OK;
}

/* The creation of the current open segment fails because there's no space. */
TEST(append, noSpaceUponPrepareCurrent, setUp, tearDown, 0, DirTmpfsParams)
{
    struct fixture *f = data;
    SKIP_IF_NO_FIXTURE;
    raft_uv_set_segment_size(&f->io, SEGMENT_BLOCK_SIZE * 32768);
    APPEND_FAILURE(
        1, 64, RAFT_NOSPACE,
        "create segment open-1: not enough space to allocate 134217728 bytes");
    return MUNIT_OK;
}

/* The creation of a spare open segment fails because there's no space. */
TEST(append, noSpaceUponPrepareSpare, setUp, tearDown, 0, DirTmpfsParams)
{
    struct fixture *f = data;
    SKIP_IF_NO_FIXTURE;
#if defined(__powerpc64__)
    /* XXX: fails on ppc64el */
    return MUNIT_SKIP;
#endif
    raft_uv_set_segment_size(&f->io, SEGMENT_BLOCK_SIZE * 2);
    DirFill(f->dir, SEGMENT_BLOCK_SIZE * 3);
    APPEND(1, SEGMENT_BLOCK_SIZE);
    APPEND_SUBMIT(0, 1, SEGMENT_BLOCK_SIZE);
    APPEND_EXPECT(0, RAFT_NOSPACE);
    APPEND_WAIT(0);
    return MUNIT_OK;
}

/* The write request fails because there's not enough space. */
TEST(append, noSpaceUponWrite, setUp, tearDownDeps, 0, DirTmpfsParams)
{
    struct fixture *f = data;
    SKIP_IF_NO_FIXTURE;
#if defined(__powerpc64__)
    /* XXX: fails on ppc64el */
    TEAR_DOWN_UV;
    return MUNIT_SKIP;
#endif
    raft_uv_set_segment_size(&f->io, SEGMENT_BLOCK_SIZE);
    DirFill(f->dir, SEGMENT_BLOCK_SIZE * 2);
    APPEND(1, 64);
    APPEND_FAILURE(1, (SEGMENT_BLOCK_SIZE + 128), RAFT_NOSPACE,
                   "short write: 4096 bytes instead of 8192");
    DirRemoveFile(f->dir, ".fill");
    LOOP_RUN(50);
    APPEND(5, 64);
    ASSERT_ENTRIES(6, 384);
    return MUNIT_OK;
}

/* A few requests fail because not enough disk space is available. Eventually
 * the space is released and the request succeeds. */
TEST(append, noSpaceResolved, setUp, tearDownDeps, 0, DirTmpfsParams)
{
    struct fixture *f = data;
    SKIP_IF_NO_FIXTURE;
#if defined(__powerpc64__)
    /* XXX: fails on ppc64el */
    TEAR_DOWN_UV;
    return MUNIT_SKIP;
#endif
    DirFill(f->dir, SEGMENT_BLOCK_SIZE);
    APPEND_FAILURE(
        1, 64, RAFT_NOSPACE,
        "create segment open-1: not enough space to allocate 16384 bytes");
    APPEND_FAILURE(
        1, 64, RAFT_NOSPACE,
        "create segment open-2: not enough space to allocate 16384 bytes");
    DirRemoveFile(f->dir, ".fill");
    f->count = 0; /* Reset the data counter */
    APPEND(1, 64);
    ASSERT_ENTRIES(1, 64);
    return MUNIT_OK;
}

/* An error occurs while performing a write. */
TEST(append, writeError, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    aio_context_t ctx = 0;

    /* FIXME: doesn't fail anymore after
     * https://github.com/CanonicalLtd/raft/pull/49 */
    return MUNIT_SKIP;

    APPEND_SUBMIT(0, 1, 64);
    AioFill(&ctx, 0);
    APPEND_WAIT(0);
    AioDestroy(ctx);
    return MUNIT_OK;
}

static char *oomHeapFaultDelay[] = {"1", /* FIXME "2", */ NULL};
static char *oomHeapFaultRepeat[] = {"1", NULL};

static MunitParameterEnum oomParams[] = {
    {TEST_HEAP_FAULT_DELAY, oomHeapFaultDelay},
    {TEST_HEAP_FAULT_REPEAT, oomHeapFaultRepeat},
    {NULL, NULL},
};

/* Out of memory conditions. */
TEST(append, oom, setUp, tearDown, 0, oomParams)
{
    struct fixture *f = data;
    HEAP_FAULT_ENABLE;
    APPEND_ERROR(1, 64, RAFT_NOMEM, "");
    return MUNIT_OK;
}

/* The uv instance is closed while a write request is in progress. */
TEST(append, closeDuringWrite, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    /* TODO: broken */
    return MUNIT_SKIP;

    APPEND_SUBMIT(0, 1, 64);
    LOOP_RUN(1);
    TEAR_DOWN_UV;

    return MUNIT_OK;
}

/* When the backend is closed, all unused open segments get removed. */
TEST(append, removeSegmentUponClose, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    APPEND(1, 64);
    while (!DirHasFile(f->dir, "open-2")) {
        LOOP_RUN(1);
    }
    TEAR_DOWN_UV;
    munit_assert_false(DirHasFile(f->dir, "open-2"));
    return MUNIT_OK;
}

/* When the backend is closed, all pending prepare get requests get canceled. */
TEST(append, cancelPrepareRequest, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    /* TODO: find a way to test a prepare request cancelation */
    return MUNIT_SKIP;
    APPEND(MAX_SEGMENT_BLOCKS, SEGMENT_BLOCK_SIZE);
    APPEND_SUBMIT(0, 1, 64);
    APPEND_EXPECT(0, RAFT_CANCELED);
    TEAR_DOWN_UV;
    return MUNIT_OK;
}

/* When the writer gets closed it tells the writer to close the segment that
 * it's currently writing. */
TEST(append, currentSegment, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;

    APPEND(1, 64);

    TEAR_DOWN_UV;

    munit_assert_true(DirHasFile(f->dir, "0000000000000001-0000000000000001"));

    return MUNIT_OK;
}

/* The kernel has ran out of available AIO events. */
TEST(append, ioSetupError, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    aio_context_t ctx = 0;
    int rv;
    rv = AioFill(&ctx, 0);
    if (rv != 0) {
        return MUNIT_SKIP;
    }
    APPEND_FAILURE(1, 64, RAFT_TOOMANY,
                   "setup writer for open-1: AIO events user limit exceeded");
    return MUNIT_OK;
}

/*===========================================================================
  Test interaction between UvAppend and UvBarrier
  ===========================================================================*/

struct barrierData
{
    int current;     /* Count the number of finished AppendEntries RPCs  */
    int expected;    /* Expected number of finished AppendEntries RPCs   */
    bool done;       /* @true if the Barrier CB has fired                */
    bool expectDone; /* Expect the Barrier CB to have fired or not       */
    char **files;    /* Expected files in the directory, NULL terminated */
    struct uv *uv;
};

static void barrierCbCompareCounter(struct UvBarrierReq *barrier)
{
    struct barrierData *bd = barrier->data;
    munit_assert_false(bd->done);
    bd->done = true;
    struct uv *uv = bd->uv;
    UvUnblock(uv);
    munit_assert_int(bd->current, ==, bd->expected);
    if (bd->files != NULL) {
        int i = 0;
        while (bd->files[i] != NULL) {
            munit_assert_true(DirHasFile(uv->dir, bd->files[i]));
            ++i;
        }
    }
}

static void barrierDoneCb(struct UvBarrierReq *barrier)
{
    struct barrierData *bd = barrier->data;
    munit_assert_false(bd->done);
    bd->done = true;
}

static void appendCbIncreaseCounterAssertResult(struct raft_io_append *req,
                                                int status)
{
    struct result *result = req->data;
    munit_assert_int(status, ==, result->status);
    result->done = true;
    struct barrierData *bd = result->data;
    munit_assert_true(bd->done == bd->expectDone);
    bd->current += 1;
}

static void appendDummyCb(struct raft_io_append *req, int status)
{
    (void)req;
    (void)status;
}

static char *bools[] = {"0", "1", NULL};
static MunitParameterEnum blocking_bool_params[] = {
    {"bool", bools},
    {NULL, NULL},
};

/* Fill up 3 segments worth of AppendEntries RPC's.
 * Request a Barrier and expect that the AppendEntries RPC's are finished before
 * the Barrier callback is fired.
 */
TEST(append, barrierOpenSegments, setUp, tearDown, 0, blocking_bool_params)
{
    struct fixture *f = data;
    struct barrierData bd = {0};
    bd.current = 0;
    bd.expected = 3;
    bd.done = false;
    bd.expectDone = false;
    bd.uv = f->io.impl;
    char *files[] = {"0000000000000001-0000000000000004",
                     "0000000000000005-0000000000000008",
                     "0000000000000009-0000000000000012", NULL};
    bd.files = files;

    APPEND_SUBMIT_CB_DATA(0, MAX_SEGMENT_BLOCKS, SEGMENT_BLOCK_SIZE,
                          appendCbIncreaseCounterAssertResult, &bd, 0);
    APPEND_SUBMIT_CB_DATA(1, MAX_SEGMENT_BLOCKS, SEGMENT_BLOCK_SIZE,
                          appendCbIncreaseCounterAssertResult, &bd, 0);
    APPEND_SUBMIT_CB_DATA(2, MAX_SEGMENT_BLOCKS, SEGMENT_BLOCK_SIZE,
                          appendCbIncreaseCounterAssertResult, &bd, 0);

    struct UvBarrierReq barrier = {0};
    barrier.data = (void *)&bd;
    barrier.blocking =
        (bool)strtoul(munit_parameters_get(params, "bool"), NULL, 0);
    barrier.cb = barrierCbCompareCounter;
    UvBarrier(f->io.impl, 1, &barrier);

    /* Make sure every callback fired */
    LOOP_RUN_UNTIL(&bd.done);
    APPEND_WAIT(0);
    APPEND_WAIT(1);
    APPEND_WAIT(2);
    return MUNIT_OK;
}

/* Fill up 3 segments worth of AppendEntries RPC's.
 * Request a Barrier and stop early.
 */
TEST(append, barrierOpenSegmentsExitEarly, setUp, NULL, 0, blocking_bool_params)
{
    struct fixture *f = data;
    struct barrierData bd = {0};
    bd.current = 0;
    bd.expected = 3;
    bd.done = false;
    bd.expectDone = false;
    bd.uv = f->io.impl;
    char *files[] = {"0000000000000001-0000000000000004",
                     "0000000000000005-0000000000000008",
                     "0000000000000009-0000000000000012", NULL};
    bd.files = files;

    APPEND_SUBMIT_CB_DATA(0, MAX_SEGMENT_BLOCKS, SEGMENT_BLOCK_SIZE,
                          appendDummyCb, NULL, 0);
    APPEND_SUBMIT_CB_DATA(1, MAX_SEGMENT_BLOCKS, SEGMENT_BLOCK_SIZE,
                          appendDummyCb, NULL, 0);
    APPEND_SUBMIT_CB_DATA(2, MAX_SEGMENT_BLOCKS, SEGMENT_BLOCK_SIZE,
                          appendDummyCb, NULL, 0);

    struct UvBarrierReq barrier = {0};
    barrier.data = (void *)&bd;
    barrier.blocking =
        (bool)strtoul(munit_parameters_get(params, "bool"), NULL, 0);
    barrier.cb = barrierDoneCb;
    UvBarrier(f->io.impl, 1, &barrier);

    /* Exit early. */
    tearDown(data);
    munit_assert_true(bd.done);

    return MUNIT_OK;
}

/* Fill up 3 segments worth of AppendEntries RPC's.
 * Request a 2 barriers and expect their callbacks to fire.
 */
TEST(append, twoBarriersOpenSegments, setUp, tearDown, 0, blocking_bool_params)
{
    struct fixture *f = data;
    struct barrierData bd1 = {0};
    bd1.current = 0;
    bd1.expected = 3;
    bd1.done = false;
    bd1.expectDone = false;
    bd1.uv = f->io.impl;
    char *files[] = {"0000000000000001-0000000000000004",
                     "0000000000000005-0000000000000008",
                     "0000000000000009-0000000000000012", NULL};
    bd1.files = files;
    /* Only expect the callback to eventually fire. */
    struct barrierData bd2 = {0};
    bd2.uv = f->io.impl;

    APPEND_SUBMIT_CB_DATA(0, MAX_SEGMENT_BLOCKS, SEGMENT_BLOCK_SIZE,
                          appendCbIncreaseCounterAssertResult, &bd1, 0);
    APPEND_SUBMIT_CB_DATA(1, MAX_SEGMENT_BLOCKS, SEGMENT_BLOCK_SIZE,
                          appendCbIncreaseCounterAssertResult, &bd1, 0);
    APPEND_SUBMIT_CB_DATA(2, MAX_SEGMENT_BLOCKS, SEGMENT_BLOCK_SIZE,
                          appendCbIncreaseCounterAssertResult, &bd1, 0);

    struct UvBarrierReq barrier1 = {0};
    barrier1.data = (void *)&bd1;
    barrier1.blocking =
        (bool)strtoul(munit_parameters_get(params, "bool"), NULL, 0);
    barrier1.cb = barrierCbCompareCounter;
    UvBarrier(f->io.impl, 1, &barrier1);
    struct UvBarrierReq barrier2 = {0};
    barrier2.data = (void *)&bd2;
    barrier2.blocking =
        (bool)strtoul(munit_parameters_get(params, "bool"), NULL, 0);
    barrier2.cb = barrierCbCompareCounter;
    UvBarrier(f->io.impl, 1, &barrier2);

    /* Make sure every callback fired */
    LOOP_RUN_UNTIL(&bd1.done);
    LOOP_RUN_UNTIL(&bd2.done);
    APPEND_WAIT(0);
    APPEND_WAIT(1);
    APPEND_WAIT(2);
    return MUNIT_OK;
}

/* Fill up 3 segments worth of AppendEntries RPC's.
 * Request 2 barriers and exit early.
 */
TEST(append, twoBarriersExitEarly, setUp, NULL, 0, blocking_bool_params)
{
    struct fixture *f = data;
    struct barrierData bd1 = {0};
    bd1.current = 0;
    bd1.expected = 3;
    bd1.done = false;
    bd1.expectDone = false;
    bd1.uv = f->io.impl;
    char *files[] = {"0000000000000001-0000000000000004",
                     "0000000000000005-0000000000000008",
                     "0000000000000009-0000000000000012", NULL};
    bd1.files = files;
    /* Only expect the callback to eventually fire. */
    struct barrierData bd2 = {0};
    bd2.uv = f->io.impl;

    APPEND_SUBMIT_CB_DATA(0, MAX_SEGMENT_BLOCKS, SEGMENT_BLOCK_SIZE,
                          appendDummyCb, NULL, 0);
    APPEND_SUBMIT_CB_DATA(1, MAX_SEGMENT_BLOCKS, SEGMENT_BLOCK_SIZE,
                          appendDummyCb, NULL, 0);
    APPEND_SUBMIT_CB_DATA(2, MAX_SEGMENT_BLOCKS, SEGMENT_BLOCK_SIZE,
                          appendDummyCb, NULL, 0);

    struct UvBarrierReq barrier1 = {0};
    barrier1.data = (void *)&bd1;
    barrier1.blocking =
        (bool)strtoul(munit_parameters_get(params, "bool"), NULL, 0);
    barrier1.cb = barrierDoneCb;
    UvBarrier(f->io.impl, 1, &barrier1);
    struct UvBarrierReq barrier2 = {0};
    barrier2.data = (void *)&bd2;
    barrier2.blocking =
        (bool)strtoul(munit_parameters_get(params, "bool"), NULL, 0);
    barrier2.cb = barrierDoneCb;
    UvBarrier(f->io.impl, 1, &barrier2);

    /* Exit early. */
    tearDown(data);
    munit_assert_true(bd1.done);
    munit_assert_true(bd2.done);

    return MUNIT_OK;
}

/* Request a blocking Barrier and expect that the no AppendEntries RPC's are
 * finished before the Barrier callback is fired.
 */
TEST(append, blockingBarrierNoOpenSegments, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct barrierData bd = {0};
    bd.current = 0;
    bd.expected = 0;
    bd.done = false;
    bd.expectDone = true;
    bd.uv = f->io.impl;

    struct UvBarrierReq barrier = {0};
    barrier.data = (void *)&bd;
    barrier.blocking = true;
    barrier.cb = barrierCbCompareCounter;
    UvBarrier(f->io.impl, 1, &barrier);

    APPEND_SUBMIT_CB_DATA(0, MAX_SEGMENT_BLOCKS, SEGMENT_BLOCK_SIZE,
                          appendCbIncreaseCounterAssertResult, &bd, 0);
    APPEND_SUBMIT_CB_DATA(1, MAX_SEGMENT_BLOCKS, SEGMENT_BLOCK_SIZE,
                          appendCbIncreaseCounterAssertResult, &bd, 0);
    APPEND_SUBMIT_CB_DATA(2, MAX_SEGMENT_BLOCKS, SEGMENT_BLOCK_SIZE,
                          appendCbIncreaseCounterAssertResult, &bd, 0);

    /* Make sure every callback fired */
    LOOP_RUN_UNTIL(&bd.done);
    APPEND_WAIT(0);
    APPEND_WAIT(1);
    APPEND_WAIT(2);
    return MUNIT_OK;
}

/* Request a blocking Barrier and expect that the no AppendEntries RPC's are
 * finished before the Barrier callback is fired. */
TEST(append, blockingBarrierSingleOpenSegment, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct barrierData bd = {0};
    bd.current = 0;
    bd.expected = 0;
    bd.done = false;
    bd.expectDone = true;
    bd.uv = f->io.impl;
    char *files[] = {"0000000000000001-0000000000000001", NULL};
    bd.files = files;

    /* Wait until there is at least 1 open segment otherwise
     * the barrier Cb is fired immediately. */
    APPEND(1, 64);
    while (!DirHasFile(f->dir, "open-1")) {
        LOOP_RUN(1);
    }

    struct UvBarrierReq barrier = {0};
    barrier.data = (void *)&bd;
    barrier.blocking = true;
    barrier.cb = barrierCbCompareCounter;
    UvBarrier(f->io.impl, 1, &barrier);

    APPEND_SUBMIT_CB_DATA(0, MAX_SEGMENT_BLOCKS, SEGMENT_BLOCK_SIZE,
                          appendCbIncreaseCounterAssertResult, &bd, 0);
    APPEND_SUBMIT_CB_DATA(1, MAX_SEGMENT_BLOCKS, SEGMENT_BLOCK_SIZE,
                          appendCbIncreaseCounterAssertResult, &bd, 0);
    APPEND_SUBMIT_CB_DATA(2, MAX_SEGMENT_BLOCKS, SEGMENT_BLOCK_SIZE,
                          appendCbIncreaseCounterAssertResult, &bd, 0);

    /* Make sure every callback fired */
    LOOP_RUN_UNTIL(&bd.done);
    APPEND_WAIT(0);
    APPEND_WAIT(1);
    APPEND_WAIT(2);
    return MUNIT_OK;
}

static void longWorkCb(uv_work_t *work)
{
    (void)work;
    sleep(1);
}

static void longAfterWorkCb(uv_work_t *work, int status)
{
    struct barrierData *bd = work->data;
    munit_assert_false(bd->done);
    bd->done = true;
    munit_assert_int(status, ==, 0);
    struct uv *uv = bd->uv;
    UvUnblock(uv);
    munit_assert_int(bd->current, ==, bd->expected);
    free(work);
}

static void barrierCbLongWork(struct UvBarrierReq *barrier)
{
    struct barrierData *bd = barrier->data;
    munit_assert_false(bd->done);
    struct uv *uv = bd->uv;
    int rv;

    uv_work_t *work = munit_malloc(sizeof(*work));
    munit_assert_ptr_not_null(work);
    work->data = bd;

    rv = uv_queue_work(uv->loop, work, longWorkCb, longAfterWorkCb);
    munit_assert_int(rv, ==, 0);
}

/* Request a non-blocking Barrier that triggers a long-running task, the barrier
 * is removed when the long running task completes. This simulates a large
 * snapshot write. Ensure Append requests complete before the long running task
 * completes.*/
TEST(append, nonBlockingBarrierLongBlockingTask, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct barrierData bd = {0};
    bd.current = 0;
    bd.expected = 1;
    bd.done = false;
    bd.expectDone = false;
    bd.uv = f->io.impl;

    struct UvBarrierReq barrier = {0};
    barrier.data = (void *)&bd;
    barrier.blocking = false;
    barrier.cb = barrierCbLongWork;
    UvBarrier(f->io.impl, bd.uv->append_next_index, &barrier);
    APPEND_SUBMIT_CB_DATA(0, 1, 64, appendCbIncreaseCounterAssertResult, &bd,
                          0);

    /* Make sure every callback fired */
    LOOP_RUN_UNTIL(&bd.done);
    APPEND_WAIT(0);
    return MUNIT_OK;
}

/* Request a blocking Barrier that triggers a long-running task, the barrier
 * is unblocked and removed when the long running task completes. This simulates
 * a large snapshot install. Ensure Append requests complete after the work
 * completes.*/
TEST(append, blockingBarrierLongBlockingTask, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct barrierData bd = {0};
    bd.current = 0;
    bd.expected = 0;
    bd.done = false;
    bd.expectDone = true;
    bd.uv = f->io.impl;

    struct UvBarrierReq barrier = {0};
    barrier.data = (void *)&bd;
    barrier.blocking = true;
    barrier.cb = barrierCbLongWork;
    UvBarrier(f->io.impl, bd.uv->append_next_index, &barrier);
    APPEND_SUBMIT_CB_DATA(0, 1, 64, appendCbIncreaseCounterAssertResult, &bd,
                          0);

    /* Make sure every callback fired */
    LOOP_RUN_UNTIL(&bd.done);
    APPEND_WAIT(0);
    return MUNIT_OK;
}
