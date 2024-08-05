#include "../lib/runner.h"
#include "../lib/uv.h"

/******************************************************************************
 *
 * Fixture
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
 * Helper macros
 *
 *****************************************************************************/

/* Maximum number of blocks a segment can have */
#define MAX_SEGMENT_BLOCKS 4

/* This block size should work fine for all file systems. */
#define SEGMENT_BLOCK_SIZE 4096

/* Default segment size */
#define SEGMENT_SIZE 4096 * MAX_SEGMENT_BLOCKS

struct result
{
    int status;
    bool done;
    void *data;
};

static void appendCbAssertResult(struct raft_io_append *req, int status)
{
    struct result *result = req->data;
    munit_assert_int(status, ==, result->status);
    result->done = true;
}

static void snapshotPutCbAssertResult(struct raft_io_snapshot_put *req,
                                      int status)
{
    struct result *result = req->data;
    munit_assert_int(status, ==, result->status);
    result->done = true;
}

/* Declare and fill the entries array for the append request identified by
 * I. The array will have N entries, and each entry will have a data buffer of
 * SIZE bytes.*/
#define ENTRIES(I, N, SIZE)                                 \
    struct raft_entry _entries##I[N];                       \
    uint8_t _entries_data##I[N * SIZE];                     \
    do {                                                    \
        int _i;                                             \
        for (_i = 0; _i < N; _i++) {                        \
            struct raft_entry *entry = &_entries##I[_i];    \
            entry->term = 1;                                \
            entry->type = RAFT_COMMAND;                     \
            entry->buf.base = &_entries_data##I[_i * SIZE]; \
            entry->buf.len = SIZE;                          \
            entry->batch = NULL;                            \
            munit_assert_ptr_not_null(entry->buf.base);     \
            memset(entry->buf.base, 0, entry->buf.len);     \
            f->count++;                                     \
            *(uint64_t *)entry->buf.base = f->count;        \
        }                                                   \
    } while (0)

/* Submit an append request identified by I, with N_ENTRIES entries, each one of
 * size ENTRY_SIZE). */
#define APPEND_SUBMIT(I, N_ENTRIES, ENTRY_SIZE)                     \
    struct raft_io_append _req##I;                                  \
    struct result _result##I = {0, false, NULL};                    \
    int _rv##I;                                                     \
    ENTRIES(I, N_ENTRIES, ENTRY_SIZE);                              \
    _req##I.data = &_result##I;                                     \
    _rv##I = f->io.append(&f->io, &_req##I, _entries##I, N_ENTRIES, \
                          appendCbAssertResult);                    \
    munit_assert_int(_rv##I, ==, 0)

#define TRUNCATE(N)                      \
    do {                                 \
        int rv_;                         \
        struct raft_io_truncate *trunc_ = munit_malloc(sizeof(*trunc_)); \
        rv_ = f->io.truncate(&f->io, trunc_, N); \
        munit_assert_int(rv_, ==, 0);    \
    } while (0)

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
    TEAR_DOWN_UV_DEPS;
    free(f);
}

/******************************************************************************
 *
 * Assertions
 *
 *****************************************************************************/

/* Shutdown the fixture's raft_io instance, then load all entries on disk using
 * a new raft_io instance, and assert that there are N entries with data
 * matching the DATA array. */
#define ASSERT_ENTRIES(N, ...)                                            \
    TEAR_DOWN_UV;                                                         \
    do {                                                                  \
        struct uv_loop_s _loop;                                           \
        struct raft_uv_transport _transport;                              \
        struct raft_io _io;                                               \
        raft_term _term;                                                  \
        raft_id _voted_for;                                               \
        struct raft_snapshot *_snap;                                      \
        raft_index _start_index;                                          \
        struct raft_entry *_entries;                                      \
        size_t _i;                                                        \
        size_t _n;                                                        \
        void *_batch = NULL;                                              \
        unsigned _data[N] = {__VA_ARGS__};                                \
        int _ret;                                                         \
                                                                          \
        _ret = uv_loop_init(&_loop);                                      \
        munit_assert_int(_ret, ==, 0);                                    \
        _transport.version = 1;                                           \
        _ret = raft_uv_tcp_init(&_transport, &_loop);                     \
        munit_assert_int(_ret, ==, 0);                                    \
        _ret = raft_uv_init(&_io, &_loop, f->dir, &_transport);           \
        munit_assert_int(_ret, ==, 0);                                    \
        _ret = _io.init(&_io, 1, "1");                                    \
        munit_assert_int(_ret, ==, 0);                                    \
        _ret = _io.load(&_io, &_term, &_voted_for, &_snap, &_start_index, \
                        &_entries, &_n);                                  \
        munit_assert_int(_ret, ==, 0);                                    \
        _io.close(&_io, NULL);                                            \
        uv_run(&_loop, UV_RUN_NOWAIT);                                    \
        raft_uv_close(&_io);                                              \
        raft_uv_tcp_close(&_transport);                                   \
        uv_loop_close(&_loop);                                            \
                                                                          \
        munit_assert_size(_n, ==, N);                                     \
        for (_i = 0; _i < _n; _i++) {                                     \
            struct raft_entry *_entry = &_entries[_i];                    \
            uint64_t _value = *(uint64_t *)_entry->buf.base;              \
            munit_assert_int(_entry->term, ==, 1);                        \
            munit_assert_int(_entry->type, ==, RAFT_COMMAND);             \
            munit_assert_int(_value, ==, _data[_i]);                      \
            munit_assert_ptr_not_null(_entry->batch);                     \
        }                                                                 \
        for (_i = 0; _i < _n; _i++) {                                     \
            struct raft_entry *_entry = &_entries[_i];                    \
            if (_entry->batch != _batch) {                                \
                _batch = _entry->batch;                                   \
                raft_free(_batch);                                        \
            }                                                             \
        }                                                                 \
        raft_free(_entries);                                              \
        if (_snap != NULL) {                                              \
            raft_configuration_close(&_snap->configuration);              \
            munit_assert_int(_snap->n_bufs, ==, 1);                       \
            raft_free(_snap->bufs[0].base);                               \
            raft_free(_snap->bufs);                                       \
            raft_free(_snap);                                             \
        }                                                                 \
    } while (0);

#define SNAPSHOT_PUT_REQ(TRAILING, INDEX, RV, STATUS)              \
    struct raft_snapshot _snapshot;                                \
    struct raft_buffer _snapshot_buf;                              \
    uint64_t _snapshot_data;                                       \
    struct raft_io_snapshot_put _req;                              \
    struct result _result = {STATUS, false, NULL};                 \
    int _rv;                                                       \
    _snapshot.term = 1;                                            \
    _snapshot.index = INDEX;                                       \
    raft_configuration_init(&_snapshot.configuration);             \
    _rv = raft_configuration_add(&_snapshot.configuration, 1, "1", \
                                 RAFT_STANDBY);                    \
    munit_assert_int(_rv, ==, 0);                                  \
    _snapshot.bufs = &_snapshot_buf;                               \
    _snapshot.n_bufs = 1;                                          \
    _snapshot_buf.base = &_snapshot_data;                          \
    _snapshot_buf.len = sizeof _snapshot_data;                     \
    _req.data = &_result;                                          \
    _rv = f->io.snapshot_put(&f->io, TRAILING, &_req, &_snapshot,  \
                             snapshotPutCbAssertResult);           \
    munit_assert_int(_rv, ==, RV)

#define SNAPSHOT_CLEANUP() raft_configuration_close(&_snapshot.configuration)

/******************************************************************************
 *
 * test interaction of raft_io->snapshot_put and raft_io->truncate()
 *
 *****************************************************************************/

SUITE(snapshot_truncate)

/* Fill up 3 segments worth of data, then take a snapshot.
 * While the snapshot is taken, start a truncate request. */
TEST(snapshot_truncate, snapshotThenTruncate, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    APPEND_SUBMIT(0, MAX_SEGMENT_BLOCKS, SEGMENT_BLOCK_SIZE);
    APPEND_SUBMIT(1, MAX_SEGMENT_BLOCKS, SEGMENT_BLOCK_SIZE);
    APPEND_SUBMIT(2, MAX_SEGMENT_BLOCKS, SEGMENT_BLOCK_SIZE);

    /* Take a snapshot, this will use a uv_barrier. */
    SNAPSHOT_PUT_REQ(8192, 6, 0, 0);

    /* Truncate, this will use a uv_barrier too.  */
    TRUNCATE(8);

    /* There's no truncate callback to wait for, loop for a while. */
    LOOP_RUN(1000);

    /* Check that truncate has done its job. */
    ASSERT_ENTRIES(7, 1, 2, 3, 4, 5, 6, 7);

    SNAPSHOT_CLEANUP();
    return MUNIT_OK;
}
