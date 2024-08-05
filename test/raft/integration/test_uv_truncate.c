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

struct result
{
    int status;
    bool done;
};

static void appendCbAssertResult(struct raft_io_append *req, int status)
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
    struct result _result##I = {0, false};                          \
    int _rv##I;                                                     \
    ENTRIES(I, N_ENTRIES, ENTRY_SIZE);                              \
    _req##I.data = &_result##I;                                     \
    _rv##I = f->io.append(&f->io, &_req##I, _entries##I, N_ENTRIES, \
                          appendCbAssertResult);                    \
    munit_assert_int(_rv##I, ==, 0)

/* Wait for the append request identified by I to complete. */
#define APPEND_WAIT(I) LOOP_RUN_UNTIL(&_result##I.done)

#define APPEND_EXPECT(I, STATUS) _result##I.status = STATUS

/* Submit an append request and wait for it to successfully complete. */
#define APPEND(N)                  \
    do {                           \
        APPEND_SUBMIT(9999, N, 8); \
        APPEND_WAIT(9999);         \
    } while (0)

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
#define ASSERT_ENTRIES(N, ...)                                               \
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
        unsigned _data[N] = {__VA_ARGS__};                                   \
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
        munit_assert_int(_rv, ==, 0);                                        \
        _rv = _io.load(&_io, &_term, &_voted_for, &_snapshot, &_start_index, \
                       &_entries, &_n);                                      \
        munit_assert_int(_rv, ==, 0);                                        \
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
            munit_assert_int(_value, ==, _data[_i]);                         \
            munit_assert_ptr_not_null(_entry->batch);                        \
        }                                                                    \
        for (_i = 0; _i < _n; _i++) {                                        \
            struct raft_entry *_entry = &_entries[_i];                       \
            if (_entry->batch != _batch) {                                   \
                _batch = _entry->batch;                                      \
                raft_free(_batch);                                           \
            }                                                                \
        }                                                                    \
        raft_free(_entries);                                                 \
    } while (0);

/******************************************************************************
 *
 * raft_io->truncate()
 *
 *****************************************************************************/

SUITE(truncate)

/* If the index to truncate is at the start of a segment, that segment and all
 * subsequent ones are removed. */
TEST(truncate, wholeSegment, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    APPEND(3);
    TRUNCATE(1);
    APPEND(1);
    ASSERT_ENTRIES(1 /* n entries */, 4 /* entries data */);
    return MUNIT_OK;
}

/* The index to truncate is the same as the last appended entry. */
TEST(truncate, sameAsLastIndex, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    APPEND(3);
    TRUNCATE(3);
    APPEND(1);
    ASSERT_ENTRIES(3 /* n entries */, 1, 2, 4 /* entries data */);
    return MUNIT_OK;
}

/* If the index to truncate is not at the start of a segment, that segment gets
 * truncated. */
TEST(truncate, partialSegment, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    APPEND(3);
    APPEND(1);
    TRUNCATE(2);
    APPEND(1);
    ASSERT_ENTRIES(2,   /* n entries */
                   1, 5 /* entries data */
    );
    return MUNIT_OK;
}

/* The truncate request is issued while an append request is still pending. */
TEST(truncate, pendingAppend, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    APPEND_SUBMIT(0, /* request ID */
                  3, /* n entries */
                  8  /* entry size */
    );
    TRUNCATE(2 /* truncation index */);
    APPEND(1);
    ASSERT_ENTRIES(2,   /* n entries */
                   1, 4 /* entries data */
    );
    return MUNIT_OK;
}

/* Multiple truncate requests pending at the same time. */
TEST(truncate, multiplePending, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    APPEND_SUBMIT(0, /* request ID */
                  3, /* n entries */
                  8  /* entry size */
    );
    TRUNCATE(2 /* truncation index */);
    APPEND_SUBMIT(1, /* request ID */
                  2, /* n entries */
                  8  /* entry size */
    );
    TRUNCATE(3 /* truncation index */);
    APPEND(1);
    ASSERT_ENTRIES(3,      /* n entries */
                   1, 4, 6 /* entries data */
    );
    return MUNIT_OK;
}

/* The truncate request gets canceled because we're closing. */
TEST(truncate, closing, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    APPEND_SUBMIT(0, /* request ID */
                  3, /* n entries */
                  8  /* entry size */
    );
    TRUNCATE(2 /* truncation index */);
    APPEND_EXPECT(0,            /* request ID */
                  RAFT_CANCELED /* status */
    );
    TEAR_DOWN_UV;
    return MUNIT_OK;
}

/* Multiple truncate requests get canceled because we're closing. */
TEST(truncate, closingMultiple, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    APPEND_SUBMIT(0, /* request ID */
                  3, /* n entries */
                  8  /* entry size */
    );
    TRUNCATE(2 /* truncation index */);
    APPEND_SUBMIT(1, /* request ID */
                  2, /* n entries */
                  8  /* entry size */
    );
    TRUNCATE(3 /* truncation index */);
    APPEND_EXPECT(0,            /* request ID */
                  RAFT_CANCELED /* status */
    );
    APPEND_EXPECT(1,            /* request ID */
                  RAFT_CANCELED /* status */
    );
    TEAR_DOWN_UV;
    return MUNIT_OK;
}
