#include <unistd.h>

#include "../../../src/raft/byte.h"
#include "../../../src/raft/uv.h"
#include "../../../src/raft/uv_encoding.h"
#include "../lib/runner.h"
#include "../lib/uv.h"

/******************************************************************************
 *
 * Fixture with a non-initialized libuv-based raft_io instance.
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_UV_DEPS;
    FIXTURE_UV;
};

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

static void closeCb(struct raft_io *io)
{
    bool *done = io->data;
    *done = true;
}

static void appendCb(struct raft_io_append *req, int status)
{
    bool *done = req->data;
    munit_assert_int(status, ==, 0);
    *done = true;
}

static void snapshotPutCb(struct raft_io_snapshot_put *req, int status)
{
    bool *done = req->data;
    munit_assert_int(status, ==, 0);
    *done = true;
}

struct snapshot
{
    raft_term term;
    raft_index index;
    uint64_t data;
};

#define WORD_SIZE 8

/* Maximum number of blocks a segment can have */
#define MAX_SEGMENT_BLOCKS 4

/* This block size should work fine for all file systems. */
#define SEGMENT_BLOCK_SIZE 4096

/* Desired segment size */
#define SEGMENT_SIZE SEGMENT_BLOCK_SIZE *MAX_SEGMENT_BLOCKS

#define CLOSED_SEGMENT_FILENAME(START, END) \
    "000000000000000" #START                \
    "-"                                     \
    "000000000000000" #END

/* Check if open segment file exists. */
#define HAS_OPEN_SEGMENT_FILE(COUNT) DirHasFile(f->dir, "open-" #COUNT)

/* Check if closed segment file exists. */
#define HAS_CLOSED_SEGMENT_FILE(START, END) \
    DirHasFile(f->dir, CLOSED_SEGMENT_FILENAME(START, END))

/* Initialize a standalone raft_io instance and use it to append N batches of
 * entries, each containing one entry. DATA should be an integer that will be
 * used as base value for the data of the first entry, and will be then
 * incremented for subsequent entries. */
#define APPEND(N, DATA)                                                      \
    do {                                                                     \
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
        struct raft_entry _new_entry;                                        \
        uint64_t _new_entry_data;                                            \
        uint64_t _data = DATA;                                               \
        struct raft_io_append _req;                                          \
        bool _done = false;                                                  \
        int _rv;                                                             \
                                                                             \
        /* Initialize the instance, loading existing data, but discarding    \
         * it. This makes sure that the start index is correctly set. */     \
        _transport.version = 1;                                              \
        _rv = raft_uv_tcp_init(&_transport, &f->loop);                       \
        munit_assert_int(_rv, ==, 0);                                        \
        _rv = raft_uv_init(&_io, &f->loop, f->dir, &_transport);             \
        munit_assert_int(_rv, ==, 0);                                        \
        _rv = _io.init(&_io, 1, "1");                                        \
        munit_assert_int(_rv, ==, 0);                                        \
        raft_uv_set_block_size(&_io, SEGMENT_BLOCK_SIZE);                    \
        raft_uv_set_segment_size(&_io, SEGMENT_SIZE);                        \
        _rv = _io.load(&_io, &_term, &_voted_for, &_snapshot, &_start_index, \
                       &_entries, &_n);                                      \
        munit_assert_int(_rv, ==, 0);                                        \
        for (_i = 0; _i < _n; _i++) {                                        \
            struct raft_entry *_entry = &_entries[_i];                       \
            if (_entry->batch != _batch) {                                   \
                _batch = _entry->batch;                                      \
                raft_free(_batch);                                           \
            }                                                                \
        }                                                                    \
        if (_entries != NULL) {                                              \
            raft_free(_entries);                                             \
        }                                                                    \
        if (_snapshot != NULL) {                                             \
            raft_configuration_close(&_snapshot->configuration);             \
            munit_assert_int(_snapshot->n_bufs, ==, 1);                      \
            raft_free(_snapshot->bufs[0].base);                              \
            raft_free(_snapshot->bufs);                                      \
            raft_free(_snapshot);                                            \
        }                                                                    \
                                                                             \
        /* Append the new entries. */                                        \
        for (_i = 0; _i < N; _i++) {                                         \
            struct raft_entry *entry = &_new_entry;                          \
            entry->term = 1;                                                 \
            entry->type = RAFT_COMMAND;                                      \
            entry->buf.base = &_new_entry_data;                              \
            entry->buf.len = sizeof _new_entry_data;                         \
            entry->batch = NULL;                                             \
            munit_assert_ptr_not_null(entry->buf.base);                      \
            memset(entry->buf.base, 0, entry->buf.len);                      \
            *(uint64_t *)entry->buf.base = _data;                            \
            _data++;                                                         \
            _req.data = &_done;                                              \
            _rv = _io.append(&_io, &_req, entry, 1, appendCb);               \
            munit_assert_int(_rv, ==, 0);                                    \
            LOOP_RUN_UNTIL(&_done);                                          \
            _done = false;                                                   \
        }                                                                    \
                                                                             \
        /* Shutdown the standalone raft_io instance. */                      \
        _done = false;                                                       \
        _io.data = &_done;                                                   \
        _io.close(&_io, closeCb);                                            \
        LOOP_RUN_UNTIL(&_done);                                              \
        raft_uv_close(&_io);                                                 \
        raft_uv_tcp_close(&_transport);                                      \
    } while (0);

/* Initialize a standalone raft_io instance and use it to persist a new snapshot
 * at the given INDEX and TERM. DATA should be an integer that will be used as
 * as snapshot content. */
#define SNAPSHOT_PUT(TERM, INDEX, DATA)                                       \
    do {                                                                      \
        struct raft_uv_transport _transport;                                  \
        struct raft_io _io;                                                   \
        raft_term _term;                                                      \
        raft_id _voted_for;                                                   \
        struct raft_snapshot *_snapshot;                                      \
        raft_index _start_index;                                              \
        struct raft_entry *_entries;                                          \
        size_t _i;                                                            \
        size_t _n;                                                            \
        void *_batch = NULL;                                                  \
        struct raft_snapshot _new_snapshot;                                   \
        struct raft_buffer _new_snapshot_buf;                                 \
        uint64_t _new_snapshot_data = DATA;                                   \
        struct raft_io_snapshot_put _req;                                     \
        bool _done = false;                                                   \
        int _rv;                                                              \
                                                                              \
        /* Initialize the instance, loading existing data, but discarding     \
         * it. This makes sure that the start index is correctly set. */      \
        _transport.version = 1;                                               \
        _rv = raft_uv_tcp_init(&_transport, &f->loop);                        \
        munit_assert_int(_rv, ==, 0);                                         \
        _rv = raft_uv_init(&_io, &f->loop, f->dir, &_transport);              \
        munit_assert_int(_rv, ==, 0);                                         \
        _rv = _io.init(&_io, 1, "1");                                         \
        munit_assert_int(_rv, ==, 0);                                         \
        raft_uv_set_block_size(&_io, SEGMENT_BLOCK_SIZE);                     \
        raft_uv_set_segment_size(&_io, SEGMENT_SIZE);                         \
        _rv = _io.load(&_io, &_term, &_voted_for, &_snapshot, &_start_index,  \
                       &_entries, &_n);                                       \
        munit_assert_int(_rv, ==, 0);                                         \
        for (_i = 0; _i < _n; _i++) {                                         \
            struct raft_entry *_entry = &_entries[_i];                        \
            if (_entry->batch != _batch) {                                    \
                _batch = _entry->batch;                                       \
                raft_free(_batch);                                            \
            }                                                                 \
        }                                                                     \
        if (_entries != NULL) {                                               \
            raft_free(_entries);                                              \
        }                                                                     \
        if (_snapshot != NULL) {                                              \
            raft_configuration_close(&_snapshot->configuration);              \
            munit_assert_int(_snapshot->n_bufs, ==, 1);                       \
            raft_free(_snapshot->bufs[0].base);                               \
            raft_free(_snapshot->bufs);                                       \
            raft_free(_snapshot);                                             \
        }                                                                     \
                                                                              \
        /* Persist the new snapshot. */                                       \
        _new_snapshot.index = INDEX;                                          \
        _new_snapshot.term = TERM;                                            \
        raft_configuration_init(&_new_snapshot.configuration);                \
        _rv = raft_configuration_add(&_new_snapshot.configuration, 1, "1",    \
                                     RAFT_VOTER);                             \
        munit_assert_int(_rv, ==, 0);                                         \
        _new_snapshot.bufs = &_new_snapshot_buf;                              \
        _new_snapshot.n_bufs = 1;                                             \
        _new_snapshot_buf.base = &_new_snapshot_data;                         \
        _new_snapshot_buf.len = sizeof _new_snapshot_data;                    \
        _req.data = &_done;                                                   \
        _rv =                                                                 \
            _io.snapshot_put(&_io, 10, &_req, &_new_snapshot, snapshotPutCb); \
        munit_assert_int(_rv, ==, 0);                                         \
        LOOP_RUN_UNTIL(&_done);                                               \
        raft_configuration_close(&_new_snapshot.configuration);               \
                                                                              \
        /* Shutdown the standalone raft_io instance. */                       \
        _done = false;                                                        \
        _io.data = &_done;                                                    \
        _io.close(&_io, closeCb);                                             \
        LOOP_RUN_UNTIL(&_done);                                               \
        raft_uv_close(&_io);                                                  \
        raft_uv_tcp_close(&_transport);                                       \
    } while (0);

/* Forcibly turn a closed segment into an open one, by renaming the underlying
 * file and growing its size. */
#define UNFINALIZE(FIRST_INDEX, LAST_INDEX, COUNTER)          \
    do {                                                      \
        const char *_filename1 =                              \
            CLOSED_SEGMENT_FILENAME(FIRST_INDEX, LAST_INDEX); \
        char _filename2[64];                                  \
        sprintf(_filename2, "open-%u", (unsigned)COUNTER);    \
        munit_assert_true(DirHasFile(f->dir, _filename1));    \
        munit_assert_false(DirHasFile(f->dir, _filename2));   \
        DirRenameFile(f->dir, _filename1, _filename2);        \
        DirGrowFile(f->dir, _filename2, SEGMENT_SIZE);        \
    } while (0)

#define LOAD_VARS                    \
    int _rv;                         \
    raft_term _term;                 \
    raft_id _voted_for;              \
    struct raft_snapshot *_snapshot; \
    raft_index _start_index;         \
    struct raft_entry *_entries;     \
    size_t _n;

/* Initialize the raft_io instance, then call raft_io->load() and assert that it
 * returns the given error code and message. */
#define LOAD_ERROR(RV, ERRMSG)                                    \
    do {                                                          \
        LOAD_VARS;                                                \
        SETUP_UV;                                                 \
        _rv = f->io.load(&f->io, &_term, &_voted_for, &_snapshot, \
                         &_start_index, &_entries, &_n);          \
        munit_assert_int(_rv, ==, RV);                            \
        munit_assert_string_equal(f->io.errmsg, ERRMSG);          \
    } while (0)

#define LOAD_ERROR_NO_SETUP(RV, ERRMSG)                           \
    do {                                                          \
        LOAD_VARS;                                                \
        _rv = f->io.load(&f->io, &_term, &_voted_for, &_snapshot, \
                         &_start_index, &_entries, &_n);          \
        munit_assert_int(_rv, ==, RV);                            \
        munit_assert_string_equal(f->io.errmsg, ERRMSG);          \
    } while (0)

#define LOAD_ERROR_NO_RECOVER(RV, ERRMSG)                         \
    do {                                                          \
        LOAD_VARS;                                                \
        SETUP_UV;                                                 \
        _rv = f->io.load(&f->io, &_term, &_voted_for, &_snapshot, \
                         &_start_index, &_entries, &_n);          \
        munit_assert_int(_rv, ==, RV);                            \
        munit_assert_string_equal(f->io.errmsg, ERRMSG);          \
    } while (0)

#define _LOAD(TERM, VOTED_FOR, SNAPSHOT, START_INDEX, N_ENTRIES)             \
    _rv = f->io.load(&f->io, &_term, &_voted_for, &_snapshot, &_start_index, \
                     &_entries, &_n);                                        \
    munit_assert_int(_rv, ==, 0);                                            \
    munit_assert_int(_term, ==, TERM);                                       \
    munit_assert_int(_voted_for, ==, VOTED_FOR);                             \
    munit_assert_int(_start_index, ==, START_INDEX);                         \
    if (_snapshot != NULL) {                                                 \
        struct snapshot *_expected = (struct snapshot *)(SNAPSHOT);          \
        munit_assert_ptr_not_null(_snapshot);                                \
        munit_assert_int(_snapshot->term, ==, _expected->term);              \
        munit_assert_int(_snapshot->index, ==, _expected->index);            \
        munit_assert_int(_snapshot->n_bufs, ==, 1);                          \
        munit_assert_int(*(uint64_t *)_snapshot->bufs[0].base, ==,           \
                         _expected->data);                                   \
        raft_configuration_close(&_snapshot->configuration);                 \
        raft_free(_snapshot->bufs[0].base);                                  \
        raft_free(_snapshot->bufs);                                          \
        raft_free(_snapshot);                                                \
    }                                                                        \
    if (_n != 0) {                                                           \
        munit_assert_int(_n, ==, N_ENTRIES);                                 \
        for (_i = 0; _i < _n; _i++) {                                        \
            struct raft_entry *_entry = &_entries[_i];                       \
            uint64_t _value = *(uint64_t *)_entry->buf.base;                 \
            munit_assert_int(_value, ==, _data);                             \
            _data++;                                                         \
        }                                                                    \
        for (_i = 0; _i < _n; _i++) {                                        \
            struct raft_entry *_entry = &_entries[_i];                       \
            if (_entry->batch != _batch) {                                   \
                _batch = _entry->batch;                                      \
                raft_free(_batch);                                           \
            }                                                                \
        }                                                                    \
        raft_free(_entries);                                                 \
    }

/* Initialize the raft_io instance, then invoke raft_io->load() and assert that
 * it returns the given state. If non-NULL, SNAPSHOT points to a struct snapshot
 * object whose attributes must match the loaded snapshot. ENTRIES_DATA is
 * supposed to be the integer stored in the data of first loaded entry. */
#define LOAD(TERM, VOTED_FOR, SNAPSHOT, START_INDEX, ENTRIES_DATA, N_ENTRIES) \
    do {                                                                      \
        LOAD_VARS;                                                            \
        void *_batch = NULL;                                                  \
        uint64_t _data = ENTRIES_DATA;                                        \
        unsigned _i;                                                          \
        SETUP_UV;                                                             \
        _LOAD(TERM, VOTED_FOR, SNAPSHOT, START_INDEX, N_ENTRIES)              \
    } while (0)

/* Same as LOAD but with auto recovery turned on. */
#define LOAD_WITH_AUTO_RECOVERY(TERM, VOTED_FOR, SNAPSHOT, START_INDEX, \
                                ENTRIES_DATA, N_ENTRIES)                \
    do {                                                                \
        LOAD_VARS;                                                      \
        void *_batch = NULL;                                            \
        uint64_t _data = ENTRIES_DATA;                                  \
        unsigned _i;                                                    \
        SETUP_UV;                                                       \
        raft_uv_set_auto_recovery(&f->io, true);                        \
        _LOAD(TERM, VOTED_FOR, SNAPSHOT, START_INDEX, N_ENTRIES)        \
    } while (0)

/* Same as LOAD without SETUP_UV */
#define LOAD_NO_SETUP(TERM, VOTED_FOR, SNAPSHOT, START_INDEX, ENTRIES_DATA, \
                      N_ENTRIES)                                            \
    do {                                                                    \
        LOAD_VARS;                                                          \
        void *_batch = NULL;                                                \
        uint64_t _data = ENTRIES_DATA;                                      \
        unsigned _i;                                                        \
        _LOAD(TERM, VOTED_FOR, SNAPSHOT, START_INDEX, N_ENTRIES)            \
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
    return f;
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_UV;
    TEAR_DOWN_UV_DEPS;
    free(f);
}

/******************************************************************************
 *
 * raft_io->load()
 *
 *****************************************************************************/

SUITE(load)

/* Load the initial state of a pristine server. */
TEST(load, emptyDir, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    LOAD(0,    /* term                                              */
         0,    /* voted for                                         */
         NULL, /* snapshot                                          */
         1,    /* start index                                       */
         0,    /* data for first loaded entry    */
         0     /* n entries                                         */
    );
    return MUNIT_OK;
}

static char *unknownFiles[] = {
    "garbage",
    "0000000000000000000000000001-00000000001garbage",
    "open-1garbage",
    NULL,
};

static MunitParameterEnum unknownFilesParams[] = {
    {"filename", unknownFiles},
    {NULL, NULL},
};

/* Files that are not part of the raft state are ignored. */
TEST(load, ignoreUnknownFiles, setUp, tearDown, 0, unknownFilesParams)
{
    struct fixture *f = data;
    const char *filename = munit_parameters_get(params, "filename");
    DirWriteFileWithZeros(f->dir, filename, 128);
    LOAD(0,    /* term                                              */
         0,    /* voted for                                         */
         NULL, /* snapshot                                          */
         1,    /* start index                                       */
         0,    /* data for first loaded entry    */
         0     /* n entries                                         */
    );
    return MUNIT_OK;
}

static char *unusableFiles[] = {"tmp-0000000001221212-0000000001221217",
                                "tmp-snapshot-15-8260687-512469866",
                                "snapshot-525-43326736-880259052",
                                "snapshot-999-13371337-880259052.meta",
                                "snapshot-20-8260687-512469866",
                                "snapshot-88-8260687-512469866.meta",
                                "snapshot-88-8260999-512469866.meta",
                                "tmp-snapshot-88-8260999-512469866.meta",
                                "tmp-snapshot-33-8260687-512469866",
                                "snapshot-33-8260687-512469866.meta",
                                "tmp-metadata1",
                                "tmp-metadata2",
                                "tmp-open1",
                                "tmp-open13",
                                NULL};

static MunitParameterEnum unusableFilesParams[] = {
    {"filename", unusableFiles},
    {NULL, NULL},
};

/* Files that can no longer be used are removed. */
TEST(load, removeUnusableFiles, setUp, tearDown, 0, unusableFilesParams)
{
    struct fixture *f = data;
    const char *filename = munit_parameters_get(params, "filename");
    DirWriteFileWithZeros(f->dir, filename, 128);
    munit_assert_true(DirHasFile(f->dir, filename));
    LOAD(0,    /* term                                              */
         0,    /* voted for                                         */
         NULL, /* snapshot                                          */
         1,    /* start index                                       */
         0,    /* data for first loaded entry    */
         0     /* n entries                                         */
    );
    munit_assert_false(DirHasFile(f->dir, filename));
    return MUNIT_OK;
}

/* The data directory has an empty open segment. */
TEST(load, emptyOpenSegment, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    DirWriteFile(f->dir, "open-1", NULL, 0);
    LOAD(0,    /* term                                              */
         0,    /* voted for                                         */
         NULL, /* snapshot                                          */
         1,    /* start index                                       */
         0,    /* data for first loaded entry    */
         0     /* n entries                                         */
    );
    /* The empty segment has been removed. */
    munit_assert_false(HAS_OPEN_SEGMENT_FILE(1));
    return MUNIT_OK;
}

/* The data directory has a freshly allocated open segment filled with zeros. */
TEST(load, openSegmentWithTrailingZeros, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    DirWriteFileWithZeros(f->dir, "open-1", 256);
    LOAD(0,    /* term                                              */
         0,    /* voted for                                         */
         NULL, /* snapshot                                          */
         1,    /* start index                                       */
         0,    /* data for first loaded entry    */
         0     /* n entries                                         */
    );
    /* The empty segment has been removed. */
    munit_assert_false(HAS_OPEN_SEGMENT_FILE(1));
    return MUNIT_OK;
}

/* The data directory has a valid closed and open segments. */
TEST(load, bothOpenAndClosedSegments, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND(2, 1);
    APPEND(1, 3);
    APPEND(1, 4);
    UNFINALIZE(4, 4, 1);
    LOAD(0,    /* term                                              */
         0,    /* voted for                                         */
         NULL, /* snapshot                                          */
         1,    /* start index                                       */
         1,    /* data for first loaded entry    */
         4     /* n entries                                         */
    );
    return MUNIT_OK;
}

/* The data directory has an allocated open segment which contains non-zero
 * corrupted data in its second batch. */
TEST(load, openSegmentWithNonZeroData, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    uint64_t corrupt = 123456789;
    APPEND(2, 1);
    UNFINALIZE(1, 2, 1);
    DirOverwriteFile(f->dir, "open-1", &corrupt, sizeof corrupt, 60);
    LOAD(0,    /* term                                              */
         0,    /* voted for                                         */
         NULL, /* snapshot                                          */
         1,    /* start index                                       */
         1,    /* data for first loaded entry    */
         1     /* n entries                                         */
    );

    /* The segment has been removed. */
    munit_assert_false(HAS_OPEN_SEGMENT_FILE(1));

    return MUNIT_OK;
}

/* The data directory has an open segment with a partially written batch that
 * needs to be truncated. */
TEST(load, openSegmentWithIncompleteBatch, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    uint8_t zero[256];
    APPEND(2, 1);
    UNFINALIZE(1, 2, 1);
    memset(zero, 0, sizeof zero);
    DirOverwriteFile(f->dir, "open-1", &zero, sizeof zero, 62);
    LOAD(0,    /* term                                              */
         0,    /* voted for                                         */
         NULL, /* snapshot                                          */
         1,    /* start index                                       */
         1,    /* data for first loaded entry    */
         1     /* n entries                                         */
    );
    return MUNIT_OK;
}

/* The data directory has an open segment whose first batch is only
 * partially written. In that case the segment gets removed. */
TEST(load, openSegmentWithIncompleteFirstBatch, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    uint8_t buf[5 * WORD_SIZE] = {
        UV__DISK_FORMAT, 0, 0, 0, 0, 0, 0, 0, /* Format version */
        0, 0, 0, 0, 0, 0, 0, 0, /* CRC32 checksums */
        0, 0, 0, 0, 0, 0, 0, 0, /* Number of entries */
	0, 0, 0, 0, 0, 0, 0, 0, /* Local data size */
        0, 0, 0, 0, 0, 0, 0, 0  /* Batch data */
    };
    APPEND(1, 1);
    UNFINALIZE(1, 1, 1);

    DirOverwriteFile(f->dir, "open-1", buf, sizeof buf, 0);

    LOAD(0,    /* term                                              */
         0,    /* voted for                                         */
         NULL, /* snapshot                                          */
         1,    /* start index                                       */
         0,    /* data for first loaded entry    */
         0     /* n entries                                         */
    );

    return MUNIT_OK;
}

/* The data directory has two segments, with the second having an entry. */
TEST(load, twoOpenSegments, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND(1, 1);
    APPEND(1, 2);
    UNFINALIZE(1, 1, 1);
    UNFINALIZE(2, 2, 2);

    LOAD(0,    /* term                                              */
         0,    /* voted for                                         */
         NULL, /* snapshot                                          */
         1,    /* start index                                       */
         1,    /* data for first loaded entry    */
         2     /* n entries                                         */
    );

    /* The first and second segments have been renamed. */
    munit_assert_false(HAS_OPEN_SEGMENT_FILE(1));
    munit_assert_false(HAS_OPEN_SEGMENT_FILE(2));
    munit_assert_true(HAS_CLOSED_SEGMENT_FILE(1, 1));
    munit_assert_true(HAS_CLOSED_SEGMENT_FILE(2, 2));

    return MUNIT_OK;
}

/* The data directory has two open segments, with the second one filled with
 * zeros. */
TEST(load, secondOpenSegmentIsAllZeros, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND(1, 1);
    UNFINALIZE(1, 1, 1);
    DirWriteFileWithZeros(f->dir, "open-2", SEGMENT_SIZE);

    LOAD(0,    /* term                                              */
         0,    /* voted for                                         */
         NULL, /* snapshot                                          */
         1,    /* start index                                       */
         1,    /* data for first loaded entry    */
         1     /* n entries                                         */
    );

    /* The first segment has been renamed. */
    munit_assert_false(HAS_OPEN_SEGMENT_FILE(1));
    munit_assert_true(HAS_CLOSED_SEGMENT_FILE(1, 1));

    /* The second segment has been removed. */
    munit_assert_false(HAS_OPEN_SEGMENT_FILE(2));

    return MUNIT_OK;
}

/* The data directory has two open segments, the first one has a corrupt header
 * and auto-recovery is on. */
TEST(load, twoOpenSegmentsFirstCorruptAutoRecovery, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND(1, 1);
    UNFINALIZE(1, 1, 1);
    DirWriteFileWithZeros(f->dir, "open-2", SEGMENT_SIZE);

    /* Corrupt open segment */
    uint64_t version = 0 /* Format version */;
    DirOverwriteFile(f->dir, "open-1", &version, sizeof version, 0);
    /* Load is successful and equals pristine condition. */
    LOAD_WITH_AUTO_RECOVERY(0,    /* term                           */
                            0,    /* voted for                      */
                            NULL, /* snapshot                       */
                            1,    /* start index                    */
                            0,    /* data for first loaded entry    */
                            0     /* n entries                      */
    );

    /* The open segments are renamed, and there is no closed segment. */
    munit_assert_false(HAS_OPEN_SEGMENT_FILE(1));
    munit_assert_false(HAS_OPEN_SEGMENT_FILE(2));
    munit_assert_false(HAS_CLOSED_SEGMENT_FILE(1, 1));

    return MUNIT_OK;
}

/* The data directory has two open segments, the first one has a corrupt header.
 */
TEST(load, twoOpenSegmentsFirstCorrupt, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND(1, 1);
    UNFINALIZE(1, 1, 1);
    DirWriteFileWithZeros(f->dir, "open-2", SEGMENT_SIZE);

    /* Corrupt open segment */
    uint64_t version = 0 /* Format version */;
    DirOverwriteFile(f->dir, "open-1", &version, sizeof version, 0);
    LOAD_ERROR(RAFT_CORRUPT,
               "load open segment open-1: unexpected format version 0");

    /* The open segments are renamed, and there is no closed segment. */
    munit_assert_true(HAS_OPEN_SEGMENT_FILE(1));
    munit_assert_true(HAS_OPEN_SEGMENT_FILE(2));
    return MUNIT_OK;
}

/* The data directory has a valid open segment. */
TEST(load, openSegment, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND(1, 1);
    UNFINALIZE(1, 1, 1);
    LOAD(0,    /* term                                              */
         0,    /* voted for                                         */
         NULL, /* snapshot                                          */
         1,    /* start index                                       */
         1,    /* data for first loaded entry    */
         1     /* n entries                                         */
    );
    return MUNIT_OK;
}

/* There is exactly one snapshot and no segments.  */
TEST(load, onlyOneSnapshot, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct snapshot snapshot = {
        1, /* term */
        1, /* index */
        1  /* data */
    };
    SNAPSHOT_PUT(1, 1, 1);
    LOAD(0,         /* term */
         0,         /* voted for */
         &snapshot, /* snapshot */
         2,         /* start index */
         0,         /* data for first loaded entry */
         0          /* n entries */
    );
    return MUNIT_OK;
}

/* There are several snapshots, including an incomplete one. The last one is
 * loaded and the incomplete or older ones are removed.  */
TEST(load, manySnapshots, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct snapshot snapshot = {
        2, /* term */
        9, /* index */
        4  /* data */
    };
    char filename[64];
    uint64_t now;

    /* Take a snapshot but then remove the data file, as if the server crashed
     * before it could complete writing it. */
    uv_update_time(&f->loop);
    now = uv_now(&f->loop);
    sprintf(filename, "snapshot-1-8-%ju", now);
    SNAPSHOT_PUT(1, 8, 1);
    DirRemoveFile(f->dir, filename);

    SNAPSHOT_PUT(1, 8, 2);
    SNAPSHOT_PUT(2, 6, 3);
    SNAPSHOT_PUT(2, 9, 4);
    LOAD(0,         /* term */
         0,         /* voted for */
         &snapshot, /* snapshot */
         10,        /* start index */
         0,         /* data for first loaded entry */
         0          /* n entries */
    );

    /* The orphaned .meta file is removed */
    char meta_filename[128];
    sprintf(meta_filename, "%s%s", filename, UV__SNAPSHOT_META_SUFFIX);
    munit_assert_false(DirHasFile(f->dir, meta_filename));

    return MUNIT_OK;
}

/* There are two snapshots, but the last one has an empty data file. The first
 * one is loaded and the empty one is discarded.  */
TEST(load, emptySnapshot, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct snapshot snapshot = {
        1, /* term */
        4, /* index */
        1  /* data */
    };
    char filename[64];
    uint64_t now;

    SNAPSHOT_PUT(1, 4, 1);

    /* Take a snapshot but then truncate the data file, as if the server ran out
     * of space before it could write it. */
    uv_update_time(&f->loop);
    now = uv_now(&f->loop);
    sprintf(filename, "snapshot-2-6-%ju", now);
    SNAPSHOT_PUT(2, 6, 2);
    DirTruncateFile(f->dir, filename, 0);

    LOAD(0,         /* term */
         0,         /* voted for */
         &snapshot, /* snapshot */
         5,         /* start index */
         0,         /* data for first loaded entry */
         0          /* n entries */
    );

    return MUNIT_OK;
}

/* There is an orphaned snapshot and an orphaned snapshot .meta file,
 * make sure they are removed */
TEST(load, orphanedSnapshotFiles, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    uv_update_time(&f->loop);
    uint64_t now = uv_now(&f->loop);

    struct snapshot expected_snapshot = {
        2,  /* term */
        16, /* index */
        4   /* data */
    };

    char filename1_removed[64];
    char metafilename1_removed[64];
    char filename2_removed[64];
    char metafilename2_removed[64];

    /* Take a snapshot but then remove the data file, as if the server crashed
     * before it could complete writing it. */
    sprintf(filename1_removed, "snapshot-2-18-%ju", now);
    sprintf(metafilename1_removed, "snapshot-2-18-%ju%s", now,
            UV__SNAPSHOT_META_SUFFIX);
    SNAPSHOT_PUT(2, 18, 1);
    munit_assert_true(DirHasFile(f->dir, filename1_removed));
    munit_assert_true(DirHasFile(f->dir, metafilename1_removed));
    DirRemoveFile(f->dir, filename1_removed);

    /* Take a snapshot but then remove the .meta file */
    now = uv_now(&f->loop);
    sprintf(filename2_removed, "snapshot-2-19-%ju", now);
    sprintf(metafilename2_removed, "snapshot-2-19-%ju%s", now,
            UV__SNAPSHOT_META_SUFFIX);
    SNAPSHOT_PUT(2, 19, 2);
    munit_assert_true(DirHasFile(f->dir, filename2_removed));
    munit_assert_true(DirHasFile(f->dir, metafilename2_removed));
    DirRemoveFile(f->dir, metafilename2_removed);

    /* Take a valid snapshot and make sure it's loaded */
    SNAPSHOT_PUT(2, 16, 4);
    LOAD(0,                  /* term */
         0,                  /* voted for */
         &expected_snapshot, /* snapshot */
         17,                 /* start index */
         0,                  /* data for first loaded entry */
         0                   /* n entries */
    );

    /* The orphaned files are removed */
    munit_assert_false(DirHasFile(f->dir, metafilename1_removed));
    munit_assert_false(DirHasFile(f->dir, filename2_removed));
    return MUNIT_OK;
}

/* The data directory has a closed segment with entries that are no longer
 * needed, since they are included in a snapshot. We still keep those segments
 * and just let the next snapshot logic delete them. */
TEST(load, closedSegmentWithEntriesBehindSnapshot, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct snapshot snapshot = {
        1, /* term */
        2, /* index */
        1  /* data */
    };
    APPEND(1, 1);
    SNAPSHOT_PUT(1, 2, 1);
    LOAD(0,         /* term */
         0,         /* voted for */
         &snapshot, /* snapshot */
         3,         /* start index */
         0,         /* data for first loaded entry */
         0          /* n entries */
    );
    munit_assert_true(HAS_CLOSED_SEGMENT_FILE(1, 1));
    return MUNIT_OK;
}

/* The data directory has a closed segment with entries that are no longer
 * needed, since they are included in a snapshot. However it also has an open
 * segment that has enough entries to reach the snapshot last index. */
TEST(load, openSegmentWithEntriesPastSnapshot, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct snapshot snapshot = {
        1, /* term */
        2, /* index */
        1  /* data */
    };
    APPEND(1, 1);
    APPEND(1, 2);
    SNAPSHOT_PUT(1, 2, 1);
    UNFINALIZE(2, 2, 1);
    LOAD(0,         /* term */
         0,         /* voted for */
         &snapshot, /* snapshot */
         1,         /* start index */
         1,         /* data for first loaded entry */
         2          /* n entries */
    );
    return MUNIT_OK;
}

/* The data directory has a closed segment whose filename encodes a number of
 * entries which is different then ones it actually contains. */
TEST(load, closedSegmentWithInconsistentFilename, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND(3, 1);
    DirRenameFile(f->dir, "0000000000000001-0000000000000003",
                  "0000000000000001-0000000000000004");
    LOAD_ERROR(RAFT_CORRUPT,
               "load closed segment 0000000000000001-0000000000000004: found 3 "
               "entries (expected 4)");
    return MUNIT_OK;
}

/* The data directory has a closed segment whose filename encodes a number of
 * entries which is different then ones it actually contains, and auto-recovery
 * is turned on. */
TEST(load,
     closedSegmentWithInconsistentFilenameAutoRecovery,
     setUp,
     tearDown,
     0,
     NULL)
{
    struct fixture *f = data;
    APPEND(3, 1);
    DirRenameFile(f->dir, "0000000000000001-0000000000000003",
                  "0000000000000001-0000000000000004");
    /* Load in pristine condition */
    LOAD_WITH_AUTO_RECOVERY(0,    /* term */
                            0,    /* voted for */
                            NULL, /* snapshot */
                            1,    /* start index */
                            0,    /* data for first loaded entry */
                            0     /* n entries */
    );
    return MUNIT_OK;
}

/* The data directory has a closed segment with entries that are no longer
 * needed, since they are included in a snapshot. It also has an open segment,
 * however that does not have enough entries to reach the snapshot last
 * index. */
TEST(load, openSegmentWithEntriesBehindSnapshot, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND(1, 1);
    APPEND(1, 2);
    SNAPSHOT_PUT(1, 3, 1);
    UNFINALIZE(2, 2, 1);
    LOAD_ERROR(RAFT_CORRUPT,
               "last entry on disk has index 2, which is behind last "
               "snapshot's index 3");
    return MUNIT_OK;
}

/* The data directory has a closed segment with entries that are no longer
 * needed, since they are included in a snapshot. It also has an open segment,
 * however that does not have enough entries to reach the snapshot last
 * index, and auto-receovery is turned on. */
TEST(load,
     openSegmentWithEntriesBehindSnapshotAutoRecovery,
     setUp,
     tearDown,
     0,
     NULL)
{
    struct fixture *f = data;
    struct snapshot snapshot = {
        1, /* term */
        3, /* index */
        1  /* data */
    };
    APPEND(1, 1);
    APPEND(1, 2);
    SNAPSHOT_PUT(1, 3, 1);
    UNFINALIZE(2, 2, 1);
    LOAD_WITH_AUTO_RECOVERY(0,         /* term */
                            0,         /* voted for */
                            &snapshot, /* snapshot */
                            4,         /* start index */
                            0,         /* data for first loaded entry */
                            0          /* n entries */
    );
    return MUNIT_OK;
}

/* The data directory contains a snapshot and an open segment containing a valid
 * entry, and no closed segments. */
TEST(load, openSegmentNoClosedSegmentsSnapshotPresent, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct snapshot snapshot = {
        1, /* term */
        3, /* index */
        1  /* data */
    };
    SNAPSHOT_PUT(1, 3, 1);
    APPEND(1, 4);
    UNFINALIZE(4, 4, 1);
    LOAD(0,         /* term */
         0,         /* voted for */
         &snapshot, /* snapshot */
         4,         /* start index */
         4,         /* data for first loaded entry */
         1          /* n entries */
    );
    return MUNIT_OK;
}

/* The data directory contains a snapshot and an open segment with a corrupt
 * format header and no closed segments. */
TEST(load,
     corruptOpenSegmentNoClosedSegmentsSnapshotPresent,
     setUp,
     tearDown,
     0,
     NULL)
{
    struct fixture *f = data;
    SNAPSHOT_PUT(1, 3, 1);
    APPEND(1, 4);
    UNFINALIZE(4, 4, 1);

    /* Corrupt open segment */
    uint64_t version = 0 /* Format version */;
    DirOverwriteFile(f->dir, "open-1", &version, sizeof version, 0);
    LOAD_ERROR(RAFT_CORRUPT,
               "load open segment open-1: unexpected format version 0");
    return MUNIT_OK;
}

/* The data directory contains a snapshot and an open segment with a corrupt
 * format header and no closed segments. Auto-recovery is turned on. */
TEST(load,
     corruptOpenSegmentNoClosedSegmentsSnapshotPresentWithAutoRecovery,
     setUp,
     tearDown,
     0,
     NULL)
{
    struct fixture *f = data;
    struct snapshot snapshot = {
        1, /* term */
        3, /* index */
        1  /* data */
    };
    SNAPSHOT_PUT(1, 3, 1);
    APPEND(1, 4);
    UNFINALIZE(4, 4, 1);

    /* Corrupt open segment */
    uint64_t version = 0 /* Format version */;
    DirOverwriteFile(f->dir, "open-1", &version, sizeof version, 0);
    /* Load is successful. */
    LOAD_WITH_AUTO_RECOVERY(0,         /* term */
                            0,         /* voted for */
                            &snapshot, /* snapshot */
                            4,         /* start index */
                            1,         /* data for first loaded entry */
                            1          /* n entries */
    );
    return MUNIT_OK;
}

/* The data directory contains a snapshot and an open segment with a corrupt
 * format header and a closed segment. */
TEST(load,
     corruptOpenSegmentClosedSegmentSnapshotPresent,
     setUp,
     tearDown,
     0,
     NULL)
{
    struct fixture *f = data;
    SNAPSHOT_PUT(1, 3, 1);
    APPEND(1, 4);
    APPEND(1, 5);
    UNFINALIZE(5, 5, 1);

    /* Corrupt open segment */
    uint64_t version = 0 /* Format version */;
    DirOverwriteFile(f->dir, "open-1", &version, sizeof version, 0);
    LOAD_ERROR(RAFT_CORRUPT,
               "load open segment open-1: unexpected format version 0");
    return MUNIT_OK;
}

/* The data directory contains a snapshot and an open segment with a corrupt
 * format header and a closed segment. Auto-recovery is turned on. */
TEST(load,
     corruptOpenSegmentClosedSegmentSnapshotPresentWithAutoRecovery,
     setUp,
     tearDown,
     0,
     NULL)
{
    struct fixture *f = data;
    struct snapshot snapshot = {
        1, /* term */
        3, /* index */
        1  /* data */
    };
    SNAPSHOT_PUT(1, 3, 1);
    APPEND(1, 4);
    APPEND(1, 5);
    UNFINALIZE(5, 5, 1);

    /* Corrupt open segment */
    uint64_t version = 0 /* Format version */;
    DirOverwriteFile(f->dir, "open-1", &version, sizeof version, 0);

    /* Load is successful. */
    LOAD_WITH_AUTO_RECOVERY(0,         /* term */
                            0,         /* voted for */
                            &snapshot, /* snapshot */
                            4,         /* start index */
                            4,         /* data for first loaded entry */
                            1          /* n entries */
    );

    /* Open segment has been renamed */
    munit_assert_false(DirHasFile(f->dir, "open-1"));
    return MUNIT_OK;
}

/* The data directory contains a snapshot and an open segment with a corrupt
 * format header and multiple closed segment. Auto-recovery is turned on. */
TEST(load,
     corruptOpenSegmentClosedSegmentsSnapshotPresentWithAutoRecovery,
     setUp,
     tearDown,
     0,
     NULL)
{
    struct fixture *f = data;
    struct snapshot snapshot = {
        1, /* term */
        3, /* index */
        1  /* data */
    };
    SNAPSHOT_PUT(1, 3, 1);
    APPEND(1, 4);
    APPEND(1, 5);
    APPEND(1, 6);
    UNFINALIZE(6, 6, 1);

    /* Corrupt open segment */
    uint64_t version = 0 /* Format version */;
    DirOverwriteFile(f->dir, "open-1", &version, sizeof version, 0);

    LOAD_WITH_AUTO_RECOVERY(0,         /* term */
                            0,         /* voted for */
                            &snapshot, /* snapshot */
                            4,         /* start index */
                            4,         /* data for first loaded entry */
                            2          /* n entries */
    );
    /* Open segment has been renamed during the first load */
    munit_assert_false(DirHasFile(f->dir, "open-1"));
    return MUNIT_OK;
}

/* The data directory contains a snapshot and an open segment with a corrupt
 * format header and multiple closed segment. */
TEST(load,
     corruptOpenSegmentClosedSegmentsSnapshotPresent,
     setUp,
     tearDown,
     0,
     NULL)
{
    struct fixture *f = data;
    SNAPSHOT_PUT(1, 3, 1);
    APPEND(1, 4);
    APPEND(1, 5);
    APPEND(1, 6);
    UNFINALIZE(6, 6, 1);

    /* Corrupt open segment */
    uint64_t version = 0 /* Format version */;
    DirOverwriteFile(f->dir, "open-1", &version, sizeof version, 0);
    LOAD_ERROR(RAFT_CORRUPT,
               "load open segment open-1: unexpected format version 0");
    return MUNIT_OK;
}

/* The data directory contains a closed segment and an open segment with a
 * corrupt format header and no snapshot. */
TEST(load, corruptOpenSegmentClosedSegments, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND(4, 1);
    APPEND(1, 5);
    UNFINALIZE(5, 5, 1);

    /* Corrupt open segment */
    uint64_t version = 0 /* Format version */;
    DirOverwriteFile(f->dir, "open-1", &version, sizeof version, 0);
    LOAD_ERROR(RAFT_CORRUPT,
               "load open segment open-1: unexpected format version 0");
    return MUNIT_OK;
}

/* The data directory contains a closed segment and an open segment with a
 * corrupt format header and no snapshot. Auto-recovery is turned on. */
TEST(load,
     corruptOpenSegmentClosedSegmentsWithAutoRecovery,
     setUp,
     tearDown,
     0,
     NULL)
{
    struct fixture *f = data;
    APPEND(4, 1);
    APPEND(1, 5);
    UNFINALIZE(5, 5, 1);

    /* Corrupt open segment */
    uint64_t version = 0 /* Format version */;
    DirOverwriteFile(f->dir, "open-1", &version, sizeof version, 0);
    /* load is successful. */
    LOAD_WITH_AUTO_RECOVERY(0,    /* term */
                            0,    /* voted for */
                            NULL, /* snapshot */
                            1,    /* start index */
                            1,    /* data for first loaded entry */
                            4     /* n entries */
    );
    /* Open segment has been renamed */
    munit_assert_false(DirHasFile(f->dir, "open-1"));
    return MUNIT_OK;
}

/* The data directory contains a closed segment and two open segments.
 * The first open segment has a corrupt header. */
TEST(load, corruptOpenSegmentsClosedSegments, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND(3, 1);
    APPEND(1, 4);
    APPEND(1, 5);
    UNFINALIZE(4, 4, 1);
    UNFINALIZE(5, 5, 2);

    /* Corrupt open segment */
    uint64_t version = 0 /* Format version */;
    DirOverwriteFile(f->dir, "open-1", &version, sizeof version, 0);
    LOAD_ERROR(RAFT_CORRUPT,
               "load open segment open-1: unexpected format version 0");

    return MUNIT_OK;
}

/* The data directory contains a closed segment and two open segments.
 * The first open segment has a corrupt header. Auto-recovery is turned on. */
TEST(load,
     corruptOpenSegmentsClosedSegmentsWithAutoRecovery,
     setUp,
     tearDown,
     0,
     NULL)
{
    struct fixture *f = data;
    APPEND(3, 1);
    APPEND(1, 4);
    APPEND(1, 5);
    UNFINALIZE(4, 4, 1);
    UNFINALIZE(5, 5, 2);

    /* Corrupt open segment */
    uint64_t version = 0 /* Format version */;
    DirOverwriteFile(f->dir, "open-1", &version, sizeof version, 0);

    LOAD_WITH_AUTO_RECOVERY(0,    /* term */
                            0,    /* voted for */
                            NULL, /* snapshot */
                            1,    /* start index */
                            1,    /* data for first loaded entry */
                            3     /* n entries */
    );

    /* Open segments have been renamed */
    munit_assert_false(DirHasFile(f->dir, "open-1"));
    munit_assert_false(DirHasFile(f->dir, "open-2"));
    return MUNIT_OK;
}

/* The data directory contains a closed segment and two open segments.
 * The second open segment has a corrupt header. */
TEST(load, corruptLastOpenSegmentClosedSegments, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND(3, 1);
    APPEND(1, 4);
    APPEND(1, 5);
    UNFINALIZE(4, 4, 1);
    UNFINALIZE(5, 5, 2);

    /* Corrupt open segment */
    uint64_t version = 0 /* Format version */;
    DirOverwriteFile(f->dir, "open-2", &version, sizeof version, 0);
    LOAD_ERROR(RAFT_CORRUPT,
               "load open segment open-2: unexpected format version 0");

    return MUNIT_OK;
}

/* The data directory contains a closed segment and two open segments.
 * The second open segment has a corrupt header. Auto-recovery is turned on. */
TEST(load,
     corruptLastOpenSegmentClosedSegmentsWithAutoRecovery,
     setUp,
     tearDown,
     0,
     NULL)
{
    struct fixture *f = data;
    APPEND(3, 1);
    APPEND(1, 4);
    APPEND(1, 5);
    UNFINALIZE(4, 4, 1);
    UNFINALIZE(5, 5, 2);

    /* Corrupt open segment */
    uint64_t version = 0 /* Format version */;
    DirOverwriteFile(f->dir, "open-2", &version, sizeof version, 0);

    LOAD_WITH_AUTO_RECOVERY(0,    /* term */
                            0,    /* voted for */
                            NULL, /* snapshot */
                            1,    /* start index */
                            1,    /* data for first loaded entry */
                            4     /* n entries */
    );
    /* Open segment has been renamed during the first load */
    munit_assert_false(DirHasFile(f->dir, "open-2"));
    return MUNIT_OK;
}

/* The data directory has several closed segments, all with entries compatible
 * with the snapshot. */
TEST(load, closedSegmentsOverlappingWithSnapshot, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct snapshot snapshot = {
        1, /* term */
        4, /* index */
        1  /* data */
    };
    APPEND(1, 1);
    APPEND(2, 2);
    APPEND(3, 4);
    SNAPSHOT_PUT(1, 4, 1);
    LOAD(0,         /* term */
         0,         /* voted for */
         &snapshot, /* snapshot */
         1,         /* start index */
         1,         /* data for first loaded entry */
         6          /* n entries */
    );
    return MUNIT_OK;
}

/* The data directory has several closed segments, the last of which is corrupt.
 * There is a snapshot. */
TEST(load,
     closedSegmentsWithSnapshotLastSegmentCorrupt,
     setUp,
     tearDown,
     0,
     NULL)
{
    struct fixture *f = data;
    SNAPSHOT_PUT(1, 4, 1);
    APPEND(1, 5);
    APPEND(2, 6);
    APPEND(2, 8);

    /* Corrupt the last closed segment */
    size_t offset =
        WORD_SIZE /* Format version */ + WORD_SIZE / 2 /* Header checksum */;
    uint32_t corrupted = 123456789;
    DirOverwriteFile(f->dir, CLOSED_SEGMENT_FILENAME(8, 9), &corrupted,
                     sizeof corrupted, offset);
    LOAD_ERROR(RAFT_CORRUPT,
               "load closed segment 0000000000000008-0000000000000009: entries "
               "batch 1 starting at byte 8: data checksum mismatch");
    return MUNIT_OK;
}

/* The data directory has several closed segments, the last of which is corrupt.
 * There is a snapshot. Auto-recovery is turned on. */
TEST(load,
     closedSegmentsWithSnapshotLastSegmentCorruptAutoRecovery,
     setUp,
     tearDown,
     0,
     NULL)
{
    struct fixture *f = data;
    struct snapshot snapshot = {
        1, /* term */
        4, /* index */
        1  /* data */
    };
    SNAPSHOT_PUT(1, 4, 1);
    APPEND(1, 5);
    APPEND(2, 6);
    APPEND(2, 8);

    /* Corrupt the last closed segment */
    size_t offset =
        WORD_SIZE /* Format version */ + WORD_SIZE / 2 /* Header checksum */;
    uint32_t corrupted = 123456789;
    DirOverwriteFile(f->dir, CLOSED_SEGMENT_FILENAME(8, 9), &corrupted,
                     sizeof corrupted, offset);
    LOAD_WITH_AUTO_RECOVERY(0,         /* term */
                            0,         /* voted for */
                            &snapshot, /* snapshot */
                            5,         /* start index */
                            5,         /* data for first loaded entry */
                            3          /* n entries */
    );
    return MUNIT_OK;
}

/* The data directory has several closed segments, the last of which is corrupt.
 * There is an open segment and a snapshot. Auto-recovery is turned on. */
TEST(load,
     closedSegmentsWithSnapshotLastSegmentCorruptOpenSegmentWithAutoRecovery,
     setUp,
     tearDown,
     0,
     NULL)
{
    struct fixture *f = data;
    struct snapshot snapshot = {
        1, /* term */
        4, /* index */
        1  /* data */
    };
    SNAPSHOT_PUT(1, 4, 1);
    APPEND(1, 5);
    APPEND(2, 6);
    APPEND(1, 8);
    APPEND(1, 9);
    UNFINALIZE(9, 9, 1);

    /* Corrupt the last closed segment */
    size_t offset =
        WORD_SIZE /* Format version */ + WORD_SIZE / 2 /* Header checksum */;
    uint32_t corrupted = 123456789;
    DirOverwriteFile(f->dir, CLOSED_SEGMENT_FILENAME(8, 8), &corrupted,
                     sizeof corrupted, offset);
    munit_assert_true(HAS_OPEN_SEGMENT_FILE(1));

    LOAD_WITH_AUTO_RECOVERY(0,         /* term */
                            0,         /* voted for */
                            &snapshot, /* snapshot */
                            5,         /* start index */
                            5,         /* data for first loaded entry */
                            3          /* n entries */
    );
    munit_assert_false(HAS_OPEN_SEGMENT_FILE(1));
    return MUNIT_OK;
}

/* The data directory has several closed segments, the last of which is corrupt.
 * There is an open segment and a snapshot. */
TEST(load,
     closedSegmentsWithSnapshotLastSegmentCorruptOpenSegment,
     setUp,
     tearDown,
     0,
     NULL)
{
    struct fixture *f = data;
    SNAPSHOT_PUT(1, 4, 1);
    APPEND(1, 5);
    APPEND(2, 6);
    APPEND(1, 8);
    APPEND(1, 9);
    UNFINALIZE(9, 9, 1);

    /* Corrupt the last closed segment */
    size_t offset =
        WORD_SIZE /* Format version */ + WORD_SIZE / 2 /* Header checksum */;
    uint32_t corrupted = 123456789;
    DirOverwriteFile(f->dir, CLOSED_SEGMENT_FILENAME(8, 8), &corrupted,
                     sizeof corrupted, offset);
    munit_assert_true(HAS_OPEN_SEGMENT_FILE(1));
    LOAD_ERROR(RAFT_CORRUPT,
               "load closed segment 0000000000000008-0000000000000008: entries "
               "batch 1 starting at byte 8: data checksum mismatch");
    return MUNIT_OK;
}

/* The data directory has several closed segments, the second to last one of
 * which is corrupt. There is a snapshot. */
TEST(load,
     closedSegmentsWithSnapshotSecondLastSegmentCorrupt,
     setUp,
     tearDown,
     0,
     NULL)
{
    struct fixture *f = data;
    SNAPSHOT_PUT(1, 4, 1);
    APPEND(1, 5);
    APPEND(2, 6);
    APPEND(2, 8);

    /* Corrupt the second last closed segment */
    size_t offset =
        WORD_SIZE /* Format version */ + WORD_SIZE / 2 /* Header checksum */;
    uint32_t corrupted = 123456789;
    DirOverwriteFile(f->dir, CLOSED_SEGMENT_FILENAME(6, 7), &corrupted,
                     sizeof corrupted, offset);
    LOAD_ERROR(RAFT_CORRUPT,
               "load closed segment 0000000000000006-0000000000000007: entries "
               "batch 1 starting at byte 8: data checksum mismatch");

    /* Second load still fails. */
    LOAD_ERROR_NO_SETUP(
        RAFT_CORRUPT,
        "load closed segment 0000000000000006-0000000000000007: entries "
        "batch 1 starting at byte 8: data checksum mismatch");

    return MUNIT_OK;
}

/* The data directory has several closed segments, some of which have a gap,
 * which is still compatible with the snapshot. */
TEST(load, nonContiguousClosedSegments, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct snapshot snapshot = {
        1, /* term */
        4, /* index */
        1  /* data */
    };
    APPEND(1, 1);
    APPEND(2, 2);
    APPEND(3, 4);
    SNAPSHOT_PUT(1, 4, 1);
    DirRemoveFile(f->dir, CLOSED_SEGMENT_FILENAME(2, 3));
    LOAD(0,         /* term */
         0,         /* voted for */
         &snapshot, /* snapshot */
         4,         /* start index */
         4,         /* data for first loaded entry */
         3          /* n entries */
    );
    return MUNIT_OK;
}

/* If the data directory has a closed segment whose start index is beyond the
 * snapshot's last index, an error is returned. */
TEST(load, closedSegmentWithEntriesPastSnapshot, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    uint64_t now;
    char errmsg[128];
    APPEND(5, 1);
    APPEND(1, 5);
    uv_update_time(&f->loop);
    now = uv_now(&f->loop);
    sprintf(errmsg,
            "closed segment 0000000000000006-0000000000000006 is past last "
            "snapshot snapshot-1-4-%ju",
            now);
    SNAPSHOT_PUT(1, 4, 1);
    DirRemoveFile(f->dir, CLOSED_SEGMENT_FILENAME(1, 5));
    LOAD_ERROR(RAFT_CORRUPT, errmsg);
    return MUNIT_OK;
}

/* The data directory has an open segment which has incomplete format data. */
TEST(load, openSegmentWithIncompleteFormat, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    DirWriteFileWithZeros(f->dir, "open-1", WORD_SIZE / 2);
    LOAD_ERROR(RAFT_IOERR, "load open segment open-1: file has only 4 bytes");
    return MUNIT_OK;
}

/* The data directory has an open segment which has an incomplete batch
 * preamble. */
TEST(load, openSegmentWithIncompletePreamble, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    size_t offset = WORD_SIZE /* Format version */ + WORD_SIZE /* Checksums */;
    APPEND(1, 1);
    UNFINALIZE(1, 1, 1);
    DirTruncateFile(f->dir, "open-1", offset);
    LOAD_ERROR(RAFT_IOERR,
               "load open segment open-1: entries batch 1 starting at byte 16: "
               "read preamble: short read: 0 bytes instead of 8");
    return MUNIT_OK;
}

/* The data directory has an open segment which has incomplete batch header. */
TEST(load, openSegmentWithIncompleteBatchHeader, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    size_t offset = WORD_SIZE + /* Format version */
                    WORD_SIZE + /* Checksums */
                    WORD_SIZE + /* Number of entries */
                    WORD_SIZE /* Partial batch header */;

    APPEND(1, 1);
    UNFINALIZE(1, 1, 1);
    DirTruncateFile(f->dir, "open-1", offset);
    const char *msg =
	    "load open segment open-1: entries batch 1 starting at byte 8: "
	    "read header: short read: 8 bytes instead of 16";
    LOAD_ERROR(RAFT_IOERR, msg);
    return MUNIT_OK;
}

/* The data directory has an open segment which has incomplete batch data. */
TEST(load, openSegmentWithIncompleteBatchData, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    size_t offset = WORD_SIZE + /* Format version */
                    WORD_SIZE + /* Checksums */
                    WORD_SIZE + /* Number of entries */
                    WORD_SIZE + /* Entry term */
                    WORD_SIZE + /* Entry type and data size */
                    WORD_SIZE / 2 /* Partial entry data */;

    APPEND(1, 1);
    UNFINALIZE(1, 1, 1);
    DirTruncateFile(f->dir, "open-1", offset);

    const char *msg =
	    "load open segment open-1: entries batch 1 starting at byte 8: "
	    "read data: short read: 4 bytes instead of 8";
    LOAD_ERROR(RAFT_IOERR, msg);
    return MUNIT_OK;
}

/* The data directory has a closed segment which has corrupted batch header. */
TEST(load, closedSegmentWithCorruptedBatchHeader, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    size_t offset = WORD_SIZE /* Format version */;
    uint64_t corrupted = 12345678;
    APPEND(1, 1);
    DirOverwriteFile(f->dir, CLOSED_SEGMENT_FILENAME(1, 1), &corrupted,
                     sizeof corrupted, offset);
    LOAD_ERROR(RAFT_CORRUPT,
               "load closed segment 0000000000000001-0000000000000001: entries "
               "batch 1 starting at byte 8: header checksum mismatch");
    return MUNIT_OK;
}

/* The data directory has a closed segment which has corrupted batch data. */
TEST(load, closedSegmentWithCorruptedBatchData, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    size_t offset =
        WORD_SIZE /* Format version */ + WORD_SIZE / 2 /* Header checksum */;
    uint32_t corrupted = 123456789;
    APPEND(1, 1);
    DirOverwriteFile(f->dir, CLOSED_SEGMENT_FILENAME(1, 1), &corrupted,
                     sizeof corrupted, offset);
    LOAD_ERROR(RAFT_CORRUPT,
               "load closed segment 0000000000000001-0000000000000001: entries "
               "batch 1 starting at byte 8: data checksum mismatch");
    return MUNIT_OK;
}

/* The data directory has a closed segment whose first index does not match what
 * we expect. */
TEST(load, closedSegmentWithBadIndex, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND(1, 1);
    APPEND(1, 2);
    DirRemoveFile(f->dir, CLOSED_SEGMENT_FILENAME(1, 1));
    LOAD_ERROR(RAFT_CORRUPT,
               "unexpected closed segment 0000000000000002-0000000000000002: "
               "first index should have been 1");
    return MUNIT_OK;
}

/* The data directory has an empty closed segment. */
TEST(load, emptyClosedSegment, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    DirWriteFile(f->dir, CLOSED_SEGMENT_FILENAME(1, 1), NULL, 0);
    LOAD_ERROR(
        RAFT_CORRUPT,
        "load closed segment 0000000000000001-0000000000000001: file is empty");
    return MUNIT_OK;
}

/* The data directory has a closed segment with an unexpected format. */
TEST(load, closedSegmentWithBadFormat, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    uint8_t buf[8] = {3, 0, 0, 0, 0, 0, 0, 0};
    DirWriteFile(f->dir, CLOSED_SEGMENT_FILENAME(1, 1), buf, sizeof buf);
    LOAD_ERROR(RAFT_CORRUPT,
               "load closed segment 0000000000000001-0000000000000001: "
               "unexpected format version 3");
    return MUNIT_OK;
}

/* The data directory has an open segment which is not readable. */
TEST(load, openSegmentWithNoAccessPermission, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    /* Skip the test when running as root, since EACCES would not be triggered
     * in that case. */
    if (getuid() == 0) {
        SETUP_UV; /* Setup the uv object since teardown expects it. */
        return MUNIT_SKIP;
    }

    APPEND(1, 1);
    UNFINALIZE(1, 1, 1);
    DirMakeFileUnreadable(f->dir, "open-1");
    LOAD_ERROR(RAFT_IOERR,
               "load open segment open-1: read file: open: permission denied");
    return MUNIT_OK;
}

/* The data directory has an open segment with format set to 0 and non-zero
 * content. */
TEST(load, openSegmentWithZeroFormatAndThenData, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    uint64_t version = 0 /* Format version */;
    APPEND(1, 1);
    UNFINALIZE(1, 1, 1);
    DirOverwriteFile(f->dir, "open-1", &version, sizeof version, 0);
    LOAD_ERROR(RAFT_CORRUPT,
               "load open segment open-1: unexpected format version 0");
    return MUNIT_OK;
}

/* The data directory has an open segment with an unexpected format. */
TEST(load, openSegmentWithBadFormat, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    uint8_t version[8] = {3, 0, 0, 0, 0, 0, 0, 0};
    APPEND(1, 1);
    UNFINALIZE(1, 1, 1);
    DirOverwriteFile(f->dir, "open-1", version, sizeof version, 0);
    LOAD_ERROR(RAFT_CORRUPT,
               "load open segment open-1: unexpected format version 3");
    return MUNIT_OK;
}
