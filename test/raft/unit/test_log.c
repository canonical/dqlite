#include "../../../src/raft/configuration.h"
#include "../../../src/raft/log.h"
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
    struct raft_log *log;
};

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

/* Accessors */
#define NUM_ENTRIES logNumEntries(f->log)
#define LAST_INDEX logLastIndex(f->log)
#define TERM_OF(INDEX) logTermOf(f->log, INDEX)
#define LAST_TERM logLastTerm(f->log)
#define GET(INDEX) logGet(f->log, INDEX)

/* Append one command entry with the given term and a hard-coded payload. */
#define APPEND(TERM)                                              \
    {                                                             \
        struct raft_buffer buf_;                                  \
        int rv_;                                                  \
        buf_.base = raft_malloc(8);                               \
        buf_.len = 8;                                             \
        strcpy(buf_.base, "hello");                               \
        rv_ = logAppend(f->log, TERM, RAFT_COMMAND, buf_, true, NULL); \
        munit_assert_int(rv_, ==, 0);                             \
    }

/* Same as APPEND, but repeated N times. */
#define APPEND_MANY(TERM, N)         \
    {                                \
        int i_;                      \
        for (i_ = 0; i_ < N; i_++) { \
            APPEND(TERM);            \
        }                            \
    }

/* Invoke append and assert that it returns the given error. */
#define APPEND_ERROR(TERM, RV)                                    \
    {                                                             \
        struct raft_buffer buf_;                                  \
        int rv_;                                                  \
        buf_.base = raft_malloc(8);                               \
        buf_.len = 8;                                             \
        rv_ = logAppend(f->log, TERM, RAFT_COMMAND, buf_, true, NULL); \
        munit_assert_int(rv_, ==, RV);                            \
        raft_free(buf_.base);                                     \
    }

/* Append N entries all belonging to the same batch. Each entry will have 64-bit
 * payload set to i * 1000, where i is the index of the entry in the batch. */
#define APPEND_BATCH(N)                                           \
    {                                                             \
        void *batch;                                              \
        size_t offset;                                            \
        int i;                                                    \
        batch = raft_malloc(8 * N);                               \
        munit_assert_ptr_not_null(batch);                         \
        offset = 0;                                               \
        for (i = 0; i < N; i++) {                                 \
            struct raft_buffer buf;                               \
            int rv;                                               \
            buf.base = (uint8_t *)batch + offset;                 \
            buf.len = 8;                                          \
            *(uint64_t *)buf.base = i * 1000;                     \
            rv = logAppend(f->log, 1, RAFT_COMMAND, buf, true, batch); \
            munit_assert_int(rv, ==, 0);                          \
            offset += 8;                                          \
        }                                                         \
    }

#define ACQUIRE(INDEX)                                 \
    {                                                  \
        int rv2;                                       \
        rv2 = logAcquire(f->log, INDEX, &entries, &n); \
        munit_assert_int(rv2, ==, 0);                  \
    }

#define RELEASE(INDEX) logRelease(f->log, INDEX, entries, n);

#define TRUNCATE(N) logTruncate(f->log, N)
#define SNAPSHOT(INDEX, TRAILING) logSnapshot(f->log, INDEX, TRAILING)
#define RESTORE(INDEX, TERM) logRestore(f->log, INDEX, TERM)

/******************************************************************************
 *
 * Set up an empty configuration.
 *
 *****************************************************************************/

static void *setUp(const MunitParameter params[], MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SET_UP_HEAP;
    f->log = logInit();
    if (f->log == NULL) {
        munit_assert_true(false);
    }
    return f;
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    logClose(f->log);
    TEAR_DOWN_HEAP;
    free(f);
}

/******************************************************************************
 *
 * Assertions
 *
 *****************************************************************************/

/* Assert the state of the fixture's log in terms of size, front/back indexes,
 * offset and number of entries. */
#define ASSERT(SIZE, FRONT, BACK, OFFSET, N)      \
    munit_assert_int(f->log->size, ==, SIZE);     \
    munit_assert_int(f->log->front, ==, FRONT);   \
    munit_assert_int(f->log->back, ==, BACK);     \
    munit_assert_int(f->log->offset, ==, OFFSET); \
    munit_assert_int(logNumEntries(f->log), ==, N)

/* Assert the last index and term of the most recent snapshot. */
#define ASSERT_SNAPSHOT(INDEX, TERM)                          \
    munit_assert_int(f->log->snapshot.last_index, ==, INDEX); \
    munit_assert_int(f->log->snapshot.last_term, ==, TERM)

/* Assert that the term of entry at INDEX equals TERM. */
#define ASSERT_TERM_OF(INDEX, TERM)              \
    {                                            \
        const struct raft_entry *entry;          \
        entry = logGet(f->log, INDEX);           \
        munit_assert_ptr_not_null(entry);        \
        munit_assert_int(entry->term, ==, TERM); \
    }

/* Assert that the number of outstanding references for the entry at INDEX
 * equals COUNT. */
#define ASSERT_REFCOUNT(INDEX, COUNT)                                 \
    {                                                                 \
        size_t i;                                                     \
        munit_assert_ptr_not_null(f->log->refs);                      \
        for (i = 0; i < f->log->refs_size; i++) {                     \
            if (f->log->refs[i].index == INDEX) {                     \
                munit_assert_int(f->log->refs[i].count, ==, COUNT);   \
                break;                                                \
            }                                                         \
        }                                                             \
        if (i == f->log->refs_size) {                                 \
            munit_errorf("no refcount found for entry with index %d", \
                         (int)INDEX);                                 \
        }                                                             \
    }

/******************************************************************************
 *
 * logNumEntries
 *
 *****************************************************************************/

SUITE(logNumEntries)

/* If the log is empty, the return value is zero. */
TEST(logNumEntries, empty, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    munit_assert_int(NUM_ENTRIES, ==, 0);
    return MUNIT_OK;
}

/* The log is not wrapped. */
TEST(logNumEntries, not_wrapped, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND(1 /* term */);
    munit_assert_int(NUM_ENTRIES, ==, 1);
    return MUNIT_OK;
}

/* The log is wrapped. */
TEST(logNumEntries, wrapped, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND_MANY(1 /* term */, 5 /* n entries */);
    SNAPSHOT(4 /* last_index */, 1 /* trailing */);
    APPEND_MANY(1 /* term */, 2 /* n entries */);
    munit_assert_int(NUM_ENTRIES, ==, 4);
    return MUNIT_OK;
}

/* The log has an offset and is empty. */
TEST(logNumEntries, offset, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND_MANY(1 /* term */, 5 /* n entries */);
    SNAPSHOT(5 /* last index */, 0 /* trailing */);
    munit_assert_int(NUM_ENTRIES, ==, 0);
    return MUNIT_OK;
}

/* The log has an offset and is not empty. */
TEST(logNumEntries, offsetNotEmpty, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND_MANY(1 /* term */, 5 /* n entries */);
    SNAPSHOT(4 /* last index */, 2 /* trailing */);
    munit_assert_int(NUM_ENTRIES, ==, 3);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * logLastIndex
 *
 *****************************************************************************/

SUITE(logLastIndex)

/* If the log is empty, last index is 0. */
TEST(logLastIndex, empty, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    munit_assert_int(LAST_INDEX, ==, 0);
    return MUNIT_OK;
}

/* If the log is empty and has an offset, last index is calculated
   accordingly. */
TEST(logLastIndex, emptyWithOffset, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND(1);
    SNAPSHOT(1, 0);
    munit_assert_int(LAST_INDEX, ==, 1);
    return MUNIT_OK;
}

/* The log has one entry. */
TEST(logLastIndex, one, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND(1 /* term */);
    munit_assert_int(LAST_INDEX, ==, 1);
    return MUNIT_OK;
}

/* The log has two entries. */
TEST(logLastIndex, two, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND_MANY(1 /* term */, 2 /* n */);
    munit_assert_int(LAST_INDEX, ==, 2);
    return MUNIT_OK;
}

/* If the log starts at a certain offset, the last index is bumped
 * accordingly. */
TEST(logLastIndex, twoWithOffset, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND_MANY(1 /* term */, 5 /* n */);
    SNAPSHOT(5 /* last index */, 2 /* trailing */);
    munit_assert_int(LAST_INDEX, ==, 5);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * logLastTerm
 *
 *****************************************************************************/

SUITE(logLastTerm)

/* If the log is empty, return zero. */
TEST(logLastTerm, empty, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    munit_assert_int(LAST_TERM, ==, 0);
    return MUNIT_OK;
}

/* If the log has a snapshot and no outstanding entries, return the last term of
 * the snapshot. */
TEST(logLastTerm, snapshot, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND(1 /* term */);
    SNAPSHOT(1 /* last index */, 0 /* trailing */);
    munit_assert_int(LAST_TERM, ==, 1);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * logTermOf
 *
 *****************************************************************************/

SUITE(logTermOf)

/* If the given index is beyond the last index, return 0. */
TEST(logTermOf, beyondLast, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    munit_assert_int(TERM_OF(2), ==, 0);
    munit_assert_int(TERM_OF(10), ==, 0);
    return MUNIT_OK;
}

/* If the log is empty but has a snapshot, and the given index matches the last
 * index of the snapshot, return the snapshot last term. */
TEST(logTermOf, snapshotLastIndex, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND_MANY(1 /* term */, 5 /* n entries */);
    SNAPSHOT(5 /* last entry */, 0 /* trailing */);
    munit_assert_int(TERM_OF(5), ==, 1);
    return MUNIT_OK;
}

/* The log has one entry. */
TEST(logTermOf, one, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND(3 /* term */);
    munit_assert_int(TERM_OF(1), ==, 3);
    return MUNIT_OK;
}

/* The log has two entries. */
TEST(logTermOf, two, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND_MANY(4 /* term */, 2 /* n */);
    munit_assert_int(TERM_OF(1), ==, 4);
    munit_assert_int(TERM_OF(2), ==, 4);
    return MUNIT_OK;
}

/* The log has a snapshot and hence has an offset. */
TEST(logTermOf, withSnapshot, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND_MANY(1 /* term */, 5 /* n entries */);
    SNAPSHOT(3 /* last index */, 0 /* trailing */);
    munit_assert_int(TERM_OF(1), ==, 0);
    munit_assert_int(TERM_OF(2), ==, 0);
    munit_assert_int(TERM_OF(3), ==, 1);
    munit_assert_int(TERM_OF(4), ==, 1);
    munit_assert_int(TERM_OF(5), ==, 1);
    return MUNIT_OK;
}

/* The log has a snapshot with trailing entries. */
TEST(logTermOf, snapshotTrailing, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND_MANY(1 /* term */, 5 /* n entries */);
    SNAPSHOT(3 /* last index */, 2 /* trailing */);
    munit_assert_int(TERM_OF(1), ==, 0);
    munit_assert_int(TERM_OF(2), ==, 1);
    munit_assert_int(TERM_OF(3), ==, 1);
    munit_assert_int(TERM_OF(4), ==, 1);
    munit_assert_int(TERM_OF(5), ==, 1);

    return MUNIT_OK;
}

/******************************************************************************
 *
 * logGet
 *
 *****************************************************************************/

SUITE(logGet)

/* The log is empty. */
TEST(logGet, empty_log, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    munit_assert_ptr_null(GET(1));
    return MUNIT_OK;
}

/* The log is empty but has an offset. */
TEST(logGet, emptyWithOffset, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND_MANY(4 /* term */, 10 /* n */);
    SNAPSHOT(10 /* last index */, 0 /* trailing */);
    munit_assert_ptr_null(GET(1));
    munit_assert_ptr_null(GET(10));
    munit_assert_ptr_null(GET(11));
    return MUNIT_OK;
}

/* The log has one entry. */
TEST(logGet, one, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND(3 /* term */);
    munit_assert_int(GET(1)->term, ==, 3);
    munit_assert_ptr_null(GET(2));
    return MUNIT_OK;
}

/* The log has two entries. */
TEST(logGet, two, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND_MANY(4 /* term */, 2 /* n */);
    munit_assert_int(GET(1)->term, ==, 4);
    munit_assert_int(GET(2)->term, ==, 4);
    munit_assert_ptr_null(GET(3));
    return MUNIT_OK;
}

/* The log starts at a certain offset. */
TEST(logGet, twoWithOffset, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND_MANY(1 /* term */, 3 /* n */);
    APPEND(2 /* term */);
    APPEND(3 /* term */);
    SNAPSHOT(4 /* las index */, 1 /* trailing */);
    munit_assert_ptr_null(GET(1));
    munit_assert_ptr_null(GET(2));
    munit_assert_ptr_null(GET(3));
    munit_assert_int(GET(4)->term, ==, 2);
    munit_assert_int(GET(5)->term, ==, 3);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * logAppend
 *
 *****************************************************************************/

SUITE(logAppend)

/* Append one entry to an empty log. */
TEST(logAppend, one, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND(1 /* term */);
    ASSERT(2 /* size                                                    */,
           0 /* front                                                   */,
           1 /* back                                                    */,
           0 /* offset                                                  */,
           1 /* n */);
    ASSERT_TERM_OF(1 /* entry index */, 1 /* term */);
    ASSERT_REFCOUNT(1 /* entry index */, 1 /* count */);
    return MUNIT_OK;
}

/* Append two entries to to an empty log. */
TEST(logAppend, two, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND(1 /* term */);
    APPEND(1 /* term */);
    ASSERT(6 /* size                                                    */,
           0 /* front                                                   */,
           2 /* back                                                    */,
           0 /* offset                                                  */,
           2 /* n */);
    ASSERT_TERM_OF(1 /* entry index */, 1 /* term */);
    ASSERT_TERM_OF(2 /* entry index */, 1 /* term */);
    ASSERT_REFCOUNT(1 /* entry index */, 1 /* count */);
    ASSERT_REFCOUNT(2 /* entry index */, 1 /* count */);
    return MUNIT_OK;
}

/* Append three entries in sequence. */
TEST(logAppend, three, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    /* One -> [e1, NULL] */
    APPEND(1 /* term */);

    /* Two -> [e1, e2, NULL, NULL, NULL, NULL] */
    APPEND(1 /* term */);

    /* Three -> [e1, e2, e3, NULL, NULL, NULL] */
    APPEND(1 /* term */);

    ASSERT(6 /* size                                                    */,
           0 /* front                                                   */,
           3 /* back                                                    */,
           0 /* offset                                                  */,
           3 /* n */);
    ASSERT_TERM_OF(1 /* entry index */, 1 /* term */);
    ASSERT_TERM_OF(2 /* entry index */, 1 /* term */);
    ASSERT_TERM_OF(3 /* entry index */, 1 /* term */);
    ASSERT_REFCOUNT(1 /* entry index */, 1 /* count */);
    ASSERT_REFCOUNT(2 /* entry index */, 1 /* count */);
    ASSERT_REFCOUNT(3 /* entry index */, 1 /* count */);

    return MUNIT_OK;
}

/* Append enough entries to force the reference count hash table to be
 * resized. */
TEST(logAppend, many, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    int i;
    for (i = 0; i < 3000; i++) {
        APPEND(1 /* term */);
    }
    munit_assert_int(f->log->refs_size, ==, 4096);
    return MUNIT_OK;
}

/* Append to wrapped log that needs to be grown. */
TEST(logAppend, wrap, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    APPEND_MANY(1 /* term */, 5 /* n */);

    /* Now the log is [e1, e2, e3, e4, e5, NULL] */
    ASSERT(6 /* size                                                    */,
           0 /* front                                                   */,
           5 /* back                                                    */,
           0 /* offset                                                  */,
           5 /* n */);

    /* Delete the first 4 entries. */
    SNAPSHOT(4 /* last entry */, 0 /* trailing */);

    /* Now the log is [NULL, NULL, NULL, NULL, e5, NULL] */
    ASSERT(6 /* size                                                    */,
           4 /* front                                                   */,
           5 /* back                                                    */,
           4 /* offset                                                  */,
           1 /* n */);

    /* Append another 3 entries. */
    APPEND_MANY(1 /* term */, 3 /* n */);

    /* Now the log is [e7, e8, NULL, NULL, e5, e6] */
    ASSERT(6 /* size                                                    */,
           4 /* front                                                   */,
           2 /* back                                                    */,
           4 /* offset                                                  */,
           4 /* n */);

    /* Append another 3 entries. */
    APPEND_MANY(1 /* term */, 3 /* n */);

    /* Now the log is [e5, ..., e11, NULL, ..., NULL] */
    ASSERT(14 /* size                                                 */,
           0 /* front                                                 */,
           7 /* back                                                  */,
           4 /* offset                                                */,
           7 /* n */);

    return MUNIT_OK;
}

/* Append a batch of entries to an empty log. */
TEST(logAppend, batch, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND_BATCH(3);
    ASSERT(6 /* size                                                 */,
           0 /* front                                                 */,
           3 /* back                                                  */,
           0 /* offset                                                */,
           3 /* n */);
    return MUNIT_OK;
}

static char *logAppendOomHeapFaultDelay[] = {"0", "1", NULL};
static char *logAppendOomHeapFaultRepeat[] = {"1", NULL};

static MunitParameterEnum logAppendOom[] = {
    {TEST_HEAP_FAULT_DELAY, logAppendOomHeapFaultDelay},
    {TEST_HEAP_FAULT_REPEAT, logAppendOomHeapFaultRepeat},
    {NULL, NULL},
};

/* Out of memory. */
TEST(logAppend, oom, setUp, tearDown, 0, logAppendOom)
{
    struct fixture *f = data;
    struct raft_buffer buf;
    int rv;
    buf.base = NULL;
    buf.len = 0;
    HeapFaultEnable(&f->heap);
    rv = logAppend(f->log, 1, RAFT_COMMAND, buf, true, NULL);
    munit_assert_int(rv, ==, RAFT_NOMEM);
    return MUNIT_OK;
}

/* Out of memory when trying to grow the refs count table. */
TEST(logAppend, oomRefs, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND_MANY(1, LOG__REFS_INITIAL_SIZE);
    HeapFaultConfig(&f->heap, 1, 1);
    HeapFaultEnable(&f->heap);
    APPEND_ERROR(1, RAFT_NOMEM);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * logAppendConfiguration
 *
 *****************************************************************************/

SUITE(logAppendConfiguration)

static char *logAppendConfigurationOomHeapFaultDelay[] = {"0", "1", NULL};
static char *logAppendConfigurationOomHeapFaultRepeat[] = {"1", NULL};

static MunitParameterEnum logAppendConfigurationOom[] = {
    {TEST_HEAP_FAULT_DELAY, logAppendConfigurationOomHeapFaultDelay},
    {TEST_HEAP_FAULT_REPEAT, logAppendConfigurationOomHeapFaultRepeat},
    {NULL, NULL},
};

/* Out of memory. */
TEST(logAppendConfiguration, oom, setUp, tearDown, 0, logAppendConfigurationOom)
{
    struct fixture *f = data;
    struct raft_configuration configuration;
    int rv;

    configurationInit(&configuration);
    rv = configurationAdd(&configuration, 1, "1", RAFT_VOTER);
    munit_assert_int(rv, ==, 0);

    HeapFaultEnable(&f->heap);

    rv = logAppendConfiguration(f->log, 1, &configuration);
    munit_assert_int(rv, ==, RAFT_NOMEM);

    configurationClose(&configuration);

    return MUNIT_OK;
}

/******************************************************************************
 *
 * logAcquire
 *
 *****************************************************************************/

SUITE(logAcquire)

/* Acquire a single log entry. */
TEST(logAcquire, one, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry *entries;
    unsigned n;
    APPEND(1 /* term */);
    ACQUIRE(1 /* index */);
    munit_assert_ptr_not_null(entries);
    munit_assert_int(n, ==, 1);
    munit_assert_int(entries[0].type, ==, RAFT_COMMAND);
    ASSERT_REFCOUNT(1 /* index */, 2 /* count */);
    RELEASE(1 /* index */);
    ASSERT_REFCOUNT(1 /* index */, 1 /* count */);
    return MUNIT_OK;
}

/* Acquire two log entries. */
TEST(logAcquire, two, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry *entries;
    unsigned n;
    APPEND(1 /* term */);
    APPEND(1 /* term */);
    ACQUIRE(1 /* index */);
    munit_assert_ptr_not_null(entries);
    munit_assert_int(n, ==, 2);
    munit_assert_int(entries[0].type, ==, RAFT_COMMAND);
    munit_assert_int(entries[1].type, ==, RAFT_COMMAND);
    ASSERT_REFCOUNT(1 /* index */, 2 /* count */);
    ASSERT_REFCOUNT(2 /* index */, 2 /* count */);
    RELEASE(1 /* index */);
    ASSERT_REFCOUNT(1 /* index */, 1 /* count */);
    ASSERT_REFCOUNT(2 /* index */, 1 /* count */);
    return MUNIT_OK;
}

/* Acquire two log entries in a wrapped log. */
TEST(logAcquire, wrap, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry *entries;
    unsigned n;

    APPEND_MANY(1 /* term */, 5 /* n */);

    /* Now the log is [e1, e2, e3, e4, e5, NULL] */
    ASSERT(6 /* size                                                 */,
           0 /* front                                                */,
           5 /* back                                                 */,
           0 /* offset                                               */,
           5 /* n */);

    /* Delete the first 4 entries. */
    SNAPSHOT(4 /* last index */, 0 /* trailing */);

    /* Now the log is [NULL, NULL, NULL, NULL, e5, NULL] */
    ASSERT(6 /* size                                                 */,
           4 /* front                                                */,
           5 /* back                                                 */,
           4 /* offset                                               */,
           1 /* n */);

    /* Append another 3 entries. */
    APPEND_MANY(1 /* term */, 3 /* n */);

    /* Now the log is [e7, e8, NULL, NULL, e5, e6] */
    ASSERT(6 /* size                                                 */,
           4 /* front                                                */,
           2 /* back                                                 */,
           4 /* offset                                               */,
           4 /* n */);

    ACQUIRE(6 /* index */);
    munit_assert_int(n, ==, 3);
    RELEASE(6 /* index */);

    return MUNIT_OK;
}

/* Acquire several entries some of which belong to batches. */
TEST(logAcquire, batch, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry *entries;
    unsigned n;

    APPEND(1 /* term */);
    APPEND_BATCH(2 /* n entries */);
    APPEND(1 /* term */);
    APPEND_BATCH(3 /* n entries */);

    ACQUIRE(2 /* index */);
    munit_assert_ptr_not_null(entries);
    munit_assert_int(n, ==, 6);
    ASSERT_REFCOUNT(2 /* index */, 2 /* count */);

    /* Truncate the last 5 entries, so the only references left for the second
     * batch are the ones in the acquired entries. */
    TRUNCATE(3 /* index */);

    RELEASE(2 /* index */);

    ASSERT_REFCOUNT(2 /* index */, 1 /* count */);

    return MUNIT_OK;
}

/* Trying to acquire entries out of range results in a NULL pointer. */
TEST(logAcquire, outOfRange, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry *entries;
    unsigned n;

    APPEND(1 /* term */);
    APPEND(1 /* term */);
    SNAPSHOT(1 /* index */, 0 /* trailing */);

    ACQUIRE(1 /* index */);
    munit_assert_ptr_null(entries);
    ACQUIRE(3 /* index */);
    munit_assert_ptr_null(entries);

    return MUNIT_OK;
}

/* Out of memory. */
TEST(logAcquire, oom, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry *entries;
    unsigned n;
    int rv;

    APPEND(1 /* term */);

    HeapFaultConfig(&f->heap, 0, 1);
    HeapFaultEnable(&f->heap);

    rv = logAcquire(f->log, 1, &entries, &n);
    munit_assert_int(rv, ==, RAFT_NOMEM);

    return MUNIT_OK;
}

/******************************************************************************
 *
 * logTruncate
 *
 *****************************************************************************/

SUITE(logTruncate)

/* Truncate the last entry of a log with a single entry. */
TEST(logTruncate, lastOfOne, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    APPEND(1 /* term */);
    TRUNCATE(1 /* index */);

    ASSERT(0 /* size                                                 */,
           0 /* front                                                */,
           0 /* back                                                 */,
           0 /* offset                                               */,
           0 /* n */);

    return MUNIT_OK;
}

/* Truncate the last entry of a log with a two entries. */
TEST(logTruncate, lastOfTwo, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    APPEND(1 /* term */);
    APPEND(1 /* term */);

    TRUNCATE(2 /* index */);

    ASSERT(6 /* size                                                 */,
           0 /* front                                                */,
           1 /* back                                                 */,
           0 /* offset                                               */,
           1 /* n */);
    ASSERT_TERM_OF(1 /* entry index */, 1 /* term */);

    return MUNIT_OK;
}

/* Truncate from an entry which makes the log wrap. */
TEST(logTruncate, wrap, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    APPEND_MANY(1 /* term */, 5 /* n entries */);

    /* Now the log is [e1, e2, e3, e4, e5, NULL] */
    ASSERT(6 /* size                                                 */,
           0 /* front                                                */,
           5 /* back                                                 */,
           0 /* offset                                               */,
           5 /* n */);

    /* Delete the first 4 entries. */
    SNAPSHOT(4 /* last index */, 0 /* trailing */);

    /* Now the log is [NULL, NULL, NULL, NULL, e5, NULL] */
    ASSERT(6 /* size                                                 */,
           4 /* front                                                */,
           5 /* back                                                 */,
           4 /* offset                                               */,
           1 /* n */);

    /* Append another 3 entries. */
    APPEND_MANY(1 /* term */, 3 /* n entries */);

    /* Now the log is [e7, e8, NULL, NULL, e5, e6] */
    ASSERT(6 /* size                                                 */,
           4 /* front                                                */,
           2 /* back                                                 */,
           4 /* offset                                               */,
           4 /* n */);

    /* Truncate from e6 onward (wrapping) */
    TRUNCATE(6 /* index */);

    /* Now the log is [NULL, NULL, NULL, NULL, e5, NULL] */
    ASSERT(6 /* size                                                 */,
           4 /* front                                                */,
           5 /* back                                                 */,
           4 /* offset                                               */,
           1 /* n */);

    return MUNIT_OK;
}

/* Truncate the last entry of a log with a single entry, which still has an
 * outstanding reference created by a call to logAcquire(). */
TEST(logTruncate, referenced, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry *entries;
    unsigned n;

    APPEND(1 /* term */);
    ACQUIRE(1 /* index */);
    TRUNCATE(1 /* index */);

    ASSERT(0 /* size                                                 */,
           0 /* front                                                */,
           0 /* back                                                 */,
           0 /* offset                                               */,
           0 /* n */);

    /* The entry has still an outstanding reference. */
    ASSERT_REFCOUNT(1 /* index */, 1 /* count */);

    munit_assert_string_equal((const char *)entries[0].buf.base, "hello");

    RELEASE(1 /* index */);
    ASSERT_REFCOUNT(1 /* index */, 0 /* count */);

    return MUNIT_OK;
}

/* Truncate all entries belonging to a batch. */
TEST(logTruncate, batch, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND_BATCH(3 /* n entries */);
    TRUNCATE(1 /* index */);
    munit_assert_int(f->log->size, ==, 0);
    return MUNIT_OK;
}

/* Acquire entries at a certain index. Truncate the log at that index. The
 * truncated entries are still referenced. Then append a new entry, which will
 * have the same index but different term. */
TEST(logTruncate, acquired, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry *entries;
    unsigned n;

    APPEND(1 /* term */);
    APPEND(1 /* term */);
    ACQUIRE(2 /* index */);
    munit_assert_int(n, ==, 1);

    TRUNCATE(2 /* index */);

    APPEND(2 /* term */);

    RELEASE(2 /*index */);

    return MUNIT_OK;
}

/* Acquire some entries, truncate the log and then append new ones forcing the
   log to be grown and the reference count hash table to be re-built. */
TEST(logTruncate, acquireAppend, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry *entries;
    unsigned n;
    size_t i;

    APPEND(1 /* term */);
    APPEND(1 /* term */);

    ACQUIRE(2);

    munit_assert_int(n, ==, 1);

    TRUNCATE(2);

    for (i = 0; i < LOG__REFS_INITIAL_SIZE; i++) {
        APPEND(2 /* term */);
    }

    RELEASE(2);

    return MUNIT_OK;
}

static char *logTruncateAcquiredHeapFaultDelay[] = {"0", NULL};
static char *logTruncateAcquiredFaultRepeat[] = {"1", NULL};

static MunitParameterEnum logTruncateAcquiredOom[] = {
    {TEST_HEAP_FAULT_DELAY, logTruncateAcquiredHeapFaultDelay},
    {TEST_HEAP_FAULT_REPEAT, logTruncateAcquiredFaultRepeat},
    {NULL, NULL},
};

/* Acquire entries at a certain index. Truncate the log at that index. The
 * truncated entries are still referenced. Then append a new entry, which fails
 * to be appended due to OOM. */
TEST(logTruncate, acquiredOom, setUp, tearDown, 0, logTruncateAcquiredOom)
{
    struct fixture *f = data;
    struct raft_entry *entries;
    unsigned n;
    struct raft_buffer buf;
    int rv;

    APPEND(1 /* term */);
    APPEND(1 /* term */);

    ACQUIRE(2);
    munit_assert_int(n, ==, 1);

    TRUNCATE(2);

    buf.base = NULL;
    buf.len = 0;

    HeapFaultEnable(&f->heap);

    rv = logAppend(f->log, 2, RAFT_COMMAND, buf, true, NULL);
    munit_assert_int(rv, ==, RAFT_NOMEM);

    RELEASE(2);

    return MUNIT_OK;
}

/******************************************************************************
 *
 * logSnapshot
 *
 *****************************************************************************/

SUITE(logSnapshot)

/* Take a snapshot at entry 3, keeping 2 trailing entries. */
TEST(logSnapshot, trailing, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    APPEND(1 /* term */);
    APPEND(2 /* term */);
    APPEND(2 /* term */);

    SNAPSHOT(3 /* last index */, 2 /* trailing */);

    ASSERT(6 /* size                                                 */,
           1 /* front                                                */,
           3 /* back                                                 */,
           1 /* offset                                               */,
           2 /* n */);

    ASSERT_SNAPSHOT(3 /* index */, 2 /* term */);

    munit_assert_int(NUM_ENTRIES, ==, 2);
    munit_assert_int(LAST_INDEX, ==, 3);

    return MUNIT_OK;
}

/* Take a snapshot when the number of outstanding entries is lower than the
 * desired trail (so no entry will be deleted). */
TEST(logSnapshot, trailingHigherThanNumEntries, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    /* Take a snapshot leaving just one entry in the log. */
    APPEND_MANY(1 /* term */, 3 /* n entries */);
    SNAPSHOT(3 /* last index */, 1 /* trailing */);

    /* Take another snapshot, trying to leave 3 entries, but only 2 are
     * available at all. */
    APPEND(2 /* term */);

    SNAPSHOT(4 /* last index */, 3 /* trailing */);

    ASSERT(6 /* size                                                 */,
           2 /* front                                                */,
           4 /* back                                                 */,
           2 /* offset                                               */,
           2 /* n */);

    ASSERT_SNAPSHOT(4 /* index */, 2 /* term */);

    munit_assert_int(NUM_ENTRIES, ==, 2);
    munit_assert_int(LAST_INDEX, ==, 4);

    return MUNIT_OK;
}

/* Take a snapshot when the number of outstanding entries is exactly equal to
 * the desired trail (so no entry will be deleted). */
TEST(logSnapshot, trailingMatchesOutstanding, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    /* Take a snapshot leaving just one entry in the log. */
    APPEND_MANY(1 /* term */, 3 /* n entries */);
    SNAPSHOT(3 /* last index */, 1 /* trailing */);

    /* Take another snapshot, leaving 2 entries, which are the ones we have. */
    APPEND(2 /* term */);

    SNAPSHOT(4 /* last index */, 2 /* trailing */);

    ASSERT(6 /* size                                                 */,
           2 /* front                                                */,
           4 /* back                                                 */,
           2 /* offset                                               */,
           2 /* n */);

    ASSERT_SNAPSHOT(4 /* index */, 2 /* term */);

    munit_assert_int(NUM_ENTRIES, ==, 2);
    munit_assert_int(LAST_INDEX, ==, 4);

    return MUNIT_OK;
}

/* Take a snapshot at an index which is not the last one. */
TEST(logSnapshot, lessThanHighestIndex, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    /* Take a snapshot leaving three entries in the log. */
    APPEND_MANY(1 /* term */, 5 /* n entries */);
    SNAPSHOT(4 /* last index */, 2 /* trailing */);

    ASSERT(6 /* size                                                 */,
           2 /* front                                                */,
           5 /* back                                                 */,
           2 /* offset                                               */,
           3 /* n */);

    ASSERT_SNAPSHOT(4 /* index */, 1 /* term */);

    munit_assert_int(NUM_ENTRIES, ==, 3);
    munit_assert_int(LAST_INDEX, ==, 5);

    return MUNIT_OK;
}

/* Take a snapshot at a point where the log needs to wrap. */
TEST(logSnapshot, wrap, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    APPEND_MANY(1 /* term */, 5 /* n entries */);

    /* Now the log is [e1, e2, e3, e4, e5, NULL] */
    ASSERT(6 /* size                                                 */,
           0 /* front                                                */,
           5 /* back                                                 */,
           0 /* offset                                               */,
           5 /* n */);

    /* Take a snapshot at e5, keeping just e5 itself. */
    SNAPSHOT(5 /* last index */, 1 /* trailing */);

    /* Now the log is [NULL, NULL, NULL, NULL, e5, NULL] */
    ASSERT(6 /* size                                                 */,
           4 /* front                                                */,
           5 /* back                                                 */,
           4 /* offset                                               */,
           1 /* n */);

    ASSERT_SNAPSHOT(5 /* index */, 1 /* term */);

    /* Append another 4 entries. */
    APPEND_MANY(1 /* term */, 4 /* n */);

    /* Now the log is [e7, e8, e9, NULL, e5, e6] */
    ASSERT(6 /* size                                                 */,
           4 /* front                                                */,
           3 /* back                                                 */,
           4 /* offset                                               */,
           5 /* n */);

    /* Take a snapshot at e8 keeping only e8 itself (wrapping) */
    SNAPSHOT(8 /* last index */, 1 /* trailing */);

    /* Now the log is [NULL, e8, e9, NULL, NULL, NULL] */
    ASSERT(6 /* size                                                 */,
           1 /* front                                                */,
           3 /* back                                                 */,
           7 /* offset                                               */,
           2 /* n */);

    ASSERT_SNAPSHOT(8 /* index */, 1 /* term */);

    return MUNIT_OK;
}

/******************************************************************************
 *
 * logRestore
 *
 *****************************************************************************/

SUITE(logRestore)

/* Mimic the initial restore of a snapshot after loading state from disk, when
 * there are no outstanding entries. */
TEST(logRestore, initial, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    RESTORE(2 /* last index */, 3 /* last term */);
    ASSERT_SNAPSHOT(2 /* index */, 3 /* term */);
    munit_assert_int(LAST_INDEX, ==, 2);
    return MUNIT_OK;
}

/* If there are existing entries they are wiped out. */
TEST(logRestore, wipe, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND_MANY(1 /* term */, 5 /* n entries */);
    RESTORE(2 /* last index */, 3 /* last term */);
    ASSERT_SNAPSHOT(2 /* index */, 3 /* term */);
    munit_assert_int(LAST_INDEX, ==, 2);
    return MUNIT_OK;
}
