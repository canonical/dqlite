#include "../../../src/raft/uv.h"
#include "../lib/runner.h"

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

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

/* Declare and fill the entries array for the append request identified by
 * I. The array will have N entries, and each entry will have a data buffer of
 * SIZE bytes.*/
#define ENTRIES(I, N, SIZE)                                 \
    struct raft_entry _entries##I[N];                       \
    uint8_t _entries_data##I[N * SIZE];                     \
    {                                                       \
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
            uint64_t _temporary = f->count;                 \
            memcpy(entry->buf.base, &_temporary, 8);        \
            f->count++;                                     \
        }                                                   \
    }

/* Submit an append request identified by I, with N_ENTRIES entries, each one of
 * size ENTRY_SIZE. When the append request completes, CB will be called
 * and DATA will be available in result->data. f->io.append is expected to
 * return RV. */
#define APPEND_SUBMIT_CB_DATA(I, N_ENTRIES, ENTRY_SIZE, CB, DATA, RV)    \
    struct raft_io_append _req##I;                                       \
    struct result _result##I = {0, false, DATA};                         \
    int _rv##I;                                                          \
    ENTRIES(I, N_ENTRIES, ENTRY_SIZE);                                   \
    _req##I.data = &_result##I;                                          \
    _rv##I = f->io.append(&f->io, &_req##I, _entries##I, N_ENTRIES, CB); \
    munit_assert_int(_rv##I, ==, RV)

/* Submit an append request identified by I, with N_ENTRIES entries, each one of
 * size ENTRY_SIZE. The default expectation is for the operation to succeed. A
 * custom STATUS can be set with APPEND_EXPECT. */
#define APPEND_SUBMIT(I, N_ENTRIES, ENTRY_SIZE)                           \
    APPEND_SUBMIT_CB_DATA(I, N_ENTRIES, ENTRY_SIZE, appendCbAssertResult, \
                          NULL, 0)

/* Try to submit an append request and assert that the given error code and
 * message are returned. */
#define APPEND_ERROR(N_ENTRIES, ENTRY_SIZE, RV, ERRMSG)                \
    do {                                                               \
        struct raft_io_append _req;                                    \
        int _rv;                                                       \
        ENTRIES(0, N_ENTRIES, ENTRY_SIZE);                             \
        _rv = f->io.append(&f->io, &_req, _entries0, N_ENTRIES, NULL); \
        munit_assert_int(_rv, ==, RV);                                 \
        /* munit_assert_string_equal(f->io.errmsg, ERRMSG);*/          \
    } while (0)

#define APPEND_EXPECT(I, STATUS) _result##I.status = STATUS

/* Wait for the append request identified by I to complete. */
#define APPEND_WAIT(I) LOOP_RUN_UNTIL(&_result##I.done)

/* Submit an append request with an entries array with N_ENTRIES entries, each
 * one of size ENTRY_SIZE, and wait for the operation to successfully
 * complete. */
#define APPEND(N_ENTRIES, ENTRY_SIZE)            \
    do {                                         \
        APPEND_SUBMIT(0, N_ENTRIES, ENTRY_SIZE); \
        APPEND_WAIT(0);                          \
    } while (0)

/* Submit an append request with the given parameters and wait for the operation
 * to fail with the given code and message. */
#define APPEND_FAILURE(N_ENTRIES, ENTRY_SIZE, STATUS, ERRMSG) \
    {                                                         \
        APPEND_SUBMIT(0, N_ENTRIES, ENTRY_SIZE);              \
        APPEND_EXPECT(0, STATUS);                             \
        APPEND_WAIT(0);                                       \
        f->count--;                                           \
        munit_assert_string_equal(f->io.errmsg, ERRMSG);      \
    }
