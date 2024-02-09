#include "fsm.h"

#include "../../../src/raft/byte.h"
#include "munit.h"

/* In-memory implementation of the raft_fsm interface. */
struct fsm
{
    int x;
    int y;
    int lock;
    void *data;
};

/* Command codes */
enum { SET_X = 1, SET_Y, ADD_X, ADD_Y };

static int fsmApply(struct raft_fsm *fsm,
                    const struct raft_buffer *buf,
                    void **result)
{
    struct fsm *f = fsm->data;
    const void *cursor = buf->base;
    unsigned command;
    int value;

    if (buf->len != 16) {
        return -1;
    }

    command = (unsigned)byteGet64(&cursor);
    value = (int)byteGet64(&cursor);

    switch (command) {
        case SET_X:
            f->x = value;
            break;
        case SET_Y:
            f->y = value;
            break;
        case ADD_X:
            f->x += value;
            break;
        case ADD_Y:
            f->y += value;
            break;
        default:
            return -1;
    }

    *result = NULL;

    return 0;
}

static int fsmRestore(struct raft_fsm *fsm, struct raft_buffer *buf)
{
    struct fsm *f = fsm->data;
    const void *cursor = buf->base;

    munit_assert_int(buf->len, ==, sizeof(uint64_t) * 2);

    f->x = byteGet64(&cursor);
    f->y = byteGet64(&cursor);

    raft_free(buf->base);

    return 0;
}

static int fsmEncodeSnapshot(int x,
                             int y,
                             struct raft_buffer *bufs[],
                             unsigned *n_bufs)
{
    struct raft_buffer *buf;
    void *cursor;

    *n_bufs = 1;

    *bufs = raft_malloc(sizeof **bufs);
    if (*bufs == NULL) {
        return RAFT_NOMEM;
    }

    buf = &(*bufs)[0];
    buf->len = sizeof(uint64_t) * 2;
    buf->base = raft_malloc(buf->len);
    if (buf->base == NULL) {
        return RAFT_NOMEM;
    }

    cursor = (*bufs)[0].base;

    bytePut64(&cursor, x);
    bytePut64(&cursor, y);

    return 0;
}

/* For use with fsm->version 1 */
static int fsmSnapshot_v1(struct raft_fsm *fsm,
                          struct raft_buffer *bufs[],
                          unsigned *n_bufs)
{
    struct fsm *f = fsm->data;
    return fsmEncodeSnapshot(f->x, f->y, bufs, n_bufs);
}

/* For use with fsmSnapshotFinalize and fsm->version >= 2 */
static int fsmSnapshot_v2(struct raft_fsm *fsm,
                          struct raft_buffer *bufs[],
                          unsigned *n_bufs)
{
    struct fsm *f = fsm->data;
    munit_assert_int(f->lock, ==, 0);
    f->lock = 1;
    f->data = raft_malloc(8); /* Detect proper cleanup in finalize */
    munit_assert_ptr_not_null(f->data);
    return fsmEncodeSnapshot(f->x, f->y, bufs, n_bufs);
}

static int fsmSnapshotInitialize(struct raft_fsm *fsm,
                                 struct raft_buffer *bufs[],
                                 unsigned *n_bufs)
{
    (void)bufs;
    (void)n_bufs;
    struct fsm *f = fsm->data;
    munit_assert_int(f->lock, ==, 0);
    f->lock = 1;
    munit_assert_ptr_null(f->data);
    f->data = raft_malloc(8); /* Detect proper cleanup in finalize */
    munit_assert_ptr_not_null(f->data);
    return 0;
}

static int fsmSnapshotAsync(struct raft_fsm *fsm,
                            struct raft_buffer *bufs[],
                            unsigned *n_bufs)
{
    struct fsm *f = fsm->data;
    return fsmEncodeSnapshot(f->x, f->y, bufs, n_bufs);
}

static int fsmSnapshotFinalize(struct raft_fsm *fsm,
                               struct raft_buffer *bufs[],
                               unsigned *n_bufs)
{
    (void)bufs;
    (void)n_bufs;
    struct fsm *f = fsm->data;
    if (*bufs != NULL) {
        for (unsigned i = 0; i < *n_bufs; ++i) {
            raft_free((*bufs)[i].base);
        }
        raft_free(*bufs);
    }
    *bufs = NULL;
    *n_bufs = 0;
    munit_assert_int(f->lock, ==, 1);
    f->lock = 0;
    munit_assert_ptr_not_null(f->data);
    raft_free(f->data);
    f->data = NULL;
    return 0;
}

void FsmInit(struct raft_fsm *fsm, int version)
{
    struct fsm *f = munit_malloc(sizeof *fsm);
    memset(fsm, 'x', sizeof(*fsm)); /* Fill  with garbage */

    f->x = 0;
    f->y = 0;
    f->lock = 0;
    f->data = NULL;

    fsm->version = version;
    fsm->data = f;
    fsm->apply = fsmApply;
    fsm->snapshot = fsmSnapshot_v1;
    fsm->restore = fsmRestore;
    if (version > 1) {
        fsm->snapshot = fsmSnapshot_v2;
        fsm->snapshot_finalize = fsmSnapshotFinalize;
        fsm->snapshot_async = NULL;
    }
}

void FsmInitAsync(struct raft_fsm *fsm, int version)
{
    munit_assert_int(version, >, 2);
    struct fsm *f = munit_malloc(sizeof *fsm);
    memset(fsm, 'x', sizeof(*fsm)); /* Fill  with garbage */

    f->x = 0;
    f->y = 0;
    f->lock = 0;
    f->data = NULL;

    fsm->version = version;
    fsm->data = f;
    fsm->apply = fsmApply;
    fsm->snapshot = fsmSnapshotInitialize;
    fsm->snapshot_async = fsmSnapshotAsync;
    fsm->snapshot_finalize = fsmSnapshotFinalize;
    fsm->restore = fsmRestore;
}

void FsmClose(struct raft_fsm *fsm)
{
    struct fsm *f = fsm->data;
    free(f);
}

void FsmEncodeSetX(const int value, struct raft_buffer *buf)
{
    void *cursor;

    buf->base = raft_malloc(16);
    buf->len = 16;

    munit_assert_ptr_not_null(buf->base);

    cursor = buf->base;
    bytePut64(&cursor, SET_X);
    bytePut64(&cursor, value);
}

void FsmEncodeAddX(const int value, struct raft_buffer *buf)
{
    void *cursor;

    buf->base = raft_malloc(16);
    buf->len = 16;

    munit_assert_ptr_not_null(buf->base);

    cursor = buf->base;
    bytePut64(&cursor, ADD_X);
    bytePut64(&cursor, value);
}

void FsmEncodeSetY(const int value, struct raft_buffer *buf)
{
    void *cursor;

    buf->base = raft_malloc(16);
    buf->len = 16;

    munit_assert_ptr_not_null(buf->base);

    cursor = buf->base;
    bytePut64(&cursor, SET_Y);
    bytePut64(&cursor, value);
}

void FsmEncodeAddY(const int value, struct raft_buffer *buf)
{
    void *cursor;

    buf->base = raft_malloc(16);
    buf->len = 16;

    munit_assert_ptr_not_null(buf->base);

    cursor = buf->base;
    bytePut64(&cursor, ADD_Y);
    bytePut64(&cursor, value);
}

void FsmEncodeSnapshot(int x,
                       int y,
                       struct raft_buffer *bufs[],
                       unsigned *n_bufs)
{
    int rc;
    rc = fsmEncodeSnapshot(x, y, bufs, n_bufs);
    munit_assert_int(rc, ==, 0);
}

int FsmGetX(struct raft_fsm *fsm)
{
    struct fsm *f = fsm->data;
    return f->x;
}

int FsmGetY(struct raft_fsm *fsm)
{
    struct fsm *f = fsm->data;
    return f->y;
}
