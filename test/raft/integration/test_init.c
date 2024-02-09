#include "../../../src/raft.h"
#include "../lib/runner.h"

/******************************************************************************
 *
 * raft_init
 *
 *****************************************************************************/

SUITE(raft_init)

/* Incompatible raft->io and raft->fsm wrt async snapshots. */
TEST(raft_init, incompatIoFsmAsyncSnapshotNotNull, NULL, NULL, 0, NULL)
{
    /* Set incompatible io and fsm versions and non-NULL snapshot_async fn */
    struct raft r = {0};
    struct raft_io io = {0};
    struct raft_fsm fsm = {0};
    io.version = 1; /* Too low */
    io.async_work = (int (*)(struct raft_io *, struct raft_io_async_work *,
                             raft_io_async_work_cb))(uintptr_t)0xDEADBEEF;
    fsm.version = 3;
    fsm.snapshot_async = (int (*)(struct raft_fsm *, struct raft_buffer **,
                                  unsigned int *))(uintptr_t)0xDEADBEEF;

    int rc;
    rc = raft_init(&r, &io, &fsm, 1, "1");
    munit_assert_int(rc, ==, -1);
    munit_assert_string_equal(
        r.errmsg,
        "async snapshot requires io->version > 1 and async_work method.");
    return MUNIT_OK;
}

/* Incompatible raft->io and raft->fsm wrt async snapshots. */
TEST(raft_init, incompatIoFsmAsyncSnapshotNull, NULL, NULL, 0, NULL)
{
    /* Set incompatible io and fsm versions and NULL snapshot_async fn */
    struct raft r = {0};
    struct raft_io io = {0};
    struct raft_fsm fsm = {0};
    io.version = 2;
    io.async_work = NULL;
    fsm.version = 3;
    fsm.snapshot_async = (int (*)(struct raft_fsm *, struct raft_buffer **,
                                  unsigned int *))(uintptr_t)0xDEADBEEF;

    int rc;
    rc = raft_init(&r, &io, &fsm, 1, "1");
    munit_assert_int(rc, ==, -1);
    munit_assert_string_equal(
        r.errmsg,
        "async snapshot requires io->version > 1 and async_work method.");
    return MUNIT_OK;
}

TEST(raft_init, ioVersionNotSet, NULL, NULL, 0, NULL)
{
    struct raft r = {0};
    struct raft_io io = {0};
    struct raft_fsm fsm = {0};
    io.version = 0;
    fsm.version = 3;

    int rc;
    rc = raft_init(&r, &io, &fsm, 1, "1");
    munit_assert_int(rc, ==, -1);
    munit_assert_string_equal(r.errmsg, "io->version must be set");
    return MUNIT_OK;
}

TEST(raft_init, fsmVersionNotSet, NULL, NULL, 0, NULL)
{
    struct raft r = {0};
    struct raft_io io = {0};
    struct raft_fsm fsm = {0};
    io.version = 2;
    fsm.version = 0;

    int rc;
    rc = raft_init(&r, &io, &fsm, 1, "1");
    munit_assert_int(rc, ==, -1);
    munit_assert_string_equal(r.errmsg, "fsm->version must be set");
    return MUNIT_OK;
}
