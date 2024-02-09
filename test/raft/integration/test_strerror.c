#include "../../../src/raft.h"
#include "../lib/runner.h"

/******************************************************************************
 *
 * raft_strerror
 *
 *****************************************************************************/

SUITE(raft_strerror)

#define ERR_CODE_MAP(X)      \
    X(RAFT_NOMEM)            \
    X(RAFT_BADID)            \
    X(RAFT_DUPLICATEID)      \
    X(RAFT_DUPLICATEADDRESS) \
    X(RAFT_BADROLE)          \
    X(RAFT_MALFORMED)        \
    X(RAFT_NOTLEADER)        \
    X(RAFT_LEADERSHIPLOST)   \
    X(RAFT_SHUTDOWN)         \
    X(RAFT_CANTBOOTSTRAP)    \
    X(RAFT_CANTCHANGE)       \
    X(RAFT_CORRUPT)          \
    X(RAFT_CANCELED)         \
    X(RAFT_NAMETOOLONG)      \
    X(RAFT_TOOBIG)           \
    X(RAFT_NOCONNECTION)     \
    X(RAFT_BUSY)             \
    X(RAFT_IOERR)

#define TEST_CASE_STRERROR(CODE)                    \
    TEST(raft_strerror, CODE, NULL, NULL, 0, NULL)  \
    {                                               \
        (void)data;                                 \
        (void)params;                               \
        munit_assert_not_null(raft_strerror(CODE)); \
        return MUNIT_OK;                            \
    }

ERR_CODE_MAP(TEST_CASE_STRERROR)

TEST(raft_strerror, default, NULL, NULL, 0, NULL)
{
    (void)data;
    (void)params;
    munit_assert_string_equal(raft_strerror(666), "unknown error");
    return MUNIT_OK;
}
