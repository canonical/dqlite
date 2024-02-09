#include <errno.h>
#include <stdio.h>

#include "../../../src/raft/err.h"
#include "../lib/heap.h"
#include "../lib/runner.h"

/* An error messages which is 249 characters. */
#define LONG_ERRMSG                                                          \
    "boom boom boom boom boom boom boom boom boom boom boom boom boom boom " \
    "boom boom boom boom boom boom boom boom boom boom boom boom boom boom " \
    "boom boom boom boom boom boom boom boom boom boom boom boom boom boom " \
    "boom boom boom boom boom boom boom boom"

/******************************************************************************
 *
 * ErrMsgPrintf
 *
 *****************************************************************************/

SUITE(ErrMsgPrintf)

/* The format string has no parameters. */
TEST(ErrMsgPrintf, noParams, NULL, NULL, 0, NULL)
{
    char errmsg[RAFT_ERRMSG_BUF_SIZE];
    ErrMsgPrintf(errmsg, "boom");
    munit_assert_string_equal(errmsg, "boom");
    return MUNIT_OK;
}

/* The format string has parameters. */
TEST(ErrMsgPrintf, params, NULL, NULL, 0, NULL)
{
    char errmsg[RAFT_ERRMSG_BUF_SIZE];
    ErrMsgPrintf(errmsg, "boom %d", 123);
    munit_assert_string_equal(errmsg, "boom 123");
    return MUNIT_OK;
}

/******************************************************************************
 *
 * ErrMsgWrapf
 *
 *****************************************************************************/

SUITE(ErrMsgWrapf)

/* The wrapping format string has no parameters. */
TEST(ErrMsgWrapf, noParams, NULL, NULL, 0, NULL)
{
    char errmsg[RAFT_ERRMSG_BUF_SIZE];
    ErrMsgPrintf(errmsg, "boom");
    ErrMsgWrapf(errmsg, "no luck");
    munit_assert_string_equal(errmsg, "no luck: boom");
    return MUNIT_OK;
}

/* The wrapping format string has parameters. */
TEST(ErrMsgWrapf, params, NULL, NULL, 0, NULL)
{
    char errmsg[RAFT_ERRMSG_BUF_SIZE];
    ErrMsgPrintf(errmsg, "boom");
    ErrMsgWrapf(errmsg, "no luck, %s", "joe");
    munit_assert_string_equal(errmsg, "no luck, joe: boom");
    return MUNIT_OK;
}

/* The wrapped error message gets partially truncated. */
TEST(ErrMsgWrapf, partialTruncate, NULL, NULL, 0, NULL)
{
    char errmsg[RAFT_ERRMSG_BUF_SIZE];
    ErrMsgPrintf(errmsg, "no luck");
    ErrMsgWrapf(errmsg, LONG_ERRMSG);
    munit_assert_string_equal(errmsg, LONG_ERRMSG ": no l");
    return MUNIT_OK;
}

/* The wrapped error message gets entirely truncated. */
TEST(ErrMsgWrapf, fullTruncate, NULL, NULL, 0, NULL)
{
    char errmsg[RAFT_ERRMSG_BUF_SIZE];
    ErrMsgPrintf(errmsg, "no luck");
    ErrMsgWrapf(errmsg, LONG_ERRMSG " boom");
    munit_assert_string_equal(errmsg, LONG_ERRMSG " boom");
    return MUNIT_OK;
}
