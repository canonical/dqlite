#include "../../../src/raft/flags.h"
#include "../lib/runner.h"

/******************************************************************************
 *
 * flags
 *
 *****************************************************************************/

SUITE(flags)

TEST(flags, empty, NULL, NULL, 0, NULL)
{
    raft_flags flags = 0;
    for (int i = 0; i < 64; i++) {
        munit_assert_false(flagsIsSet(flags, ((raft_flags)1) << i));
    }
    return MUNIT_OK;
}

TEST(flags, setClear, NULL, NULL, 0, NULL)
{
    raft_flags flags = 0;
    raft_flags flag = 0;
    for (int i = 0; i < 64; i++) {
        flag = ((raft_flags)1) << i;
        flags = flagsSet(flags, flag);
        munit_assert_true(flagsIsSet(flags, flag));
        flags = flagsClear(flags, flag);
        munit_assert_false(flagsIsSet(flags, flag));
        munit_assert_true(flags == 0);
    }
    return MUNIT_OK;
}

TEST(flags, setMultipleClearMultiple, NULL, NULL, 0, NULL)
{
    raft_flags in = 0;
    raft_flags out;
    raft_flags flags = (raft_flags)(1 | 1 << 4 | 1 << 13 | (raft_flags)1 << 40 |
                                    (raft_flags)1 << 63);
    out = flagsSet(in, flags);
    /* clang-format off */
    int positions[64] = {
        1, 0, 0, 0, 1, 0, 0, 0, // 0th and 4th
        0, 0, 0, 0, 0, 1, 0, 0, // 13th
        0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,
        1, 0, 0, 0, 0, 0, 0, 0, // 40th
        0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 1, // 63th
    };
    /* clang-format on */
    for (unsigned i = 0; i < 64; i++) {
        if (positions[i]) {
            munit_assert_true(flagsIsSet(out, (raft_flags)1 << i));
        } else {
            munit_assert_false(flagsIsSet(out, (raft_flags)1 << i));
        }
    }
    out = flagsClear(out, flags);
    munit_assert_true(out == 0);
    return MUNIT_OK;
}

TEST(flags, setMultipleClearSingle, NULL, NULL, 0, NULL)
{
    raft_flags in = 0;
    raft_flags out;
    raft_flags flags = (raft_flags)(1 << 3 | 1 << 5 | 1 << 18 |
                                    (raft_flags)1 << 32 | (raft_flags)1 << 35);
    out = flagsSet(in, flags);
    /* clang-format off */
    int positions[64] = {
        0, 0, 0, 1, 0, 1, 0, 0, // 3rd and 5th
        0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 1, 0, 0, 0, 0, 0, // 18th
        0, 0, 0, 0, 0, 0, 0, 0,
        1, 0, 0, 1, 0, 0, 0, 0, // 32rd 35th
        0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,
    };
    /* clang-format on */
    for (unsigned i = 0; i < 64; i++) {
        if (positions[i]) {
            munit_assert_true(flagsIsSet(out, (raft_flags)1 << i));
        } else {
            munit_assert_false(flagsIsSet(out, (raft_flags)1 << i));
        }
    }
    out = flagsClear(out, (raft_flags)1 << 32);
    munit_assert_true(
        out == (raft_flags)(1 << 3 | 1 << 5 | 1 << 18 | (raft_flags)1 << 35));
    return MUNIT_OK;
}
