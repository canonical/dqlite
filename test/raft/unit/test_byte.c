#include <ctype.h>
#include <stdio.h>

#include "../../../src/raft/byte.h"
#include "../lib/runner.h"

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

#define CRC32(VALUE) byteCrc32(&(VALUE), sizeof VALUE, 0)

/******************************************************************************
 *
 * byteCrc32
 *
 *****************************************************************************/

SUITE(byteCrc32)

/* The same data produces the same sum. */
TEST(byteCrc32, valid, NULL, NULL, 0, NULL)
{
    uint64_t value1 = 123456789;
    uint64_t value2 = 123456789;
    munit_assert_int(CRC32(value1), ==, CRC32(value2));
    return MUNIT_OK;
}

/* Different data produces a different sum. */
TEST(byteCrc32, invalid, NULL, NULL, 0, NULL)
{
    uint64_t value1 = 123456789;
    uint64_t value2 = 123466789;
    munit_assert_int(CRC32(value1), !=, CRC32(value2));
    return MUNIT_OK;
}

/******************************************************************************
 *
 * Convert to little endian representation (least significant byte first).
 *
 *****************************************************************************/

SUITE(byteFlip)

/* Convert a 32-bit number. */
TEST(byteFlip, 32, NULL, NULL, 0, NULL)
{
    uint32_t value;
    unsigned i;
    value = byteFlip32(0x03020100);
    for (i = 0; i < 4; i++) {
        munit_assert_int(*((uint8_t *)&value + i), ==, i);
    }
    return MUNIT_OK;
}

/* Convert a 64-bit number. */
TEST(byteFlip, 64, NULL, NULL, 0, NULL)
{
    uint64_t value;
    unsigned i;
    value = byteFlip64(0x0706050403020100);
    for (i = 0; i < 8; i++) {
        munit_assert_int(*((uint8_t *)&value + i), ==, i);
    }
    return MUNIT_OK;
}

/******************************************************************************
 *
 * byteGetString
 *
 *****************************************************************************/

SUITE(byteGetString)

TEST(byteGetString, success, NULL, NULL, 0, NULL)
{
    uint8_t buf[] = {'h', 'e', 'l', 'l', 'o', 0};
    const void *cursor = buf;
    munit_assert_string_equal(byteGetString(&cursor, sizeof buf), "hello");
    munit_assert_ptr_equal(cursor, buf + sizeof buf);
    return MUNIT_OK;
}

TEST(byteGetString, malformed, NULL, NULL, 0, NULL)
{
    uint8_t buf[] = {'h', 'e', 'l', 'l', 'o', 'w'};
    const void *cursor = buf;
    munit_assert_ptr_equal(byteGetString(&cursor, sizeof buf), NULL);
    munit_assert_ptr_equal(cursor, buf);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * byteGet64
 *
 *****************************************************************************/

SUITE(byteGet64)

TEST(byteGet64, success, NULL, NULL, 0, NULL)
{
    uint8_t *buf = munit_malloc(sizeof(uint64_t) * 2);
    void *cursor1 = buf + 1;
    const void *cursor2 = buf + 1;
    bytePut64(&cursor1, 1);
    munit_assert_int(byteGet64(&cursor2), ==, 1);
    free(buf);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * byteSha1
 *
 *****************************************************************************/

/* Assert that the 20 bytes contained in VALUE match the given DIGEST
 * hexadecimal representation. */
#define ASSERT_SHA1(VALUE, DIGEST)                      \
    do {                                                \
        char _digest[41];                               \
        unsigned _i;                                    \
        for (_i = 0; _i < 20; _i++) {                   \
            unsigned _j = _i * 2;                       \
            sprintf(&_digest[_j], "%.2x", value[_i]);   \
            _digest[_j] = toupper(_digest[_j]);         \
            _digest[_j + 1] = toupper(_digest[_j + 1]); \
        }                                               \
        _digest[40] = '\0';                             \
        munit_assert_string_equal(_digest, DIGEST);     \
    } while (0)

SUITE(byteSha1)

TEST(byteSha1, abc, NULL, NULL, 0, NULL)
{
    struct byteSha1 sha1;
    uint8_t text[] = "abc";
    uint8_t value[20];
    byteSha1Init(&sha1);
    byteSha1Update(&sha1, text, sizeof text - 1);
    byteSha1Digest(&sha1, value);
    ASSERT_SHA1(value, "A9993E364706816ABA3E25717850C26C9CD0D89D");
    return MUNIT_OK;
}

TEST(byteSha1, abcWithZeroLen, NULL, NULL, 0, NULL)
{
    struct byteSha1 sha1;
    uint8_t text[] = "abc";
    uint8_t garbage[] = "garbage";
    uint8_t value[20];
    byteSha1Init(&sha1);
    byteSha1Update(&sha1, text, sizeof text - 1);
    /* Update with 0 length buffer doesn't change digest */
    byteSha1Update(&sha1, garbage, 0);
    byteSha1Digest(&sha1, value);
    ASSERT_SHA1(value, "A9993E364706816ABA3E25717850C26C9CD0D89D");
    return MUNIT_OK;
}

TEST(byteSha1, abcbd, NULL, NULL, 0, NULL)
{
    struct byteSha1 sha1;
    uint8_t text[] = "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq";
    uint8_t value[20];
    byteSha1Init(&sha1);
    byteSha1Update(&sha1, text, sizeof text - 1);
    byteSha1Digest(&sha1, value);
    ASSERT_SHA1(value, "84983E441C3BD26EBAAE4AA1F95129E5E54670F1");
    return MUNIT_OK;
}
