#include <string.h>

#include "../../../src/raft/byte.h"
#include "../../../src/raft/configuration.h"
#include "../../../src/raft/uv_encoding.h"
#include "../../lib/runner.h"

#define APPEND_ENTRIES_PREFIX_BYTES (4 * sizeof(uint64_t))
#define INSTALL_SNAPSHOT_MIN_HEADER_BYTES (6 * sizeof(uint64_t))

static int decodeMessage(uint16_t type, void *base, size_t len)
{
    uv_buf_t header;
    struct raft_message message;
    size_t payload_len = 0;

    header.base = base;
    header.len = len;

    return uvDecodeMessage(type, &header, &message, &payload_len);
}

SUITE(uvDecodeBatchHeaderLimits)

TEST(uvDecodeBatchHeaderLimits, shortPrefix, NULL, NULL, 0, NULL)
{
    uint8_t buf[sizeof(uint64_t) - 1] = {0};
    struct raft_entry *entries = NULL;
    unsigned n = 0;

    munit_assert_int(uvDecodeBatchHeader(buf, sizeof buf, &entries, &n), ==,
                     RAFT_MALFORMED);
    return MUNIT_OK;
}

TEST(uvDecodeBatchHeaderLimits, truncatedEntryHeader, NULL, NULL, 0, NULL)
{
    uint8_t buf[sizeof(uint64_t) + 15] = {0};
    void *cursor = buf;
    struct raft_entry *entries = NULL;
    unsigned n = 0;

    bytePut64(&cursor, 1);

    munit_assert_int(uvDecodeBatchHeader(buf, sizeof buf, &entries, &n), ==,
                     RAFT_MALFORMED);
    munit_assert_ptr_null(entries);
    return MUNIT_OK;
}

SUITE(uvDecodeMessageLimits)

TEST(uvDecodeMessageLimits, appendEntriesTooShort, NULL, NULL, 0, NULL)
{
    uint8_t buf[APPEND_ENTRIES_PREFIX_BYTES - 1] = {0};

    munit_assert_int(
        decodeMessage(RAFT_IO_APPEND_ENTRIES, buf, sizeof buf), ==,
        RAFT_MALFORMED);
    return MUNIT_OK;
}

TEST(uvDecodeMessageLimits,
     appendEntriesTruncatedBatchHeader,
     NULL,
     NULL,
     0,
     NULL)
{
    uint8_t buf[APPEND_ENTRIES_PREFIX_BYTES + sizeof(uint64_t) - 1] = {0};
    void *cursor = buf;

    bytePut64(&cursor, 1);
    bytePut64(&cursor, 2);
    bytePut64(&cursor, 3);
    bytePut64(&cursor, 4);

    munit_assert_int(
        decodeMessage(RAFT_IO_APPEND_ENTRIES, buf, sizeof buf), ==,
        RAFT_MALFORMED);
    return MUNIT_OK;
}

TEST(uvDecodeMessageLimits, installSnapshotTooShort, NULL, NULL, 0, NULL)
{
    uint8_t buf[INSTALL_SNAPSHOT_MIN_HEADER_BYTES - 1] = {0};

    munit_assert_int(
        decodeMessage(RAFT_IO_INSTALL_SNAPSHOT, buf, sizeof buf), ==,
        RAFT_MALFORMED);
    return MUNIT_OK;
}

TEST(uvDecodeMessageLimits, installSnapshotBadConfLen, NULL, NULL, 0, NULL)
{
    uint8_t buf[INSTALL_SNAPSHOT_MIN_HEADER_BYTES] = {0};
    void *cursor = buf;

    bytePut64(&cursor, 1); /* term */
    bytePut64(&cursor, 2); /* last_index */
    bytePut64(&cursor, 3); /* last_term */
    bytePut64(&cursor, 4); /* conf_index */
    bytePut64(&cursor, 1); /* conf.len */
    bytePut64(&cursor, 0); /* data.len */

    munit_assert_int(
        decodeMessage(RAFT_IO_INSTALL_SNAPSHOT, buf, sizeof buf), ==,
        RAFT_MALFORMED);
    return MUNIT_OK;
}

SUITE(configurationDecodeLimits)

TEST(configurationDecodeLimits, emptyBuffer, NULL, NULL, 0, NULL)
{
    struct raft_buffer buf;
    struct raft_configuration c;

    buf.base = NULL;
    buf.len = 0;

    munit_assert_int(configurationDecode(&buf, &c), ==, RAFT_MALFORMED);
    return MUNIT_OK;
}

TEST(configurationDecodeLimits, truncatedAfterVersion, NULL, NULL, 0, NULL)
{
    uint8_t raw[1] = {1};
    struct raft_buffer buf;
    struct raft_configuration c;

    buf.base = raw;
    buf.len = sizeof raw;

    munit_assert_int(configurationDecode(&buf, &c), ==, RAFT_MALFORMED);
    return MUNIT_OK;
}

TEST(configurationDecodeLimits, truncatedRole, NULL, NULL, 0, NULL)
{
    uint8_t raw[1 + sizeof(uint64_t) + sizeof(uint64_t) + 2] = {0};
    void *cursor = raw;
    struct raft_buffer buf;
    struct raft_configuration c;

    bytePut8(&cursor, 1); /* ENCODING_FORMAT */
    bytePut64(&cursor, 1); /* n_servers */
    bytePut64(&cursor, 123); /* server id */
    bytePutString(&cursor, "a");

    buf.base = raw;
    buf.len = sizeof raw;

    munit_assert_int(configurationDecode(&buf, &c), ==, RAFT_MALFORMED);
    return MUNIT_OK;
}
