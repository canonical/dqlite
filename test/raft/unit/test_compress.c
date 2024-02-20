#include "../../../src/raft/byte.h"
#include "../../../src/raft/compress.h"
#include "../lib/munit.h"
#include "../lib/runner.h"

#include <sys/random.h>
#ifdef LZ4_AVAILABLE
#include <lz4frame.h>
#endif

SUITE(Compress)

struct raft_buffer getBufWithRandom(size_t len)
{
    struct raft_buffer buf = {0};
    buf.len = len;
    buf.base = munit_malloc(buf.len);
    if (len != 0) {
        munit_assert_ptr_not_null(buf.base);
    }

    size_t offset = 0;
    /* Write as many random ints in buf as possible */
    for (size_t n = buf.len / sizeof(int); n > 0; n--) {
        *((int *)(buf.base) + offset) = rand();
        offset += 1;
    }

    /* Fill the remaining bytes */
    size_t rem = buf.len % sizeof(int);
    /* Offset will now be used in char* arithmetic */
    offset *= sizeof(int);
    if (rem) {
        int r_int = rand();
        for (unsigned i = 0; i < rem; i++) {
            *((char *)buf.base + offset) = *((char *)&r_int + i);
            offset++;
        }
    }

    munit_assert_ulong(offset, ==, buf.len);
    return buf;
}

struct raft_buffer getBufWithNonRandom(size_t len)
{
    struct raft_buffer buf = {0};
    buf.len = len;
    buf.base = munit_malloc(buf.len);
    if (len != 0) {
        munit_assert_ptr_not_null(buf.base);
    }

    memset(buf.base, 0xAC, buf.len);
    return buf;
}

#ifdef LZ4_AVAILABLE

static void sha1(struct raft_buffer bufs[], unsigned n_bufs, uint8_t value[20])
{
    struct byteSha1 sha;
    byteSha1Init(&sha);
    for (unsigned i = 0; i < n_bufs; i++) {
        byteSha1Update(&sha, (const uint8_t *)bufs[i].base,
                       (uint32_t)bufs[i].len);
    }
    byteSha1Digest(&sha, value);
}

TEST(Compress, compressDecompressZeroLength, NULL, NULL, 0, NULL)
{
    char errmsg[RAFT_ERRMSG_BUF_SIZE] = {0};
    struct raft_buffer bufs1[2] = {{NULL, 0},
                                   {(void *)0xDEADBEEF, 0}}; /* 0 length */
    struct raft_buffer bufs2[2] = {{(void *)0xDEADBEEF, 0},
                                   {NULL, 0}}; /* 0 length */
    struct raft_buffer compressed = {0};
    munit_assert_int(Compress(&bufs1[0], 1, &compressed, errmsg), ==,
                     RAFT_INVALID);
    munit_assert_int(Compress(&bufs1[1], 1, &compressed, errmsg), ==,
                     RAFT_INVALID);
    munit_assert_int(Compress(bufs1, 2, &compressed, errmsg), ==, RAFT_INVALID);
    munit_assert_int(Compress(bufs2, 2, &compressed, errmsg), ==, RAFT_INVALID);
    return MUNIT_OK;
}

static char *len_one_params[] = {
    /*    16B   1KB     64KB     4MB        128MB */
    "16", "1024", "65536", "4194304", "134217728",
    /*    Around Blocksize*/
    "65516", "65517", "65518", "65521", "65535", "65537", "65551", "65555",
    "65556",
    /*    Ugly lengths */
    "0", "1", "9", "123450", "1337", "6655111", NULL};

static MunitParameterEnum random_one_params[] = {
    {"len_one", len_one_params},
    {NULL, NULL},
};

TEST(Compress, compressDecompressRandomOne, NULL, NULL, 0, random_one_params)
{
    char errmsg[RAFT_ERRMSG_BUF_SIZE] = {0};
    struct raft_buffer compressed = {0};
    struct raft_buffer decompressed = {0};
    uint8_t sha1_virgin[20] = {0};
    uint8_t sha1_decompressed[20] = {1};

    /* Fill a buffer with random data */
    size_t len = strtoul(munit_parameters_get(params, "len_one"), NULL, 0);
    if (len == 0) {
        return MUNIT_SKIP;
    }
    struct raft_buffer buf = getBufWithRandom(len);

    /* Assert that after compression and decompression the data is unchanged */
    sha1(&buf, 1, sha1_virgin);
    munit_assert_int(Compress(&buf, 1, &compressed, errmsg), ==, 0);
    free(buf.base);
    munit_assert_true(IsCompressed(compressed.base, compressed.len));
    munit_assert_int(Decompress(compressed, &decompressed, errmsg), ==, 0);
    munit_assert_ulong(decompressed.len, ==, len);
    sha1(&decompressed, 1, sha1_decompressed);
    munit_assert_int(memcmp(sha1_virgin, sha1_decompressed, 20), ==, 0);

    raft_free(compressed.base);
    raft_free(decompressed.base);
    return MUNIT_OK;
}

static char *len_nonrandom_one_params[] = {
#if !defined(__LP64__) && \
    (defined(__arm__) || defined(__i386__) || defined(__mips__))
    /*    4KB     64KB     4MB        1GB           INT_MAX (larger allocations
       fail on 32-bit archs */
    "4096", "65536", "4194304", "1073741824", "2147483647",
#else
    /*    4KB     64KB     4MB        1GB           2GB + 200MB */
    "4096", "65536", "4194304", "1073741824", "2357198848",
#endif
    /*    Around Blocksize*/
    "65516", "65517", "65518", "65521", "65535", "65537", "65551", "65555",
    "65556",
    /*    Ugly lengths */
    "0", "993450", "31337", "83883825", NULL};

static MunitParameterEnum nonrandom_one_params[] = {
    {"len_one", len_nonrandom_one_params},
    {NULL, NULL},
};

TEST(Compress,
     compressDecompressNonRandomOne,
     NULL,
     NULL,
     0,
     nonrandom_one_params)
{
    char errmsg[RAFT_ERRMSG_BUF_SIZE] = {0};
    struct raft_buffer compressed = {0};
    struct raft_buffer decompressed = {0};
    uint8_t sha1_virgin[20] = {0};
    uint8_t sha1_decompressed[20] = {1};

    /* Fill a buffer with non-random data */
    size_t len = strtoul(munit_parameters_get(params, "len_one"), NULL, 0);
    if (len == 0) {
        return MUNIT_SKIP;
    }
    struct raft_buffer buf = getBufWithNonRandom(len);

    /* Assert that after compression and decompression the data is unchanged and
     * that the compressed data is actually smaller */
    sha1(&buf, 1, sha1_virgin);
    munit_assert_int(Compress(&buf, 1, &compressed, errmsg), ==, 0);
    free(buf.base);
    munit_assert_true(IsCompressed(compressed.base, compressed.len));
    if (len > 0) {
        munit_assert_ulong(compressed.len, <, buf.len);
    }
    munit_assert_int(Decompress(compressed, &decompressed, errmsg), ==, 0);
    munit_assert_ulong(decompressed.len, ==, len);
    sha1(&decompressed, 1, sha1_decompressed);
    munit_assert_int(memcmp(sha1_virgin, sha1_decompressed, 20), ==, 0);

    raft_free(compressed.base);
    raft_free(decompressed.base);
    return MUNIT_OK;
}

static char *len_two_params[] = {"4194304", "13373", "66", "0", NULL};

static MunitParameterEnum random_two_params[] = {
    {"len_one", len_one_params},
    {"len_two", len_two_params},
    {NULL, NULL},
};

TEST(Compress, compressDecompressRandomTwo, NULL, NULL, 0, random_two_params)
{
    char errmsg[RAFT_ERRMSG_BUF_SIZE] = {0};
    struct raft_buffer compressed = {0};
    struct raft_buffer decompressed = {0};
    uint8_t sha1_virgin[20] = {0};
    uint8_t sha1_single[20] = {0};
    uint8_t sha1_decompressed[20] = {1};

    /* Fill two buffers with random data */
    size_t len1 = strtoul(munit_parameters_get(params, "len_one"), NULL, 0);
    size_t len2 = strtoul(munit_parameters_get(params, "len_two"), NULL, 0);
    if (len1 + len2 == 0) {
        return MUNIT_SKIP;
    }
    struct raft_buffer buf1 = getBufWithRandom(len1);
    struct raft_buffer buf2 = getBufWithRandom(len2);
    struct raft_buffer bufs[2] = {buf1, buf2};

    /* If one of the buffers is empty ensure data is identical to single buffer
     * case. */
    if (len1 == 0) {
        sha1(&buf2, 1, sha1_single);
    } else if (len2 == 0) {
        sha1(&buf1, 1, sha1_single);
    }

    /* Assert that after compression and decompression the data is unchanged */
    sha1(bufs, 2, sha1_virgin);
    munit_assert_int(Compress(bufs, 2, &compressed, errmsg), ==, 0);
    free(buf1.base);
    free(buf2.base);
    munit_assert_true(IsCompressed(compressed.base, compressed.len));
    munit_assert_int(Decompress(compressed, &decompressed, errmsg), ==, 0);
    munit_assert_ulong(decompressed.len, ==, buf1.len + buf2.len);
    sha1(&decompressed, 1, sha1_decompressed);
    munit_assert_int(memcmp(sha1_virgin, sha1_decompressed, 20), ==, 0);

    if (len1 == 0 || len2 == 0) {
        munit_assert_int(memcmp(sha1_single, sha1_virgin, 20), ==, 0);
        munit_assert_int(memcmp(sha1_single, sha1_decompressed, 20), ==, 0);
    }

    raft_free(compressed.base);
    raft_free(decompressed.base);
    return MUNIT_OK;
}

TEST(Compress, compressDecompressCorruption, NULL, NULL, 0, NULL)
{
    char errmsg[RAFT_ERRMSG_BUF_SIZE] = {0};
    struct raft_buffer compressed = {0};
    struct raft_buffer decompressed = {0};

    /* Fill a buffer with random data */
    size_t len = 2048;
    struct raft_buffer buf = getBufWithRandom(len);

    munit_assert_int(Compress(&buf, 1, &compressed, errmsg), ==, 0);
    munit_assert_true(IsCompressed(compressed.base, compressed.len));

    /* Corrupt the a data byte after the header */
    munit_assert_ulong(LZ4F_HEADER_SIZE_MAX_RAFT, <, compressed.len);
    ((char *)compressed.base)[LZ4F_HEADER_SIZE_MAX_RAFT] += 1;

    munit_assert_int(Decompress(compressed, &decompressed, errmsg), !=, 0);
    munit_assert_string_equal(errmsg,
                              "LZ4F_decompress ERROR_contentChecksum_invalid");
    munit_assert_ptr_null(decompressed.base);

    raft_free(compressed.base);
    free(buf.base);
    return MUNIT_OK;
}

#else

TEST(Compress, lz4Disabled, NULL, NULL, 0, NULL)
{
    char errmsg[RAFT_ERRMSG_BUF_SIZE] = {0};
    struct raft_buffer compressed = {0};

    /* Fill a buffer with random data */
    size_t len = 2048;
    struct raft_buffer buf = getBufWithRandom(len);

    munit_assert_int(Compress(&buf, 1, &compressed, errmsg), ==, RAFT_INVALID);
    munit_assert_ptr_null(compressed.base);

    free(buf.base);
    return MUNIT_OK;
}

#endif /* LZ4_AVAILABLE */

static const char LZ4_MAGIC[4] = {0x04, 0x22, 0x4d, 0x18};
TEST(Compress, isCompressedTooSmall, NULL, NULL, 0, NULL)
{
    munit_assert_false(IsCompressed(&LZ4_MAGIC[1], sizeof(LZ4_MAGIC) - 1));
    return MUNIT_OK;
}

TEST(Compress, isCompressedNull, NULL, NULL, 0, NULL)
{
    munit_assert_false(IsCompressed(NULL, sizeof(LZ4_MAGIC)));
    return MUNIT_OK;
}

TEST(Compress, isCompressed, NULL, NULL, 0, NULL)
{
    munit_assert_true(IsCompressed(LZ4_MAGIC, sizeof(LZ4_MAGIC)));
    return MUNIT_OK;
}

TEST(Compress, notCompressed, NULL, NULL, 0, NULL)
{
    char not_compressed[4] = {0x18, 0x4d, 0x22, 0x04};
    munit_assert_false(IsCompressed(not_compressed, sizeof(not_compressed)));
    return MUNIT_OK;
}
