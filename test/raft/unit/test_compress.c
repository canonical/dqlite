#include "src/raft/uv_fs.h"
#include "src/raft/uv_os.h"
#include "test/lib/munit.h"
#include "test/lib/runner.h"
#include "test/raft/lib/dir.h"

#include <fcntl.h>
#include <linux/limits.h>
#include <stdlib.h>
#include <sys/random.h>
#include <uv.h>

#ifdef LZ4_AVAILABLE
# include <lz4frame.h> /* for LZ4F_HEADER_SIZE_MAX */
#endif

SUITE(compress)

void *random_buffer(size_t len) {
    void *result = munit_malloc(len);
    size_t offset = 0;
    while (offset < len) {
        ssize_t r = getrandom((char *)result + offset, len - offset, 0);
        if (r < 0) {
            if (errno == EINTR) continue; // retry
            free(result);
            return NULL;
        }
        offset += r;
    }
    return result;
}


struct fixture {
    char *dir;
    char errmsg[RAFT_ERRMSG_BUF_SIZE];
};

static void *setUp(const MunitParameter params[], void *user_data)
{
	struct fixture *f = munit_malloc(sizeof *f);
	SET_UP_DIR;
	return f;
}

static void tearDown(void *data)
{
	struct fixture *f = data;
	TEAR_DOWN_DIR;
	free(f);
}

#ifdef LZ4_AVAILABLE

static int compare_bufs(const struct raft_buffer *original,
			int original_len,
			const struct raft_buffer *decompressed)
{
	size_t offset = 0;
	for (int i = 0; i < original_len; i++) {
		if (original[i].len > (decompressed->len - offset)) {
            assert(false);
			return -1;
		}
		int rv = memcmp(original[i].base, decompressed->base + offset,
				original[i].len);
		if (rv != 0) {
			return rv;
		}
		offset += original[i].len;
	}

	if (offset != decompressed->len) {
        assert(false);
		return -1;
	}
	return 0;
}

#define COMPRESS_IMPL(file, bufs, len, ...)                                  \
	do {                                                                 \
		int compress_rv = UvFsMakeCompressedFile(f->dir, file, bufs, \
							 len, f->errmsg);    \
		munit_assert_int(compress_rv, ==, RAFT_OK);                  \
		struct raft_buffer decompressed = {};                        \
		int decompress_rv = UvFsReadCompressedFile(                  \
		    f->dir, file, &decompressed, f->errmsg);                 \
		munit_assert_int(decompress_rv, ==, RAFT_OK);                \
		int compare_rv = compare_bufs(bufs, len, &decompressed);     \
		munit_assert_int(compare_rv, ==, RAFT_OK);                   \
		raft_free(decompressed.base);                                \
	} while (0)

#define COMPRESS(file, bufs, ...)                \
	COMPRESS_IMPL(file, bufs, ##__VA_ARGS__, \
		      (sizeof(bufs) / sizeof(bufs[0])))

TEST(compress, compressDecompressZeroLength, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_buffer empty = {};
    struct raft_buffer empty_invalid_ptr = {};
    struct raft_buffer many_empty[] = { empty, empty_invalid_ptr };

    COMPRESS("temp1", &empty, 1);
    COMPRESS("temp2", &empty_invalid_ptr, 1);
    COMPRESS("temp3", many_empty);

    return MUNIT_OK;
}

static char *len_one_params[] = {
	/*    16B   1KB     64KB     4MB        128MB */
	"16", "1024", "65536", "4194304", "134217728",
	/*    Around Blocksize*/
	"65516", "65517", "65518", "65521", "65535", "65537", "65551", "65555",
	"65556",
	/*    Ugly lengths */
	"0", "1", "9", "123450", "1337", "6655111", NULL
};

static MunitParameterEnum random_one_params[] = {
	{ "len_one", len_one_params },
	{ NULL, NULL },
};

TEST(compress,
     compressDecompressRandomOne,
     setUp,
     tearDown,
     0,
     random_one_params)
{
    struct fixture *f = data;

	/* Fill a buffer with random data */
	size_t len = strtoul(munit_parameters_get(params, "len_one"), NULL, 0);
	if (len == 0) {
		return MUNIT_SKIP;
	}
	struct raft_buffer buf = {
        .base = random_buffer(len),
        .len = len,
    };
    COMPRESS("temp", &buf, 1);
	free(buf.base);
	return MUNIT_OK;
}

static char *len_nonrandom_one_params[] = {
#if !defined(__LP64__) && \
    (defined(__arm__) || defined(__i386__) || defined(__mips__))
	/*    4KB     64KB     4MB        1GB           INT_MAX (larger
	   allocations fail on 32-bit archs */
	"4096",
	"65536", "4194304", "1073741824", "2147483647",
#else
	/*    4KB     64KB     4MB        1GB           2GB + 200MB */
	"4096", "65536", "4194304", "1073741824", "2357198848",
#endif
	/*    Around Blocksize*/
	"65516", "65517", "65518", "65521", "65535", "65537", "65551", "65555",
	"65556",
	/*    Ugly lengths */
	"0", "993450", "31337", "83883825", NULL
};

static MunitParameterEnum nonrandom_one_params[] = {
	{ "len_one", len_nonrandom_one_params },
	{ NULL, NULL },
};

TEST(compress,
     compressDecompressNonRandomOne,
     setUp,
     tearDown,
     0,
     nonrandom_one_params)
{
    struct fixture *f = data;

	size_t len = strtoul(munit_parameters_get(params, "len_one"), NULL, 0);
	if (len == 0) {
		return MUNIT_SKIP;
	}

	/* Fill a buffer with easy-to-compress data */
	struct raft_buffer buf = {
        .base = munit_malloc(len),
        .len = len,
    };
    memset(buf.base, 0xAC, buf.len);

    COMPRESS("test", &buf, 1);

    char path[PATH_MAX] = {};
	int rv = UvOsJoin(f->dir, "test", path);
    munit_assert_int(rv, ==, 0);

    uv_stat_t sb = {};
    rv = UvOsStat(path, &sb);
    munit_assert_int(rv, ==, 0);
    munit_assert_ulong(sb.st_size, >, 0);
    munit_assert_ulong(sb.st_size, <, len);

	free(buf.base);
	return MUNIT_OK;
}

static char *len_two_params[] = { "4194304", "13373", "66", "0", NULL };

static MunitParameterEnum random_two_params[] = {
	{ "len_one", len_one_params },
	{ "len_two", len_two_params },
	{ NULL, NULL },
};

TEST(compress,
     compressDecompressRandomTwo,
     setUp,
     tearDown,
     0,
     random_two_params)
{
    struct fixture *f = data;

	/* Fill two buffers with random data */
	size_t len1 = strtoul(munit_parameters_get(params, "len_one"), NULL, 0);
	size_t len2 = strtoul(munit_parameters_get(params, "len_two"), NULL, 0);
	if (len1 + len2 == 0) {
		return MUNIT_SKIP;
	}
	struct raft_buffer bufs[] = { {
					  .base = random_buffer(len1),
					  .len = len1,
				      },
				      {
					  .base = random_buffer(len2),
					  .len = len2,
				      } };

	COMPRESS("temp", bufs);

	free(bufs[0].base);
	free(bufs[1].base);
	return MUNIT_OK;
}

TEST(compress, compressDecompressCorruption, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

	/* Fill a buffer with random data */
	const size_t len = 2048;
	struct raft_buffer buf = {
        .base = random_buffer(len),
        .len = len,
    };

    int rv = UvFsMakeCompressedFile(f->dir, "temp", &buf, 1, f->errmsg);
    munit_assert_int(rv, ==, RAFT_OK);

	/* Corrupt the a data byte after the header */
    char path[PATH_MAX] = {};
	rv = UvOsJoin(f->dir, "temp", path);
    munit_assert_int(rv, ==, 0);
    int fd = open(path, O_RDWR);
    munit_assert_int(fd, !=, -1);
    char b;
    rv = pread(fd, &b, 1, LZ4F_HEADER_SIZE_MAX);
    munit_assert_int(rv, ==, 1);
    b++;
    rv = pwrite(fd, &b, 1, LZ4F_HEADER_SIZE_MAX);
    munit_assert_int(rv, ==, 1);
    close(fd);

    struct raft_buffer decompressed = {};
    rv = UvFsReadCompressedFile(f->dir, "temp", &decompressed, f->errmsg);
	munit_assert_int(rv, ==, RAFT_IOERR);
	munit_assert_string_equal(
	    f->errmsg, "LZ4F_decompress ERROR_contentChecksum_invalid");
	munit_assert_ptr_null(decompressed.base);

	free(buf.base);
	return MUNIT_OK;
}

#else

TEST(compress, lz4Disabled, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

	const size_t len = 2048;
	struct raft_buffer buf = {
        .base = random_buffer(len),
        .len = len,
    };

    int rv = UvFsMakeCompressedFile(f->dir, "temp", &buf, 1, f->errmsg);
	munit_assert_int(rv, ==, RAFT_INVALID);

	free(buf.base);
	return MUNIT_OK;
}

#endif /* LZ4_AVAILABLE */
