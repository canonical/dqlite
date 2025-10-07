#include "src/raft/byte.h"
#include "src/raft/uv_fs.h"
#include "src/raft/uv_os.h"
#include "test/lib/munit.h"
#include "test/lib/runner.h"
#include "test/raft/lib/dir.h"

#include <fcntl.h>
#include <linux/limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/random.h>
#include <uv.h>

#ifdef LZ4_AVAILABLE
#include <lz4frame.h> /* for LZ4F_HEADER_SIZE_MAX */
#endif

SUITE(compress)

void *random_buffer(size_t len)
{
	void *result = munit_malloc(len);
	size_t offset = 0;
	while (offset < len) {
		ssize_t r = getrandom((char *)result + offset, len - offset, 0);
		if (r < 0) {
			if (errno == EINTR)
				continue;  // retry
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

static void sha1(const uint8_t *buffer, size_t size, uint8_t value[20])
{
    struct byteSha1 sha;
    byteSha1Init(&sha);
	byteSha1Update(&sha, buffer, (uint32_t)size);
    byteSha1Digest(&sha, value);
}

/* Split a buffer into n chunks */
static struct raft_buffer* chunk_buffer(void* buf, size_t size, size_t n) {
	struct raft_buffer* bufs = munit_malloc(n * sizeof *bufs);
	const size_t chunk_size = (size + n - 1) / n;
	for (size_t i = 0; i < n; i++) {
		bufs[i].base = buf + i * chunk_size;
		bufs[i].len = MIN(chunk_size, size - i * chunk_size);
	}
	return bufs;
}

#define TEST_COMPRESS2(file, buf, size, n)                                    \
	do {                                                                  \
		uint8_t sha_uncompressed[20];                                 \
		sha1(buf, size, sha_uncompressed);                            \
		struct raft_buffer *bufs = chunk_buffer(buf, size, n);        \
		int compress_rv =                                             \
		    UvFsMakeCompressedFile(f->dir, file, bufs, n, f->errmsg); \
		munit_assert_int(compress_rv, ==, RAFT_OK);                   \
		free(bufs);                                                   \
		free(buf);                                                    \
		struct raft_buffer decompressed = {};                         \
		int decompress_rv = UvFsReadCompressedFile(                   \
		    f->dir, file, &decompressed, f->errmsg);                  \
		munit_assert_int(decompress_rv, ==, RAFT_OK);                 \
		uint8_t sha_decompressed[20];                                 \
		sha1(decompressed.base, decompressed.len, sha_decompressed);  \
		munit_assert_memory_equal(20, sha_uncompressed,               \
					  sha_decompressed);                  \
		free(decompressed.base);                                      \
	} while (0)

TEST(compress, compressDecompressZeroLength, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;

	TEST_COMPRESS2("temp1", NULL, 0, 1);
	TEST_COMPRESS2("temp2", NULL, 0, 2);

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
	{ "len", len_one_params },
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
	size_t len = strtoul(munit_parameters_get(params, "len"), NULL, 0);
	if (len == 0) {
		return MUNIT_SKIP;
	}
	void *buf = random_buffer(len);

	/* Split the buffer into many chunks of at most 4096 bytes */
	size_t n_bufs = (len + 4095) / 4096;

	TEST_COMPRESS2("temp", buf, len, n_bufs);

	return MUNIT_OK;
}



static char *len_nonrandom_one_params[] = {
#if !defined(__LP64__) && \
    (defined(__arm__) || defined(__i386__) || defined(__mips__))
	/*    4KB     64KB     4MB        1GB           INT_MAX (larger
	   allocations fail on 32-bit archs */
	"4096", "65536", "4194304", "1073741824", "2147483647",
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
	{ "len", len_nonrandom_one_params },
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

	size_t len = strtoul(munit_parameters_get(params, "len"), NULL, 0);
	if (len == 0) {
		return MUNIT_SKIP;
	}

	/* Fill a buffer with easy-to-compress data */
	void *buf = munit_malloc(len);
	memset(buf, 0xAC, len);

	/* Split the buffer into many chunks of at most 4096 bytes */
	size_t n_bufs = (len + 4095) / 4096;

	TEST_COMPRESS2("test", buf, len, n_bufs); 

	char path[PATH_MAX] = {};
	int rv = UvOsJoin(f->dir, "test", path);
	munit_assert_int(rv, ==, 0);

	uv_stat_t sb = {};
	rv = UvOsStat(path, &sb);
	munit_assert_int(rv, ==, 0);
	munit_assert_ulong(sb.st_size, >, 0);
	munit_assert_ulong(sb.st_size, <, len);

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
	if (len1 + len2 == 0 || len1 > 4 * 1024 * 1024) {
		return MUNIT_SKIP;
	}
	void *buf = random_buffer(len1 + len2);
	uint8_t sha_uncompressed[20];
	sha1(buf, len1 + len2, sha_uncompressed);

	struct raft_buffer bufs[] = { {
					  .base = buf,
					  .len = len1,
				      },
				      {
					  .base = buf + len1,
					  .len = len2,
				      } };

	int compress_rv =
	    UvFsMakeCompressedFile(f->dir, "test", bufs, 2, f->errmsg);
	munit_assert_int(compress_rv, ==, RAFT_OK);
	free(buf);

	struct raft_buffer decompressed = {};
	int decompress_rv =
	    UvFsReadCompressedFile(f->dir, "test", &decompressed, f->errmsg);
	munit_assert_int(decompress_rv, ==, RAFT_OK);
	uint8_t sha_decompressed[20];
	sha1(decompressed.base, decompressed.len, sha_decompressed);
	munit_assert_memory_equal(20, sha_uncompressed, sha_decompressed);
	free(decompressed.base);

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
