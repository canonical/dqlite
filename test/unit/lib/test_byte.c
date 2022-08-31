#include "../../../src/lib/byte.h"

#include "../../lib/runner.h"

TEST_MODULE(lib_addr);
TEST_SUITE(endian);

static uint16_t vfsFlip16(uint16_t v)
{
#if defined(DQLITE_BIG_ENDIAN)
	return v;
#elif defined(DQLITE_LITTLE_ENDIAN) && defined(DQLITE_HAVE_BSWAP)
    defined(__GNUC__) && __GNUC__ >= 4 && __GNUC_MINOR__ >= 8
	return __builtin_bswap16(v);
#else
	union {
		uint16_t u;
		uint8_t v[4];
	} s;

	s.v[0] = (uint8_t)(v >> 8);
	s.v[1] = (uint8_t)v;

	return s.u;
#endif
}

static uint32_t vfsFlip32(uint32_t v)
{
#if defined(DQLITE_BIG_ENDIAN)
	return v;
#elif defined(DQLITE_LITTLE_ENDIAN) && defined(DQLITE_HAVE_BSWAP)
	return __builtin_bswap32(v);
#else
	union {
		uint32_t u;
		uint8_t v[4];
	} s;

	s.v[0] = (uint8_t)(v >> 24);
	s.v[1] = (uint8_t)(v >> 16);
	s.v[2] = (uint8_t)(v >> 8);
	s.v[3] = (uint8_t)v;

	return s.u;
#endif
}

static uint16_t vfsGet16(const uint8_t *buf)
{
	union {
		uint16_t u;
		uint8_t v[2];
	} s;

	s.v[0] = buf[0];
	s.v[1] = buf[1];

	return vfsFlip16(s.u);
}

static uint32_t vfsGet32(const uint8_t *buf)
{
	union {
		uint32_t u;
		uint8_t v[4];
	} s;

	s.v[0] = buf[0];
	s.v[1] = buf[1];
	s.v[2] = buf[2];
	s.v[3] = buf[3];

	return vfsFlip32(s.u);
}

static void vfsPut32(uint32_t v, uint8_t *buf)
{
	uint32_t u = vfsFlip32(v);
	memcpy(buf, &u, sizeof u);
}

TEST_CASE(endian, get16, NULL)
{
	(void)params;
	(void)data;
	uint16_t x, y;
	uint8_t buf[2];
	for (x = 0; x < 1 << 8; x++) {
		for (y = 0; y < 1 << 8; y++) {
			buf[0] = (uint8_t)x;
			buf[1] = (uint8_t)y;
			munit_assert_uint16(ByteGetBe16(buf), ==, vfsGet16(buf));
		}
	}
	return MUNIT_OK;
}

TEST_CASE(endian, get32, NULL)
{
	(void)params;
	(void)data;
	uint8_t buf[4];
	uint32_t i;
	for (i = 0; i < 1 << 16; i++) {
		munit_rand_memory(4, buf);
		munit_assert_uint32(ByteGetBe32(buf), ==, vfsGet32(buf));
	}
	return MUNIT_OK;
}

TEST_CASE(endian, put32, NULL)
{
	(void)params;
	(void)data;
	uint32_t v;
	uint8_t buf[4], vfs_buf[4];
	uint32_t i;
	for (i = 0; i < (1 << 16); i++) {
		v = munit_rand_uint32();
		BytePutBe32(v, buf);
		vfsPut32(v, vfs_buf);
		munit_assert_memory_equal(4, buf, vfs_buf);
	}
	return MUNIT_OK;
}
