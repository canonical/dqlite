#ifndef LIB_BYTE_H_
#define LIB_BYTE_H_

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#if defined(__cplusplus)
#define DQLITE_INLINE inline
#else
#define DQLITE_INLINE static inline
#endif

#if defined(__BYTE_ORDER__) && (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
#define DQLITE_LITTLE_ENDIAN
#elif defined(__BYTE_ORDER__) && (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
#define DQLITE_BIG_ENDIAN
#endif

#if defined(__GNUC__) && __GNUC__ >= 4 && __GNUC_MINOR__ >= 8
#define DQLITE_HAVE_BSWAP
#endif

/* Flip a 16-bit number to little-endian byte order */
DQLITE_INLINE uint16_t ByteFlipLe16(uint16_t v)
{
#if defined(DQLITE_LITTLE_ENDIAN)
	return v;
#elif defined(DQLITE_BIG_ENDIAN) && defined(DQLITE_HAVE_BSWAP)
	return __builtin_bswap16(v);
#else
	union {
		uint16_t u;
		uint8_t v[2];
	} s;

	s.v[0] = (uint8_t)v;
	s.v[1] = (uint8_t)(v >> 8);

	return s.u;
#endif
}

/* Flip a 32-bit number to little-endian byte order */
DQLITE_INLINE uint32_t ByteFlipLe32(uint32_t v)
{
#if defined(DQLITE_LITTLE_ENDIAN)
	return v;
#elif defined(DQLITE_BIG_ENDIAN) && defined(DQLITE_HAVE_BSWAP)
	return __builtin_bswap32(v);
#else
	union {
		uint32_t u;
		uint8_t v[4];
	} s;

	s.v[0] = (uint8_t)v;
	s.v[1] = (uint8_t)(v >> 8);
	s.v[2] = (uint8_t)(v >> 16);
	s.v[3] = (uint8_t)(v >> 24);

	return s.u;
#endif
}

/* Flip a 64-bit number to little-endian byte order */
DQLITE_INLINE uint64_t ByteFlipLe64(uint64_t v)
{
#if defined(DQLITE_LITTLE_ENDIAN)
	return v;
#elif defined(DQLITE_BIG_ENDIAN) && defined(DQLITE_HAVE_BSWAP)
	return __builtin_bswap64(v);
#else
	union {
		uint64_t u;
		uint8_t v[8];
	} s;

	s.v[0] = (uint8_t)v;
	s.v[1] = (uint8_t)(v >> 8);
	s.v[2] = (uint8_t)(v >> 16);
	s.v[3] = (uint8_t)(v >> 24);
	s.v[4] = (uint8_t)(v >> 32);
	s.v[5] = (uint8_t)(v >> 40);
	s.v[6] = (uint8_t)(v >> 48);
	s.v[7] = (uint8_t)(v >> 56);

	return s.u;
#endif
}

/* -Wconversion before GCC 10 is overly sensitive. */
#if defined(__GNUC__) && __GNUC__ < 10
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#endif

DQLITE_INLINE uint16_t ByteGetBe16(const uint8_t *buf)
{
	uint16_t x = buf[0];
	uint16_t y = buf[1];
	x <<= 8;
	return x | y;
}

DQLITE_INLINE uint32_t ByteGetBe32(const uint8_t *buf)
{
	uint32_t w = buf[0];
	uint32_t x = buf[1];
	uint32_t y = buf[2];
	uint32_t z = buf[3];
	w <<= 24;
	x <<= 16;
	y <<= 8;
	return w | x | y | z;
}

DQLITE_INLINE void BytePutBe32(uint32_t v, uint8_t *buf)
{
	buf[0] = (uint8_t)(v >> 24);
	buf[1] = (uint8_t)(v >> 16);
	buf[2] = (uint8_t)(v >> 8);
	buf[3] = (uint8_t)v;
}

/**
 * Add padding to size if it's not a multiple of 8. E.g. if 11 is passed, 16 is
 * returned.
 */
DQLITE_INLINE size_t BytePad64(size_t size)
{
	size_t rest = size % sizeof(uint64_t);
	if (rest != 0) {
		size += sizeof(uint64_t) - rest;
	}
	return size;
}

#define ARRAY_SIZE(a) ((sizeof(a)) / (sizeof(a)[0]))

#if defined(__GNUC__) && __GNUC__ < 10
#pragma GCC diagnostic pop
#endif

#endif /* LIB_BYTE_H_ */
