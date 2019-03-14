#ifndef BYTE_H_
#define BYTE_H_

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/**
 * Basic type aliases to used by macro-based processing.
 */
typedef const char *text_t;
typedef double double_t;

#if DQLITE_COVERAGE
#define DQLITE_INLINE static inline
#elif defined(__cplusplus) || \
    (defined(__STDC_VERSION__) && __STDC_VERSION__ >= 199901L)
#define DQLITE_INLINE inline
#else
#define DQLITE_INLINE static
#endif

/* Flip a 16-bit number to network byte order (little endian) */
DQLITE_INLINE uint16_t byte__flip16(uint16_t v)
{
#if defined(__BYTE_ORDER) && (__BYTE_ORDER == __LITTLE_ENDIAN)
	return v;
#elif defined(__BYTE_ORDER) && (__BYTE_ORDER == __BIG_ENDIAN) && \
    defined(__GNUC__) && __GNUC__ >= 4 && __GNUC_MINOR__ >= 8
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

/* Flip a 32-bit number to network byte order (little endian) */
DQLITE_INLINE uint32_t byte__flip32(uint32_t v)
{
#if defined(__BYTE_ORDER) && (__BYTE_ORDER == __LITTLE_ENDIAN)
	return v;
#elif defined(__BYTE_ORDER) && (__BYTE_ORDER == __BIG_ENDIAN) && \
    defined(__GNUC__) && __GNUC__ >= 4 && __GNUC_MINOR__ >= 8
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

/* Flip a 64-bit number to network byte order (little endian) */
DQLITE_INLINE uint64_t byte__flip64(uint64_t v)
{
#if defined(__BYTE_ORDER) && (__BYTE_ORDER == __LITTLE_ENDIAN)
	return v;
#elif defined(__BYTE_ORDER) && (__BYTE_ORDER == __BIG_ENDIAN) && \
    defined(__GNUC__) && __GNUC__ >= 4 && __GNUC_MINOR__ >= 8
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

/**
 * Add padding to size if it's not a multiple of 8. E.g. if 11 is passed, 16 is
 * returned.
 */
DQLITE_INLINE size_t byte__pad64(size_t size)
{
	size_t rest = size % sizeof(uint64_t);
	if (rest != 0) {
		size += sizeof(uint64_t) - rest;
	}
	return size;
}

DQLITE_INLINE size_t byte__sizeof_uint8(uint8_t value)
{
	return sizeof(value);
}

DQLITE_INLINE size_t byte__sizeof_uint16(uint16_t value)
{
	return sizeof(value);
}

DQLITE_INLINE size_t byte__sizeof_uint32(uint32_t value)
{
	return sizeof(value);
}

DQLITE_INLINE size_t byte__sizeof_uint64(uint64_t value)
{
	return sizeof(value);
}

DQLITE_INLINE size_t byte__sizeof_text(text_t value)
{
	return byte__pad64(strlen(value) + 1);
}

DQLITE_INLINE void byte__encode_uint8(uint8_t value, void **cursor)
{
	*(uint8_t *)(*cursor) = value;
	*cursor += sizeof(uint8_t);
}

DQLITE_INLINE void byte__encode_uint32(uint32_t value, void **cursor)
{
	*(uint32_t *)(*cursor) = byte__flip32(value);
	*cursor += sizeof(uint32_t);
}

DQLITE_INLINE void byte__encode_uint16(uint16_t value, void **cursor)
{
	*(uint16_t *)(*cursor) = byte__flip16(value);
	*cursor += sizeof(uint16_t);
}

DQLITE_INLINE void byte__encode_uint64(uint64_t value, void **cursor)
{
	*(uint64_t *)(*cursor) = byte__flip64(value);
	*cursor += sizeof(uint64_t);
}

DQLITE_INLINE void byte__encode_text(text_t value, void **cursor)
{
	size_t len = byte__pad64(strlen(value) + 1);
	memset(*cursor, 0, len);
	strcpy(*cursor, value);
	*cursor += len;
}

DQLITE_INLINE uint8_t byte__decode_uint8(const void **cursor)
{
	uint8_t value = *(uint8_t *)(*cursor);
	*cursor += sizeof(uint8_t);
	return value;
}

DQLITE_INLINE uint16_t byte__decode_uint16(const void **cursor)
{
	uint16_t value = byte__flip16(*(uint16_t *)(*cursor));
	*cursor += sizeof(uint16_t);
	return value;
}

DQLITE_INLINE uint32_t byte__decode_uint32(const void **cursor)
{
	uint32_t value = byte__flip32(*(uint32_t *)(*cursor));
	*cursor += sizeof(uint32_t);
	return value;
}

DQLITE_INLINE uint64_t byte__decode_uint64(const void **cursor)
{
	uint64_t value = byte__flip64(*(uint64_t *)(*cursor));
	*cursor += sizeof(uint64_t);
	return value;
}

DQLITE_INLINE text_t byte__decode_text(const void **cursor)
{
	text_t value = *cursor;
	*cursor += byte__pad64(strlen(value) + 1);
	return value;
}

#endif /* BYTE_H_ */
