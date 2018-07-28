#ifndef DQLITE_BINARY_H
#define DQLITE_BINARY_H

#include <stdint.h>

#if defined(__cplusplus) ||                                                         \
    (defined(__STDC_VERSION__) && __STDC_VERSION__ >= 199901L)
#define DQLITE_INLINE inline
#else
#define DQLITE_INLINE static
#endif

/* Flip a 32-bit number to network byte order (little endian) */
DQLITE_INLINE uint32_t dqlite__flip32(uint32_t v) {
#if defined(__BYTE_ORDER) && (__BYTE_ORDER == __LITTLE_ENDIAN)
	return v;
#elif defined(__BYTE_ORDER) && (__BYTE_ORDER == __BIG_ENDIAN) &&                    \
    defined(__GNUC__) && __GNUC__ >= 4 && __GNUC_MINOR__ >= 8
	return __builtin_bswap32(v);
#else
	union {
		uint32_t u;
		uint8_t  v[4];
	} s;

	s.v[0] = (uint8_t)v;
	s.v[1] = (uint8_t)(v >> 8);
	s.v[2] = (uint8_t)(v >> 16);
	s.v[3] = (uint8_t)(v >> 24);

	return s.u;
#endif
}

/* Flip a 64-bit number to network byte order (little endian) */
DQLITE_INLINE uint64_t dqlite__flip64(uint64_t v) {
#if defined(__BYTE_ORDER) && (__BYTE_ORDER == __LITTLE_ENDIAN)
	return v;
#elif defined(__BYTE_ORDER) && (__BYTE_ORDER == __BIG_ENDIAN) &&                    \
    defined(__GNUC__) && __GNUC__ >= 4 && __GNUC_MINOR__ >= 8
	return __builtin_bswap64(v);
#else
	union {
		uint64_t u;
		uint8_t  v[8];
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

#endif /* DQLITE_BINARY_H */
