#ifndef DQLITE_BINARY_H
#define DQLITE_BINARY_H

#include <stdint.h>

#include "dqlite.h"

/*
 * Utilities for handling byte order.
 */

DQLITE_INLINE uint16_t dqlite__flip16(uint16_t v) {
#if defined(__BYTE_ORDER) && (__BYTE_ORDER == __LITTLE_ENDIAN)
	return v;
#elif defined(__BYTE_ORDER) && (__BYTE_ORDER == __BIG_ENDIAN) &&	\
	defined(__GNUC__) && __GNUC__ >= 4 && __GNUC_MINOR__ >= 8
	return __builtin_bswap16(v);
#else
	union { uint16_t u; uint8_t v[2]; } s;
	s.v[0] = (uint8_t)v;
	s.v[1] = (uint8_t)(v>>8);
	return s.u;
#endif
}

DQLITE_INLINE uint32_t dqlite__flip32(uint32_t v) {
#if defined(__BYTE_ORDER) && (__BYTE_ORDER == __LITTLE_ENDIAN)
	return v;
#elif defined(__BYTE_ORDER) && (__BYTE_ORDER == __BIG_ENDIAN) &&	\
	defined(__GNUC__) && __GNUC__ >= 4 && __GNUC_MINOR__ >= 8
	return __builtin_bswap32(v);
#else
	union { uint32_t u; uint8_t v[4]; } s;
	s.v[0] = (uint8_t)v;
	s.v[1] = (uint8_t)(v>>8);
	s.v[2] = (uint8_t)(v>>16);
	s.v[3] = (uint8_t)(v>>24);
	return s.u;
#endif
}

DQLITE_INLINE uint64_t dqlite__flip64(uint64_t v) {
#if defined(__BYTE_ORDER) && (__BYTE_ORDER == __LITTLE_ENDIAN)
	return v;
#elif defined(__BYTE_ORDER) && (__BYTE_ORDER == __BIG_ENDIAN) &&	\
	defined(__GNUC__) && __GNUC__ >= 4 && __GNUC_MINOR__ >= 8
	return __builtin_bswap64(v);
#else
	union { uint64_t u; uint8_t v[8]; } s;
	s.v[0] = (uint8_t)v;
	s.v[1] = (uint8_t)(v>>8);
	s.v[2] = (uint8_t)(v>>16);
	s.v[3] = (uint8_t)(v>>24);
	s.v[4] = (uint8_t)(v>>32);
	s.v[5] = (uint8_t)(v>>40);
	s.v[6] = (uint8_t)(v>>48);
	s.v[7] = (uint8_t)(v>>56);
	return s.u;
#endif
}

#endif /* DQLITE_BINARY_H */
