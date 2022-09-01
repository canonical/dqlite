#ifndef LIB_SERIALIZE_H_
#define LIB_SERIALIZE_H_

#include <stdint.h>
#include <string.h>

#include <uv.h>

#include "../../include/dqlite.h"

#include "assert.h"
#include "byte.h"

#define DQLITE_PARSE 1005

/**
 * The size in bytes of a single serialized word.
 */
#define SERIALIZE__WORD_SIZE 8

/* We rely on the size of double to be 64 bit, since that's what is sent over
 * the wire.
 *
 * See https://stackoverflow.com/questions/752309/ensuring-c-doubles-are-64-bits
 */
#ifndef __STDC_IEC_559__
#if __SIZEOF_DOUBLE__ != 8
#error "Requires IEEE 754 floating point!"
#endif
#endif
#ifdef static_assert
static_assert(sizeof(double) == sizeof(uint64_t),
	      "Size of 'double' is not 64 bits");
#endif

/**
 * Basic type aliases to used by macro-based processing.
 */
typedef const char *text_t;
typedef double float_t;
typedef uv_buf_t blob_t;

/**
 * Cursor to progressively read a buffer.
 */
struct cursor
{
	const void *p; /* Next byte to read */
	size_t cap;    /* Number of bytes left in the buffer */
};

/**
 * Define a serializable struct.
 *
 * NAME:   Name of the structure which will be defined.
 * FIELDS: List of X-based macros defining the fields in the schema, in the form
 *         of X(KIND, NAME, ##__VA_ARGS__). E.g. X(uint64, id, ##__VA_ARGS__).
 *
 * A new struct called NAME will be defined, along with sizeof, encode and
 * decode functions.
 */
#define SERIALIZE__DEFINE(NAME, FIELDS)         \
	SERIALIZE__DEFINE_STRUCT(NAME, FIELDS); \
	SERIALIZE__DEFINE_METHODS(NAME, FIELDS)

#define SERIALIZE__DEFINE_STRUCT(NAME, FIELDS)  \
	struct NAME                             \
	{                                       \
		FIELDS(SERIALIZE__DEFINE_FIELD) \
	}

#define SERIALIZE__DEFINE_METHODS(NAME, FIELDS)                   \
	size_t NAME##__sizeof(const struct NAME *p);              \
	void NAME##__encode(const struct NAME *p, void **cursor); \
	int NAME##__decode(struct cursor *cursor, struct NAME *p)

/* Define a single field in serializable struct.
 *
 * KIND:   Type code (e.g. uint64, text, etc).
 * MEMBER: Field name. */
#define SERIALIZE__DEFINE_FIELD(KIND, MEMBER) KIND##_t MEMBER;

/**
 * Implement the sizeof, encode and decode function of a serializable struct.
 */
#define SERIALIZE__IMPLEMENT(NAME, FIELDS)                        \
	size_t NAME##__sizeof(const struct NAME *p)               \
	{                                                         \
		size_t size = 0;                                  \
		FIELDS(SERIALIZE__SIZEOF_FIELD, p);               \
		return size;                                      \
	}                                                         \
	void NAME##__encode(const struct NAME *p, void **cursor)  \
	{                                                         \
		FIELDS(SERIALIZE__ENCODE_FIELD, p, cursor);       \
	}                                                         \
	int NAME##__decode(struct cursor *cursor, struct NAME *p) \
	{                                                         \
		int rc;                                           \
		FIELDS(SERIALIZE__DECODE_FIELD, p, cursor);       \
		return 0;                                         \
	}

#define SERIALIZE__SIZEOF_FIELD(KIND, MEMBER, P) \
	size += KIND##__sizeof(&((P)->MEMBER));

#define SERIALIZE__ENCODE_FIELD(KIND, MEMBER, P, CURSOR) \
	KIND##__encode(&((P)->MEMBER), CURSOR);

#define SERIALIZE__DECODE_FIELD(KIND, MEMBER, P, CURSOR) \
	rc = KIND##__decode(CURSOR, &((P)->MEMBER));     \
	if (rc != 0) {                                   \
		return rc;                               \
	}

DQLITE_INLINE size_t uint8__sizeof(const uint8_t *value)
{
	(void)value;
	return sizeof(uint8_t);
}

DQLITE_INLINE size_t uint16__sizeof(const uint16_t *value)
{
	(void)value;
	return sizeof(uint16_t);
}

DQLITE_INLINE size_t uint32__sizeof(const uint32_t *value)
{
	(void)value;
	return sizeof(uint32_t);
}

DQLITE_INLINE size_t uint64__sizeof(const uint64_t *value)
{
	(void)value;
	return sizeof(uint64_t);
}

DQLITE_INLINE size_t int64__sizeof(const int64_t *value)
{
	(void)value;
	return sizeof(int64_t);
}

DQLITE_INLINE size_t float__sizeof(const float_t *value)
{
	(void)value;
	return sizeof(double);
}

DQLITE_INLINE size_t text__sizeof(const text_t *value)
{
	return BytePad64(strlen(*value) + 1);
}

DQLITE_INLINE size_t blob__sizeof(const blob_t *value)
{
	return sizeof(uint64_t) /* length */ +
	       BytePad64(value->len) /* data */;
}

DQLITE_INLINE void uint8__encode(const uint8_t *value, void **cursor)
{
	*(uint8_t *)(*cursor) = *value;
	*cursor += sizeof(uint8_t);
}

DQLITE_INLINE void uint16__encode(const uint16_t *value, void **cursor)
{
	*(uint16_t *)(*cursor) = ByteFlipLe16(*value);
	*cursor += sizeof(uint16_t);
}

DQLITE_INLINE void uint32__encode(const uint32_t *value, void **cursor)
{
	*(uint32_t *)(*cursor) = ByteFlipLe32(*value);
	*cursor += sizeof(uint32_t);
}

DQLITE_INLINE void uint64__encode(const uint64_t *value, void **cursor)
{
	*(uint64_t *)(*cursor) = ByteFlipLe64(*value);
	*cursor += sizeof(uint64_t);
}

DQLITE_INLINE void int64__encode(const int64_t *value, void **cursor)
{
	*(int64_t *)(*cursor) = (int64_t)ByteFlipLe64((uint64_t)*value);
	*cursor += sizeof(int64_t);
}

DQLITE_INLINE void float__encode(const float_t *value, void **cursor)
{
	*(uint64_t *)(*cursor) = ByteFlipLe64(*(uint64_t *)value);
	*cursor += sizeof(uint64_t);
}

DQLITE_INLINE void text__encode(const text_t *value, void **cursor)
{
	size_t len = BytePad64(strlen(*value) + 1);
	memset(*cursor, 0, len);
	strcpy(*cursor, *value);
	*cursor += len;
}

DQLITE_INLINE void blob__encode(const blob_t *value, void **cursor)
{
	size_t len = BytePad64(value->len);
	uint64_t value_len = value->len;
	uint64__encode(&value_len, cursor);
	memcpy(*cursor, value->base, value->len);
	*cursor += len;
}

DQLITE_INLINE int uint8__decode(struct cursor *cursor, uint8_t *value)
{
	size_t n = sizeof(uint8_t);
	if (n > cursor->cap) {
		return DQLITE_PARSE;
	}
	*value = *(uint8_t *)cursor->p;
	cursor->p += n;
	cursor->cap -= n;
	return 0;
}

DQLITE_INLINE int uint16__decode(struct cursor *cursor, uint16_t *value)
{
	size_t n = sizeof(uint16_t);
	if (n > cursor->cap) {
		return DQLITE_PARSE;
	}
	*value = ByteFlipLe16(*(uint16_t *)cursor->p);
	cursor->p += n;
	cursor->cap -= n;
	return 0;
}

DQLITE_INLINE int uint32__decode(struct cursor *cursor, uint32_t *value)
{
	size_t n = sizeof(uint32_t);
	if (n > cursor->cap) {
		return DQLITE_PARSE;
	}
	*value = ByteFlipLe32(*(uint32_t *)cursor->p);
	cursor->p += n;
	cursor->cap -= n;
	return 0;
}

DQLITE_INLINE int uint64__decode(struct cursor *cursor, uint64_t *value)
{
	size_t n = sizeof(uint64_t);
	if (n > cursor->cap) {
		return DQLITE_PARSE;
	}
	*value = ByteFlipLe64(*(uint64_t *)cursor->p);
	cursor->p += n;
	cursor->cap -= n;
	return 0;
}

DQLITE_INLINE int int64__decode(struct cursor *cursor, int64_t *value)
{
	size_t n = sizeof(int64_t);
	if (n > cursor->cap) {
		return DQLITE_PARSE;
	}
	*value = (int64_t)ByteFlipLe64((uint64_t)*(int64_t *)cursor->p);
	cursor->p += n;
	cursor->cap -= n;
	return 0;
}

DQLITE_INLINE int float__decode(struct cursor *cursor, float_t *value)
{
	size_t n = sizeof(double);
	if (n > cursor->cap) {
		return DQLITE_PARSE;
	}
	*(uint64_t *)value = ByteFlipLe64(*(uint64_t *)cursor->p);
	cursor->p += n;
	cursor->cap -= n;
	return 0;
}

DQLITE_INLINE int text__decode(struct cursor *cursor, text_t *value)
{
	/* Find the terminating null byte of the next string, if any. */
	size_t len = strnlen(cursor->p, cursor->cap);
	size_t n;
	if (len == cursor->cap) {
		return DQLITE_PARSE;
	}
	*value = cursor->p;
	n = BytePad64(strlen(*value) + 1);
	cursor->p += n;
	cursor->cap -= n;
	return 0;
}

DQLITE_INLINE int blob__decode(struct cursor *cursor, blob_t *value)
{
	uint64_t len;
	size_t n;
	int rv;
	rv = uint64__decode(cursor, &len);
	if (rv != 0) {
		return rv;
	}
	n = BytePad64((size_t)len);
	if (n > cursor->cap) {
		return DQLITE_PARSE;
	}
	value->base = (char *)cursor->p;
	value->len = (size_t)len;
	cursor->p += n;
	cursor->cap -= n;
	return 0;
}

#endif /* LIB_SERIALIZE_H_ */
