#ifndef ENCODING_H_
#define ENCODING_H_

#include <stdint.h>
#include <string.h>

#include "byte.h"

/**
 * The size in bytes of a single serialized word.
 */
#define SERIALIZE__WORD_SIZE 8

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
	void NAME##__decode(const void **cursor, struct NAME *p)

/* Define a single field in serializable struct.
 *
 * KIND:   Type code (e.g. uint64, text, etc).
 * MEMBER: Field name. */
#define SERIALIZE__DEFINE_FIELD(KIND, MEMBER) KIND##_t MEMBER;

/**
 * Implement the sizeof, encode and decode function of a serializable struct.
 */
#define SERIALIZE__IMPLEMENT(NAME, FIELDS)                       \
	size_t NAME##__sizeof(const struct NAME *p)              \
	{                                                        \
		size_t size = 0;                                 \
		FIELDS(SERIALIZE__SIZEOF_FIELD, p);              \
		return size;                                     \
	}                                                        \
	void NAME##__encode(const struct NAME *p, void **cursor) \
	{                                                        \
		FIELDS(SERIALIZE__ENCODE_FIELD, p, cursor);      \
	}                                                        \
	void NAME##__decode(const void **cursor, struct NAME *p) \
	{                                                        \
		FIELDS(SERIALIZE__DECODE_FIELD, p, cursor);      \
	}

#define SERIALIZE__SIZEOF_FIELD(KIND, MEMBER, P) \
	size += KIND##__sizeof(P->MEMBER);

#define SERIALIZE__ENCODE_FIELD(KIND, MEMBER, P, CURSOR) \
	KIND##__encode(P->MEMBER, CURSOR);

#define SERIALIZE__DECODE_FIELD(KIND, MEMBER, P, CURSOR) \
	KIND##__decode(CURSOR, &((P)->MEMBER));

DQLITE_INLINE size_t uint8__sizeof(uint8_t value)
{
	return sizeof(value);
}

DQLITE_INLINE size_t uint16__sizeof(uint16_t value)
{
	return sizeof(value);
}

DQLITE_INLINE size_t uint32__sizeof(uint32_t value)
{
	return sizeof(value);
}

DQLITE_INLINE size_t uint64__sizeof(uint64_t value)
{
	return sizeof(value);
}

DQLITE_INLINE size_t text__sizeof(text_t value)
{
	return byte__pad64(strlen(value) + 1);
}

DQLITE_INLINE void uint8__encode(uint8_t value, void **cursor)
{
	*(uint8_t *)(*cursor) = value;
	*cursor += sizeof(uint8_t);
}

DQLITE_INLINE void uint16__encode(uint16_t value, void **cursor)
{
	*(uint16_t *)(*cursor) = byte__flip16(value);
	*cursor += sizeof(uint16_t);
}

DQLITE_INLINE void uint32__encode(uint32_t value, void **cursor)
{
	*(uint32_t *)(*cursor) = byte__flip32(value);
	*cursor += sizeof(uint32_t);
}

DQLITE_INLINE void uint64__encode(uint64_t value, void **cursor)
{
	*(uint64_t *)(*cursor) = byte__flip64(value);
	*cursor += sizeof(uint64_t);
}

DQLITE_INLINE void text__encode(text_t value, void **cursor)
{
	size_t len = byte__pad64(strlen(value) + 1);
	memset(*cursor, 0, len);
	strcpy(*cursor, value);
	*cursor += len;
}

DQLITE_INLINE void uint8__decode(const void **cursor, uint8_t *value)
{
	*value = *(uint8_t *)(*cursor);
	*cursor += sizeof(uint8_t);
}

DQLITE_INLINE void uint16__decode(const void **cursor, uint16_t *value)
{
	*value = byte__flip16(*(uint16_t *)(*cursor));
	*cursor += sizeof(uint16_t);
}

DQLITE_INLINE void uint32__decode(const void **cursor, uint32_t *value)
{
	*value = byte__flip32(*(uint32_t *)(*cursor));
	*cursor += sizeof(uint32_t);
}

DQLITE_INLINE void uint64__decode(const void **cursor, uint64_t *value)
{
	*value = byte__flip64(*(uint64_t *)(*cursor));
	*cursor += sizeof(uint64_t);
}

DQLITE_INLINE void text__decode(const void **cursor, text_t *value)
{
	*value = *cursor;
	*cursor += byte__pad64(strlen(*value) + 1);
}

#endif /* ENCODING_H_ */
