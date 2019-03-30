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

#define SERIALIZE__DEFINE_METHODS(NAME, FIELDS)               \
	size_t NAME##__sizeof(const struct NAME *p);          \
	void NAME##__encode(const struct NAME *p, void *buf); \
	void NAME##__decode(void *buf, struct NAME *p)

/* Define a single field in serializable struct.
 *
 * KIND:   Type code (e.g. uint64, text, etc).
 * MEMBER: Field name. */
#define SERIALIZE__DEFINE_FIELD(KIND, MEMBER) KIND##_t MEMBER;

/**
 * Implement the sizeof, encode and decode function of a serializable struct.
 */
#define SERIALIZE__IMPLEMENT(NAME, FIELDS)                   \
	size_t NAME##__sizeof(const struct NAME *p)          \
	{                                                    \
		size_t size = 0;                             \
		FIELDS(SERIALIZE__SIZEOF_FIELD, p);          \
		return size;                                 \
	}                                                    \
	void NAME##__encode(const struct NAME *p, void *buf) \
	{                                                    \
		void *cursor = buf;                          \
		FIELDS(SERIALIZE__ENCODE_FIELD, p, &cursor); \
	}                                                    \
	void NAME##__decode(void *buf, struct NAME *p)       \
	{                                                    \
		const void *cursor = buf;                    \
		FIELDS(SERIALIZE__DECODE_FIELD, p, &cursor); \
	}

#define SERIALIZE__SIZEOF_FIELD(KIND, MEMBER, P) \
	size += byte__sizeof_##KIND(P->MEMBER);

#define SERIALIZE__ENCODE_FIELD(KIND, MEMBER, P, CURSOR) \
	byte__encode_##KIND(P->MEMBER, CURSOR);

#define SERIALIZE__DECODE_FIELD(KIND, MEMBER, P, CURSOR) \
	P->MEMBER = byte__decode_##KIND(CURSOR);

#endif /* ENCODING_H_ */
