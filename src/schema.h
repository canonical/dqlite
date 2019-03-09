#ifndef DQLITE_SCHEMA_H
#define DQLITE_SCHEMA_H

#include "./lib/assert.h"

#include "error.h"
#include "lifecycle.h"
#include "message.h"

/**
 * Define a new schema object.
 *
 * NAME:   Name of the structure which will be defined.
 * SCHEMA: List of X-based macros defining the fields in the schema, in the form
 *         of X(KIND, NAME, __VA_ARGS__). E.g. X(uint64, id, __VA_ARGS__).
 */
#define SCHEMA__DEFINE(NAME, SCHEMA)                                         \
	struct NAME                                                          \
	{                                                                    \
		SCHEMA(SCHEMA__FIELD_DEFINE, )                               \
	};                                                                   \
                                                                             \
	int NAME##_put(struct NAME *p, struct message *m, dqlite__error *e); \
                                                                             \
	int NAME##_get(struct NAME *p, struct message *m, dqlite__error *e)

/**
 * Implement a new schema object.
 *
 * NAME:   Name of the structure which will be defined.
 * SCHEMA: List of X-macros defining the fields in the schema, in the form
 *         of X(KIND, NAME, __VA_ARGS__). E.g. X(uint64, id, __VA_ARGS__).
 */
#define SCHEMA__IMPLEMENT(NAME, SCHEMA)                                     \
                                                                            \
	int NAME##_put(struct NAME *p, struct message *m, dqlite__error *e) \
	{                                                                   \
		int err;                                                    \
                                                                            \
		assert(p != NULL);                                          \
		assert(m != NULL);                                          \
                                                                            \
		SCHEMA(SCHEMA__FIELD_PUT, p, m, e);                         \
                                                                            \
		return 0;                                                   \
	};                                                                  \
                                                                            \
	int NAME##_get(struct NAME *p, struct message *m, dqlite__error *e) \
	{                                                                   \
		int err;                                                    \
                                                                            \
		assert(p != NULL);                                          \
		assert(m != NULL);                                          \
                                                                            \
		SCHEMA(SCHEMA__FIELD_GET, p, m, e);                         \
                                                                            \
		return 0;                                                   \
	}

/* Define a single field in message schema.
 *
 * KIND:   Type code (e.g. uint64, text, etc).
 * MEMBER: Field name. */
#define SCHEMA__FIELD_DEFINE(KIND, MEMBER, _) KIND##_t MEMBER;

/* Encode a single field in message schema.
 *
 * KIND:   Type code.
 * MEMBER: Field name.
 * P:      Pointer to the message schema object.
 * M:      Pointer to the underlying message.
 * E:      Pointer to the error to fill in case of failures. */
#define SCHEMA__FIELD_PUT(KIND, MEMBER, P, M, E)                        \
	err = message__body_put_##KIND(M, (P)->MEMBER);                 \
	if (err != 0 && err != DQLITE_EOM) {                            \
		dqlite__error_wrapf(E, &(M)->error, "failed to put %s", \
				    #MEMBER);                           \
		return err;                                             \
	}

/* Decode a single field in message schema.
 *
 * KIND:   Type code.
 * MEMBER: Field name.
 * P:      Pointer to the message schema object.
 * M:      Pointer to the underlying message.
 * E:      Pointer to the error to fill in case of failures. */
#define SCHEMA__FIELD_GET(KIND, MEMBER, P, M, E)                          \
	err = message__body_get_##KIND(M, &(P)->MEMBER);                  \
	if (err != 0 && err != DQLITE_EOM) {                              \
		dqlite__error_wrapf(E, &(M)->error,                       \
				    "failed to get '%s' field", #MEMBER); \
		return err;                                               \
	}

#define SCHEMA__HANDLER_FIELD_DEFINE(CODE, STRUCT, NAME, _) struct STRUCT NAME;

/* Define a new schema handler.
 *
 * A schema handler can encode or decode messages of multiple schema types.
 *
 * NAME:  Name of the structure which will be defined.
 * TYPES: List of X-macros defining the schema types this handler supports, in
 *        the form X(CODE, STRUCT, NAME, __VA_ARGS__), where CODE is and integer
 *        type code identifying the schema type, STRUCT is the message schema
 *        structure as defined with SCHEMA__DEFINE and NAME is the name
 *        of the field holding this schema object in the schema handler.
 * */
#define SCHEMA__HANDLER_DEFINE(NAME, TYPES)                   \
	struct NAME                                           \
	{                                                     \
		struct message message;                       \
		uint64_t timestamp;                           \
		uint8_t type;                                 \
		uint8_t flags;                                \
		dqlite__error error;                          \
		union {                                       \
			TYPES(SCHEMA__HANDLER_FIELD_DEFINE, ) \
		};                                            \
	};                                                    \
                                                              \
	void NAME##_init(struct NAME *h);                     \
                                                              \
	void NAME##_close(struct NAME *h);                    \
                                                              \
	int NAME##_encode(struct NAME *h);                    \
                                                              \
	int NAME##_decode(struct NAME *h)

#define SCHEMA__HANDLER_FIELD_PUT(CODE, STRUCT, NAME, _)              \
	case CODE:                                                    \
		err = STRUCT##_put(&h->NAME, &h->message, &h->error); \
		break;

#define SCHEMA__HANDLER_FIELD_GET(CODE, STRUCT, NAME, _)                     \
	case CODE:                                                           \
                                                                             \
		err = STRUCT##_get(&h->NAME, &h->message, &h->error);        \
		if (err != 0) {                                              \
			dqlite__error_wrapf(&h->error, &h->error,            \
					    "failed to decode '%s'", #NAME); \
			return err;                                          \
		}                                                            \
                                                                             \
		break;

/* Implement a new schema handler. */
#define SCHEMA__HANDLER_IMPLEMENT(NAME, TYPES)                            \
                                                                          \
	void NAME##_init(struct NAME *h)                                  \
	{                                                                 \
		assert(h != NULL);                                        \
                                                                          \
		h->type = 0;                                              \
		h->flags = 0;                                             \
                                                                          \
		message__init(&h->message);                               \
		dqlite__error_init(&h->error);                            \
                                                                          \
		dqlite__lifecycle_init(DQLITE__LIFECYCLE_ENCODER);        \
	};                                                                \
                                                                          \
	void NAME##_close(struct NAME *h)                                 \
	{                                                                 \
		assert(h != NULL);                                        \
                                                                          \
		dqlite__error_close(&h->error);                           \
		message__close(&h->message);                              \
                                                                          \
		dqlite__lifecycle_close(DQLITE__LIFECYCLE_ENCODER);       \
	}                                                                 \
                                                                          \
	int NAME##_encode(struct NAME *h)                                 \
	{                                                                 \
		int err = 0;                                              \
                                                                          \
		assert(h != NULL);                                        \
                                                                          \
		message__header_put(&h->message, h->type, h->flags);      \
                                                                          \
		switch (h->type) {                                        \
			TYPES(SCHEMA__HANDLER_FIELD_PUT, );               \
                                                                          \
			default:                                          \
				dqlite__error_printf(                     \
				    &h->error, "unknown message type %d", \
				    h->type);                             \
				err = DQLITE_PROTO;                       \
				goto out;                                 \
		}                                                         \
                                                                          \
		if (err != 0) {                                           \
			dqlite__error_wrapf(&h->error, &h->message.error, \
					    "encode error");              \
			goto out;                                         \
		}                                                         \
                                                                          \
	out:                                                              \
		return err;                                               \
	}                                                                 \
                                                                          \
	int NAME##_decode(struct NAME *h)                                 \
	{                                                                 \
		int err;                                                  \
                                                                          \
		assert(h != NULL);                                        \
                                                                          \
		h->type = h->message.type;                                \
                                                                          \
		switch (h->type) {                                        \
			TYPES(SCHEMA__HANDLER_FIELD_GET, );               \
			default:                                          \
				dqlite__error_printf(                     \
				    &h->error, "unknown message type %d", \
				    h->type);                             \
				return DQLITE_PROTO;                      \
		}                                                         \
                                                                          \
		return 0;                                                 \
	}

#endif /* DQLITE_SCHEMA_H */
