#ifndef DQLITE_SCHEMA_H
#define DQLITE_SCHEMA_H

#include <assert.h>

#include "error.h"
#include "lifecycle.h"
#include "message.h"

/*
 * Macros for defining message schemas
 */

#define __DQLITE__SCHEMA_FIELD_DEFINE(KIND, MEMBER, _)	\
	KIND ## _t MEMBER;

#define __DQLITE__SCHEMA_FIELD_PUT(KIND, MEMBER, P, M, E)		\
	err = dqlite__message_body_put_ ## KIND(M, (P)->MEMBER);	\
	if (err != 0 && err != DQLITE_EOM) {				\
		dqlite__error_wrapf(					\
			E, &(M)->error,	"failed to put %s", #MEMBER);	\
		return err;						\
	}

#define __DQLITE__SCHEMA_FIELD_GET(KIND, MEMBER, P, M, E)		\
	err = dqlite__message_body_get_ ## KIND(M, &(P)->MEMBER);	\
	if (err != 0 && err != DQLITE_EOM) {				\
		dqlite__error_wrapf(					\
			E, &(M)->error,					\
			"failed to get '%s' field", #MEMBER);		\
		return err;						\
	}

#define DQLITE__SCHEMA_DEFINE(NAME, SCHEMA)				\
	struct NAME {							\
		SCHEMA(__DQLITE__SCHEMA_FIELD_DEFINE, )			\
	};								\
									\
	int NAME ## _put(						\
		struct NAME *p,						\
		struct dqlite__message *m,				\
		dqlite__error *e);					\
									\
	int NAME ## _get(						\
		struct NAME *p,						\
		struct dqlite__message *m,				\
		dqlite__error *e)

#define DQLITE__SCHEMA_IMPLEMENT(NAME, SCHEMA)				\
									\
	int NAME ## _put(						\
		struct NAME *p,						\
		struct dqlite__message *m,				\
		dqlite__error *e)					\
	{								\
		int err;						\
									\
		assert(p != NULL);					\
		assert(m != NULL);					\
									\
		SCHEMA(__DQLITE__SCHEMA_FIELD_PUT, p, m, e);		\
									\
		return 0;						\
	};								\
									\
	int NAME ## _get(						\
		struct NAME *p,						\
		struct dqlite__message *m,				\
		dqlite__error *e)					\
	{								\
		int err;						\
									\
		assert(p != NULL);					\
		assert(m != NULL);					\
									\
		SCHEMA(__DQLITE__SCHEMA_FIELD_GET, p, m, e);		\
									\
		return 0;						\
	}

#define __DQLITE__SCHEMA_ENCODER_FIELD_DEFINE(CODE, STRUCT, NAME, _)	\
	struct STRUCT NAME;

#define DQLITE__SCHEMA_ENCODER_DEFINE(NAME, TYPES)			\
	struct NAME {							\
		struct dqlite__message message;				\
		uint8_t type;						\
		uint8_t flags;						\
		dqlite__error error;					\
		union {							\
			TYPES(__DQLITE__SCHEMA_ENCODER_FIELD_DEFINE, )	\
		};							\
	};								\
									\
	void NAME ## _init(struct NAME *e);				\
									\
	void NAME ## _close(struct NAME *e);				\
									\
	int NAME ## _encode(struct NAME *e)				\

#define __DQLITE__SCHEMA_ENCODER_FIELD_PUT(CODE, STRUCT, NAME, _)	\
	case CODE:							\
	err = STRUCT ## _put(&e->NAME, &e->message, &e->error);		\
	break;								\

#define DQLITE__SCHEMA_ENCODER_IMPLEMENT(NAME, TYPES)	\
									\
	void NAME ## _init(struct NAME *e)				\
	{								\
		assert(e != NULL);					\
									\
		e->type = 0;						\
		e->flags = 0;						\
									\
		dqlite__message_init(&e->message);			\
		dqlite__error_init(&e->error);				\
									\
		dqlite__lifecycle_init(DQLITE__LIFECYCLE_ENCODER);	\
	};								\
									\
	void NAME ## _close(struct NAME *e)				\
	{								\
		assert(e != NULL);					\
									\
		dqlite__error_close(&e->error);				\
		dqlite__message_close(&e->message);			\
									\
		dqlite__lifecycle_close(DQLITE__LIFECYCLE_ENCODER);	\
	}								\
									\
	int NAME ## _encode(struct NAME *e)				\
	{								\
		int err;						\
									\
		assert(e != NULL);					\
									\
		dqlite__message_header_put(				\
			&e->message, e->type, e->flags);		\
									\
		switch (e->type) {					\
			TYPES(__DQLITE__SCHEMA_ENCODER_FIELD_PUT, );	\
									\
		default:						\
			dqlite__error_printf(				\
				&e->error, "unknown message type %d",	\
				e->type);				\
			return DQLITE_PROTO;				\
		}							\
									\
		if (err != 0) {						\
			dqlite__error_wrapf(\
				&e->error, &e->message.error,		\
				"encode error");			\
			return err;					\
		}							\
									\
		return 0;						\
	}

#define __DQLITE__SCHEMA_DECODER_FIELD_DEFINE(CODE, STRUCT, NAME, _)	\
	struct STRUCT NAME;

#define DQLITE__SCHEMA_DECODER_DEFINE(NAME, TYPES)			\
	struct NAME {							\
		struct dqlite__message message;				\
		uint64_t timestamp;					\
		uint8_t type;						\
		dqlite__error   error;					\
		union {							\
			TYPES(__DQLITE__SCHEMA_DECODER_FIELD_DEFINE, NAME) \
		};							\
	};								\
									\
	void NAME ## _init(struct NAME *d);				\
									\
	void NAME ## _close(struct NAME *d);				\
									\
	int NAME ## _decode(struct NAME *d)

#define __DQLITE__SCHEMA_DECODER_FIELD_GET(CODE, STRUCT, NAME, _)	\
	case CODE:							\
									\
	err = STRUCT ## _get(&d->NAME, &d->message, &d->error);		\
	if (err != 0) {							\
		dqlite__error_wrapf(					\
			&d->error, &d->error,				\
			"failed to decode '%s'", #NAME);		\
		return err;						\
	}								\
									\
	break;

#define DQLITE__SCHEMA_DECODER_IMPLEMENT(NAME, TYPES)			\
									\
	void NAME ## _init(struct NAME *d)				\
	{								\
		assert(d != NULL);					\
									\
		dqlite__message_init(&d->message);			\
		dqlite__error_init(&d->error);				\
									\
		dqlite__lifecycle_init(DQLITE__LIFECYCLE_DECODER);	\
	};								\
									\
	void NAME ## _close(struct NAME *d)				\
	{								\
		assert(d != NULL);					\
									\
		dqlite__error_close(&d->error);				\
		dqlite__message_close(&d->message);			\
									\
		dqlite__lifecycle_close(DQLITE__LIFECYCLE_DECODER);	\
	}								\
									\
	int NAME ## _decode(struct NAME *d)				\
	{								\
		int err;						\
									\
		assert(d != NULL);					\
									\
		d->type = d->message.type;				\
									\
		switch (d->type) {					\
			TYPES(__DQLITE__SCHEMA_DECODER_FIELD_GET, );	\
		default:						\
			dqlite__error_printf(				\
				&d->error, "unknown message type %d",	\
				d->type);				\
			return DQLITE_PROTO;				\
		}							\
									\
		return 0;						\
	}


#endif /* DQLITE_SCHEMA_H */
