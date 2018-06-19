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

#define __DQLITE__SCHEMA_HANDLER_FIELD_DEFINE(CODE, STRUCT, NAME, _)	\
	struct STRUCT NAME;

#define DQLITE__SCHEMA_HANDLER_DEFINE(NAME, TYPES)			\
	struct NAME {							\
		struct dqlite__message message;				\
		uint64_t timestamp;					\
		uint8_t type;						\
		uint8_t flags;						\
		dqlite__error error;					\
		union {							\
			TYPES(__DQLITE__SCHEMA_HANDLER_FIELD_DEFINE, )	\
		};							\
	};								\
									\
	void NAME ## _init(struct NAME *h);				\
									\
	void NAME ## _close(struct NAME *h);				\
									\
	int NAME ## _encode(struct NAME *h);				\
									\
	int NAME ## _decode(struct NAME *h)

#define __DQLITE__SCHEMA_HANDLER_FIELD_PUT(CODE, STRUCT, NAME, _)	\
	case CODE:							\
	err = STRUCT ## _put(&h->NAME, &h->message, &h->error);		\
	break;								\

#define __DQLITE__SCHEMA_HANDLER_FIELD_GET(CODE, STRUCT, NAME, _)	\
	case CODE:							\
									\
	err = STRUCT ## _get(&h->NAME, &h->message, &h->error);		\
	if (err != 0) {							\
		dqlite__error_wrapf(					\
			&h->error, &h->error,				\
			"failed to decode '%s'", #NAME);		\
		return err;						\
	}								\
									\
	break;

#define DQLITE__SCHEMA_HANDLER_IMPLEMENT(NAME, TYPES)	\
									\
	void NAME ## _init(struct NAME *h)				\
	{								\
		assert(h != NULL);					\
									\
		h->type = 0;						\
		h->flags = 0;						\
									\
		dqlite__message_init(&h->message);			\
		dqlite__error_init(&h->error);				\
									\
		dqlite__lifecycle_init(DQLITE__LIFECYCLE_ENCODER);	\
	};								\
									\
	void NAME ## _close(struct NAME *h)				\
	{								\
		assert(h != NULL);					\
									\
		dqlite__error_close(&h->error);				\
		dqlite__message_close(&h->message);			\
									\
		dqlite__lifecycle_close(DQLITE__LIFECYCLE_ENCODER);	\
	}								\
									\
	int NAME ## _encode(struct NAME *h)				\
	{								\
		int err;						\
									\
		assert(h != NULL);					\
									\
		dqlite__message_header_put(				\
			&h->message, h->type, h->flags);		\
									\
		switch (h->type) {					\
			TYPES(__DQLITE__SCHEMA_HANDLER_FIELD_PUT, );	\
									\
		default:						\
			dqlite__error_printf(				\
				&h->error, "unknown message type %d",	\
				h->type);				\
			return DQLITE_PROTO;				\
		}							\
									\
		if (err != 0) {						\
			dqlite__error_wrapf(\
				&h->error, &h->message.error,		\
				"encode error");			\
			return err;					\
		}							\
									\
		return 0;						\
	}								\
									\
	int NAME ## _decode(struct NAME *h)				\
	{								\
		int err;						\
									\
		assert(h != NULL);					\
									\
		h->type = h->message.type;				\
									\
		switch (h->type) {					\
			TYPES(__DQLITE__SCHEMA_HANDLER_FIELD_GET, );	\
		default:						\
			dqlite__error_printf(				\
				&h->error, "unknown message type %d",	\
				h->type);				\
			return DQLITE_PROTO;				\
		}							\
									\
		return 0;						\
	}

#endif /* DQLITE_SCHEMA_H */
