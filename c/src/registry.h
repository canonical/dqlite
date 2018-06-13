#ifndef DQLITE_REGISTRY_H
#define DQLITE_REGISTRY_H

#include <assert.h>
#include <string.h>
#include <stdint.h>
#include <stddef.h>

#include <sqlite3.h>

#include "dqlite.h"

/* Define a type-safe registry able to allocate and lookup objects of the given
 * type. */
#define DQLITE__REGISTRY(NAME, TYPE)					\
	struct NAME {							\
		struct TYPE **buf;					\
		size_t        len;					\
		size_t        cap;					\
	};								\
	void NAME ## _init(struct NAME *r);				\
	void NAME ## _close(struct NAME *r);				\
	int NAME ## _add(struct NAME *r, struct TYPE **item, size_t *i); \
	struct TYPE *NAME ## _get(struct NAME *r, size_t i);		\
	int NAME ## _del(struct NAME *r, size_t i)

/* Define the methods of a registry */
#define DQLITE__REGISTRY_METHODS(NAME, TYPE)				\
	void NAME ## _init(struct NAME *r)				\
	{								\
		assert(r != NULL);					\
									\
		r->buf = NULL;						\
		r->len = 0;						\
		r->cap = 0;						\
	}								\
									\
	void NAME ## _close(struct NAME *r)				\
	{								\
		size_t i;						\
		struct TYPE *item;					\
									\
		assert(r != NULL);					\
									\
		for (i = 0; i < r->len; i++) {				\
			item = *(r->buf + i);				\
			if (item != NULL) {				\
				TYPE ## _close(item);			\
				sqlite3_free(item);			\
			}						\
		}							\
									\
		if (r->buf != NULL)					\
			sqlite3_free(r->buf);				\
	}								\
									\
	int NAME ## _add(struct NAME *r, struct TYPE **item, size_t *i)	\
	{								\
		struct TYPE **buf;					\
		size_t cap;						\
									\
		assert(r != NULL);					\
		assert(item != NULL);					\
		assert(i != NULL);					\
									\
		/* Check if there is an unllocated slot. */		\
		for (*i = 0; *i < r->len; (*i)++) {			\
			if (*(r->buf + *i) == NULL)			\
				goto ok_slot;				\
		}							\
									\
		assert(*i == r->len);					\
									\
		/* There are no unallocated slots. If we are full, then	\
		 * double the capacity.					\
		 */							\
		if (r->len + 1 > r->cap) {				\
			cap = (r->cap == 0) ? 1 : r->cap * 2;		\
			buf = sqlite3_realloc(				\
				r->buf, cap * sizeof(*r->buf));		\
			if (buf == NULL)				\
				return DQLITE_NOMEM;			\
			r->buf = buf;					\
			r->cap = cap;					\
		}							\
		r->len++;						\
									\
	ok_slot:							\
		assert(*i < r->len);					\
									\
		*item = (struct TYPE*)sqlite3_malloc(sizeof(**item));	\
		if (*item == NULL)					\
			return DQLITE_NOMEM;				\
									\
		TYPE ## _init(*item);					\
									\
		*(r->buf + *i) = *item;					\
									\
		return 0;						\
	}								\
									\
	struct TYPE *NAME ## _get(struct NAME *r, size_t i)		\
	{								\
		assert(r != NULL);					\
									\
		if (i >= r->len)					\
			return NULL;					\
									\
		return *(r->buf + i);					\
									\
	}								\
									\
	int NAME ## _del(struct NAME *r, size_t i)			\
	{								\
		struct TYPE *item;					\
		struct TYPE **buf;					\
		size_t cap;						\
									\
		assert(r != NULL);					\
									\
		if (i >= r->len)					\
			return DQLITE_NOTFOUND;				\
									\
		item = *(r->buf + i);					\
		if (item == NULL)					\
			return DQLITE_NOTFOUND;				\
									\
		TYPE ## _close(item);					\
		sqlite3_free(item);					\
									\
		*(r->buf + i) = NULL;					\
									\
		/* If this was the last item in the registry buffer,	\
		 * decrease the length. */				\
		if (i == r->len -1) {					\
			r->len --;					\
		}							\
									\
		/* If the new length is less than half of the capacity,	\
		 * try to shrink the registry. */			\
		if (r->len < (r->cap / 2)) {				\
			cap = r->cap / 2;				\
			buf = sqlite3_realloc(				\
				r->buf, cap * sizeof(*r->buf));		\
			if (buf != NULL) {				\
				r->buf = buf;				\
				r->cap = cap;				\
			}						\
		}							\
									\
		return 0;						\
	}

#endif /* DQLITE_REGISTRY_H */
