#ifndef LIB_QUEUE_H_
#define LIB_QUEUE_H_

#include <stddef.h>

typedef void *queue[2];

/* Private macros. */
#define QUEUE__NEXT(q) (*(queue **)&((*(q))[0]))
#define QUEUE__PREV(q) (*(queue **)&((*(q))[1]))

#define QUEUE__PREV_NEXT(q) (QUEUE__NEXT(QUEUE__PREV(q)))
#define QUEUE__NEXT_PREV(q) (QUEUE__PREV(QUEUE__NEXT(q)))

/**
 * Initialize an empty queue.
 */
#define QUEUE__INIT(q)                \
	{                             \
		QUEUE__NEXT(q) = (q); \
		QUEUE__PREV(q) = (q); \
	}

/**
 * Return true if the queue has no element.
 */
#define QUEUE__IS_EMPTY(q) ((const queue *)(q) == (const queue *)QUEUE__NEXT(q))

/**
 * Insert an element at the back of a queue.
 */
#define QUEUE__PUSH(q, e)                        \
	{                                        \
		QUEUE__NEXT(e) = (q);            \
		QUEUE__PREV(e) = QUEUE__PREV(q); \
		QUEUE__PREV_NEXT(e) = (e);       \
		QUEUE__PREV(q) = (e);            \
	}

#define QUEUE__INSERT_TAIL(q, e) QUEUE__PUSH(q, e)

/**
 * Insert an element at the front of a queue.
 */
#define QUEUE__INSERT_HEAD(h, q)                 \
	{                                        \
		QUEUE__NEXT(q) = QUEUE__NEXT(h); \
		QUEUE__PREV(q) = (h);            \
		QUEUE__NEXT_PREV(q) = (q);       \
		QUEUE__NEXT(h) = (q);            \
	}

/**
 * Remove the given element from the queue. Any element can be removed at any
 * time.
 */
#define QUEUE__REMOVE(e)                              \
	{                                             \
		QUEUE__PREV_NEXT(e) = QUEUE__NEXT(e); \
		QUEUE__NEXT_PREV(e) = QUEUE__PREV(e); \
	}

/**
 * Moves elements from queue @h to queue @n
 * Note: Removed QUEUE__SPLIT() and merged it into QUEUE__MOVE().
 */
#define QUEUE__MOVE(h, n)                                  \
	{                                                  \
		if (QUEUE__IS_EMPTY(h)) {                  \
			QUEUE__INIT(n);                    \
		} else {                                   \
			queue *__q = QUEUE__HEAD(h);       \
			QUEUE__PREV(n) = QUEUE__PREV(h);   \
			QUEUE__PREV_NEXT(n) = (n);         \
			QUEUE__NEXT(n) = (__q);            \
			QUEUE__PREV(h) = QUEUE__PREV(__q); \
			QUEUE__PREV_NEXT(h) = (h);         \
			QUEUE__PREV(__q) = (n);            \
		}                                          \
	}

/**
 * Return the element at the front of the queue.
 */
#define QUEUE__HEAD(q) (QUEUE__NEXT(q))

/**
 * Return the element at the back of the queue.
 */
#define QUEUE__TAIL(q) (QUEUE__PREV(q))

/**
 * Iternate over the element of a queue.
 *
 * Mutating the queue while iterating results in undefined behavior.
 */
#define QUEUE__FOREACH(q, e) \
	for ((q) = QUEUE__NEXT(e); (q) != (e); (q) = QUEUE__NEXT(q))

/**
 * Return the structure holding the given element.
 */
#define QUEUE__DATA(e, type, field) \
	((type *)(uintptr_t)((char *)(e)-offsetof(type, field)))

#endif /* LIB_QUEUE_H_*/
