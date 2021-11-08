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
