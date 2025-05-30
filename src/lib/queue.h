#ifndef LIB_QUEUE_H_
#define LIB_QUEUE_H_

#include <stddef.h> /* offsetof */

struct queue
{
	struct queue *next;
	struct queue *prev;
};

typedef struct queue queue;

#define QUEUE_DATA(e, type, field) \
	((type *)((void *)((char *)(e)-offsetof(type, field))))

#define QUEUE_FOREACH(q, h) for ((q) = (h)->next; (q) != (h); (q) = (q)->next)

static inline void queue_init(struct queue *q)
{
	q->next = q;
	q->prev = q;
}

static inline int queue_empty(const struct queue *q)
{
	return q == q->next;
}

static inline struct queue *queue_head(const struct queue *q)
{
	return q->next;
}

static inline struct queue *queue_next(const struct queue *q)
{
	return q->next;
}

static inline struct queue *queue_tail(const struct queue *q)
{
	return q->prev;
}

static inline void queue_add(struct queue *h, struct queue *n)
{
	h->prev->next = n->next;
	n->next->prev = h->prev;
	h->prev = n->prev;
	h->prev->next = h;
}

static inline void queue_split(struct queue *h,
			       struct queue *q,
			       struct queue *n)
{
	n->prev = h->prev;
	n->prev->next = n;
	n->next = q;
	h->prev = q->prev;
	h->prev->next = h;
	q->prev = n;
}

static inline void queue_move(struct queue *h, struct queue *n)
{
	if (queue_empty(h))
		queue_init(n);
	else
		queue_split(h, h->next, n);
}

static inline void queue_insert_head(struct queue *h, struct queue *q)
{
	q->next = h->next;
	q->prev = h;
	q->next->prev = q;
	h->next = q;
}

static inline void queue_insert_tail(struct queue *h, struct queue *q)
{
	q->next = h;
	q->prev = h->prev;
	q->prev->next = q;
	h->prev = q;
}

static inline void queue_remove(struct queue *q)
{
	q->prev->next = q->next;
	q->next->prev = q->prev;
}

#endif /* LIB_QUEUE_H_*/
