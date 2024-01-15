/* Copyright (c) 2013, Ben Noordhuis <info@bnoordhuis.nl>
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#ifndef __XX_QUEUE_H__
#define __XX_QUEUE_H__

#include <stddef.h>

struct xx__queue {
  struct xx__queue* next;
  struct xx__queue* prev;
};

#define xx__queue_data(pointer, type, field)		\
  ((type*) ((char*) (pointer) - offsetof(type, field)))

#define xx__queue_foreach(q, h)				\
  for ((q) = (h)->next; (q) != (h); (q) = (q)->next)

static inline void xx__queue_init(struct xx__queue* q) {
  q->next = q;
  q->prev = q;
}

static inline int xx__queue_empty(const struct xx__queue* q) {
  return q == q->next;
}

static inline struct xx__queue* xx__queue_head(const struct xx__queue* q) {
  return q->next;
}

static inline struct xx__queue* xx__queue_next(const struct xx__queue* q) {
  return q->next;
}

static inline void xx__queue_add(struct xx__queue* h, struct xx__queue* n) {
  h->prev->next = n->next;
  n->next->prev = h->prev;
  h->prev = n->prev;
  h->prev->next = h;
}

static inline void xx__queue_split(struct xx__queue* h,
                                   struct xx__queue* q,
                                   struct xx__queue* n) {
  n->prev = h->prev;
  n->prev->next = n;
  n->next = q;
  h->prev = q->prev;
  h->prev->next = h;
  q->prev = n;
}

static inline void xx__queue_move(struct xx__queue* h, struct xx__queue* n) {
  if (xx__queue_empty(h))
    xx__queue_init(n);
  else
    xx__queue_split(h, h->next, n);
}

static inline void xx__queue_insert_head(struct xx__queue* h,
                                         struct xx__queue* q) {
  q->next = h->next;
  q->prev = h;
  q->next->prev = q;
  h->next = q;
}

static inline void xx__queue_insert_tail(struct xx__queue* h,
                                         struct xx__queue* q) {
  q->next = h;
  q->prev = h->prev;
  q->prev->next = q;
  h->prev = q;
}

static inline void xx__queue_remove(struct xx__queue* q) {
  q->prev->next = q->next;
  q->next->prev = q->prev;
}

#endif /* __XX_QUEUE_H__ */
