#include <stdbool.h>
#include <stdlib.h>
#include <unistd.h>

#include "buffer.h"

#include "../../include/dqlite.h"

/* How large is the buffer currently */
#define SIZE(B) (B->n_pages * B->page_size)

/* How many remaining bytes the buffer currently */
#define CAP(B) (SIZE(B) - B->offset)

int buffer__init(struct buffer *b)
{
	b->page_size = (unsigned)sysconf(_SC_PAGESIZE);
	b->n_pages = 1;
	b->data = malloc(SIZE(b));
	if (b->data == NULL) {
		return DQLITE_NOMEM;
	}
	b->offset = 0;
	return 0;
}

void buffer__close(struct buffer *b)
{
	free(b->data);
}

/* Ensure that the buffer has at least @size spare bytes */
static inline bool ensure(struct buffer *b, size_t size)
{
	void *data;
	uint32_t n_pages = b->n_pages;

	/* Double the buffer until we have enough capacity */
	while (size > CAP(b)) {
		b->n_pages *= 2;
	}

	/* CAP(b) was insufficient */
	if (b->n_pages > n_pages) {
		data = realloc(b->data, SIZE(b));
		if (data == NULL) {
			b->n_pages = n_pages;
			return false;
		}
		b->data = data;
	}

	return true;
}

void *buffer__advance(struct buffer *b, size_t size)
{
	void *cursor;

	if (!ensure(b, size)) {
		return NULL;
	}

	cursor = buffer__cursor(b, b->offset);
	b->offset += size;
	return cursor;
}

size_t buffer__offset(struct buffer *b) {
	return b->offset;
}

void *buffer__cursor(struct buffer *b, size_t offset)
{
	return b->data + offset;
}

void buffer__reset(struct buffer *b)
{
	b->offset = 0;
}
