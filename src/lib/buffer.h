/**
 * A dynamic buffer which can grow as needed when writing to it.
 *
 * The buffer size is always a multiple of the OS virtual memory page size, so
 * resizing the buffer *should* not incur in memory being copied.
 *
 * See https://stackoverflow.com/questions/16765389
 *
 * TODO: consider using mremap.
 */

#ifndef LIB_BUFFER_H_
#define LIB_BUFFER_H_

#include <unistd.h>

struct buffer
{
	void *data;         /* Allocated buffer */
	unsigned page_size; /* Size of an OS page */
	unsigned n_pages;   /* Number of pages allocated */
	size_t offset;      /* Next byte to write in the buffer */
};

/**
 * Initialize the buffer. It will initially have 1 memory  page.
 */
int buffer__init(struct buffer *b);

/**
 * Release the memory of the buffer.
 */
void buffer__close(struct buffer *b);

/**
 * Return a write cursor pointing to the next byte to write, ensuring that the
 * buffer has at least @size spare bytes.
 *
 * Return #NULL in case of out-of-memory errors.
 */
void *buffer__advance(struct buffer *b, size_t size);

/**
 * Return the offset of next byte to write.
 */
size_t buffer__offset(struct buffer *b);

/**
 * Return a write cursor pointing to the @offset'th byte of the buffer.
 */
void *buffer__cursor(struct buffer *b, size_t offset);

/**
 * Reset the write offset of the buffer.
 */
void buffer__reset(struct buffer *b);

#endif /* LIB_BUFFER_H_ */
