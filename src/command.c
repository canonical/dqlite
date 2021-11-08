#include <sqlite3.h>

#include "../include/dqlite.h"

#include "lib/serialize.h"

#include "command.h"
#include "protocol.h"

#define FORMAT 1 /* Format version */

#define HEADER(X, ...)                    \
	X(uint8, format, ##__VA_ARGS__)   \
	X(uint8, type, ##__VA_ARGS__)     \
	X(uint8, _unused1, ##__VA_ARGS__) \
	X(uint8, _unused2, ##__VA_ARGS__) \
	X(uint32, _unused3, ##__VA_ARGS__)

SERIALIZE__DEFINE(header, HEADER);
SERIALIZE__IMPLEMENT(header, HEADER);

static size_t frames__sizeof(const frames_t *frames)
{
	size_t s = uint32__sizeof(&frames->n_pages) +
		   uint16__sizeof(&frames->page_size) +
		   uint16__sizeof(&frames->__unused__) +
		   sizeof(uint64_t) * frames->n_pages + /* Page numbers */
		   frames->page_size * frames->n_pages; /* Page data */
	return s;
}

static void frames__encode(const frames_t *frames, void **cursor)
{
	const dqlite_vfs_frame *list;
	unsigned i;
	uint32__encode(&frames->n_pages, cursor);
	uint16__encode(&frames->page_size, cursor);
	uint16__encode(&frames->__unused__, cursor);
	list = frames->data;
	for (i = 0; i < frames->n_pages; i++) {
		uint64_t pgno = list[i].page_number;
		uint64__encode(&pgno, cursor);
	}
	for (i = 0; i < frames->n_pages; i++) {
		memcpy(*cursor, list[i].data, frames->page_size);
		*cursor += frames->page_size;
	}
}

static int frames__decode(struct cursor *cursor, frames_t *frames)
{
	int rc;
	rc = uint32__decode(cursor, &frames->n_pages);
	if (rc != 0) {
		return rc;
	}
	rc = uint16__decode(cursor, &frames->page_size);
	if (rc != 0) {
		return rc;
	}
	rc = uint16__decode(cursor, &frames->__unused__);
	if (rc != 0) {
		return rc;
	}
	frames->data = cursor->p;
	return 0;
}

#define COMMAND__IMPLEMENT(LOWER, UPPER, _) \
	SERIALIZE__IMPLEMENT(command_##LOWER, COMMAND__##UPPER);

COMMAND__TYPES(COMMAND__IMPLEMENT, );

#define ENCODE(LOWER, UPPER, _)                                 \
	case COMMAND_##UPPER:                                   \
		h.type = COMMAND_##UPPER;                       \
		buf->len = header__sizeof(&h);                  \
		buf->len += command_##LOWER##__sizeof(command); \
		buf->base = raft_malloc(buf->len);              \
		if (buf->base == NULL) {                        \
			return DQLITE_NOMEM;                    \
		}                                               \
		cursor = buf->base;                             \
		header__encode(&h, &cursor);                    \
		command_##LOWER##__encode(command, &cursor);    \
		break;

int command__encode(int type, const void *command, struct raft_buffer *buf)
{
	struct header h = {0};
	void *cursor;
	int rc = 0;
	h.format = FORMAT;
	switch (type) {
		COMMAND__TYPES(ENCODE, )
	};
	return rc;
}

#define DECODE(LOWER, UPPER, _)                                         \
	case COMMAND_##UPPER:                                           \
		*command = raft_malloc(sizeof(struct command_##LOWER)); \
		if (*command == NULL) {                                 \
			return DQLITE_NOMEM;                            \
		}                                                       \
		rc = command_##LOWER##__decode(&cursor, *command);      \
		break;

int command__decode(const struct raft_buffer *buf, int *type, void **command)
{
	struct header h;
	struct cursor cursor;
	int rc;

	cursor.p = buf->base;
	cursor.cap = buf->len;

	rc = header__decode(&cursor, &h);
	if (rc != 0) {
		return rc;
	}
	if (h.format != FORMAT) {
		return DQLITE_PROTO;
	}
	switch (h.type) {
		COMMAND__TYPES(DECODE, )
		default:
			rc = DQLITE_PROTO;
			break;
	};
	if (rc != 0) {
		return rc;
	}
	*type = h.type;
	return 0;
}

int command_frames__page_numbers(const struct command_frames *c,
				 unsigned long *page_numbers[])
{
	unsigned i;
	struct cursor cursor;

	cursor.p = c->frames.data;
	cursor.cap = sizeof(uint64_t) * c->frames.n_pages;

	*page_numbers =
	    sqlite3_malloc64(sizeof **page_numbers * c->frames.n_pages);
	if (*page_numbers == NULL) {
		return DQLITE_NOMEM;
	}

	for (i = 0; i < c->frames.n_pages; i++) {
		uint64_t pgno;
		int r = uint64__decode(&cursor, &pgno);
		if (r != 0) {
			return r;
		}
		(*page_numbers)[i] = (unsigned long)pgno;
	}

	return 0;
}

void command_frames__pages(const struct command_frames *c, void **pages)
{
	*pages =
	    (void *)(c->frames.data + (sizeof(uint64_t) * c->frames.n_pages));
}
