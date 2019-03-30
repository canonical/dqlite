#include "../include/dqlite.h"

#include "./lib/serialize.h"

#include "command.h"

#define FORMAT 1 /* Format version */

#define HEADER(X, ...)                    \
	X(uint8, format, ##__VA_ARGS__)   \
	X(uint8, type, ##__VA_ARGS__)     \
	X(uint8, _unused1, ##__VA_ARGS__) \
	X(uint8, _unused2, ##__VA_ARGS__) \
	X(uint32, _unused3, ##__VA_ARGS__)

SERIALIZE__DEFINE(header, HEADER);
SERIALIZE__IMPLEMENT(header, HEADER);

static size_t frames__sizeof(frames_t frames)
{
	size_t s = uint32__sizeof(frames.n_pages) +
		   uint16__sizeof(frames.page_size) +
		   uint16__sizeof(frames.__unused__) +
		   uint64__sizeof(0) * frames.n_pages + /* Page numbers */
		   frames.page_size * frames.n_pages;   /* Page data */
	return s;
}

static void frames__encode(frames_t frames, void **cursor)
{
	const sqlite3_wal_replication_frame *list;
	unsigned i;
	uint32__encode(frames.n_pages, cursor);
	uint16__encode(frames.page_size, cursor);
	uint16__encode(0, cursor);
	list = frames.data;
	for (i = 0; i < frames.n_pages; i++) {
		uint64__encode(list[i].pgno, cursor);
	}
	for (i = 0; i < frames.n_pages; i++) {
		memcpy(*cursor, list[i].pBuf, frames.page_size);
		*cursor += frames.page_size;
	}
}

static void frames__decode(const void **cursor, frames_t *frames)
{
	uint32__decode(cursor, &frames->n_pages);
	uint16__decode(cursor, &frames->page_size);
	uint16__decode(cursor, &frames->__unused__);
	frames->data = *cursor;
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
	struct header h;
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
		command_##LOWER##__decode(&cursor, *command);           \
		break;

int command__decode(const struct raft_buffer *buf, int *type, void **command)
{
	struct header h;
	const void *cursor = buf->base;
	int rc = 0;
	header__decode(&cursor, &h);
	if (h.format != FORMAT) {
		return DQLITE_PROTO;
	}
	switch (h.type) {
		COMMAND__TYPES(DECODE, )
		default:
			return DQLITE_PROTO;
	};
	*type = h.type;
	return rc;
}

int command_frames__page_numbers(const struct command_frames *c,
				 unsigned *page_numbers[])
{
	unsigned i;
	const void *cursor;

	*page_numbers =
	    sqlite3_malloc(sizeof **page_numbers * c->frames.n_pages);
	if (*page_numbers == NULL) {
		return DQLITE_NOMEM;
	}

	cursor = c->frames.data;
	for (i = 0; i < c->frames.n_pages; i++) {
		uint64_t pgno;
		uint64__decode(&cursor, &pgno);
		(*page_numbers)[i] = pgno;
	}

	return 0;
}

void command_frames__pages(const struct command_frames *c, void **pages)
{
	*pages =
	    (void *)(c->frames.data + (sizeof(uint64_t) * c->frames.n_pages));
}
