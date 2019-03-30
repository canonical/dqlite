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

#define COMMAND__IMPLEMENT(LOWER, UPPER, _) \
	SERIALIZE__IMPLEMENT(command_##LOWER, COMMAND__##UPPER);

COMMAND__TYPES(COMMAND__IMPLEMENT, );

#define ENCODE(LOWER, UPPER, _)                                              \
	case COMMAND_##UPPER:                                                \
		h.type = COMMAND_##UPPER;                                    \
		header_size = header__sizeof(&h);                            \
		buf->len = header_size;                                      \
		buf->len += command_##LOWER##__sizeof(command);              \
		buf->base = raft_malloc(buf->len);                           \
		if (buf->base == NULL) {                                     \
			return DQLITE_NOMEM;                                 \
		}                                                            \
		header__encode(&h, buf->base);                               \
		command_##LOWER##__encode(command, buf->base + header_size); \
		break;

static int command__encode_frames(const struct command_frames *c,
				  struct raft_buffer *buf)
{
	struct header h;
	size_t header_size;
	void *cursor;
	const sqlite3_wal_replication_frame *frames;
	unsigned i;

	h.format = FORMAT;
	h.type = COMMAND_FRAMES;
	header_size = header__sizeof(&h);
	buf->len = header_size;

	/* Fixed size part */
	buf->len += text__sizeof(c->filename);
	buf->len += uint64__sizeof(c->tx_id);
	buf->len += uint32__sizeof(c->truncate);
	buf->len += uint16__sizeof(c->page_size);
	buf->len += uint8__sizeof(c->is_commit);
	buf->len += uint8__sizeof(c->_unused);
	buf->len += uint64__sizeof(c->n_pages);

	/* Dynamic size part */
	buf->len += uint64__sizeof(0) * c->n_pages;
	buf->len += c->page_size * c->n_pages;
	buf->base = raft_malloc(buf->len);
	if (buf->base == NULL) {
		return DQLITE_NOMEM;
	}
	header__encode(&h, buf->base);

	cursor = buf->base + header_size;
	text__encode(c->filename, &cursor);
	uint64__encode(c->tx_id, &cursor);
	uint32__encode(c->truncate, &cursor);
	uint16__encode(c->page_size, &cursor);
	uint8__encode(c->is_commit, &cursor);
	uint8__encode(c->_unused, &cursor);
	uint64__encode(c->n_pages, &cursor);

	frames = c->data;

	for (i = 0; i < c->n_pages; i++) {
		uint64__encode(frames[i].pgno, &cursor);
	}
	for (i = 0; i < c->n_pages; i++) {
		memcpy(cursor, frames[i].pBuf, c->page_size);
		cursor += c->page_size;
	}

	return 0;
}

int command__encode(int type, const void *command, struct raft_buffer *buf)
{
	struct header h;
	size_t header_size;
	int rc = 0;
	h.format = FORMAT;
	switch (type) {
		COMMAND__TYPES(ENCODE, )
		case COMMAND_FRAMES:
			rc = command__encode_frames(command, buf);
			break;
	};
	return rc;
}

#define DECODE(LOWER, UPPER, _)                                               \
	case COMMAND_##UPPER:                                                 \
		*command = raft_malloc(sizeof(struct command_##LOWER));       \
		if (*command == NULL) {                                       \
			return DQLITE_NOMEM;                                  \
		}                                                             \
		command_##LOWER##__decode(buf->base + header_size, *command); \
		break;

static int command__decode_frames(const struct raft_buffer *buf, void **command)
{
	struct command_frames *c;
	struct header h;
	const void *cursor;

	c = raft_malloc(sizeof *c);
	if (c == NULL) {
		return DQLITE_NOMEM;
	}

	cursor = buf->base + header__sizeof(&h);
	text__decode(&cursor, &c->filename);
	uint64__decode(&cursor, &c->tx_id);
	uint32__decode(&cursor, &c->truncate);
	uint16__decode(&cursor, &c->page_size);
	uint8__decode(&cursor, &c->is_commit);
	uint8__decode(&cursor, &c->_unused);
	uint64__decode(&cursor, &c->n_pages);
	c->data = cursor;

	*command = c;

	return 0;
}

int command__decode(const struct raft_buffer *buf, int *type, void **command)
{
	struct header h;
	size_t header_size;
	int rc = 0;
	header__decode(buf->base, &h);
	if (h.format != FORMAT) {
		return DQLITE_PROTO;
	}
	header_size = header__sizeof(&h);
	switch (h.type) {
		COMMAND__TYPES(DECODE, )
		case COMMAND_FRAMES:
			rc = command__decode_frames(buf, command);
			break;
		default:
			return DQLITE_PROTO;
	};
	*type = h.type;
	return rc;
}

int command_frames__page_numbers(const struct command_frames *c, unsigned *page_numbers[])
{
	unsigned i;
	const void *cursor;

	*page_numbers = sqlite3_malloc(sizeof **page_numbers * c->n_pages);
	if (*page_numbers == NULL) {
		return DQLITE_NOMEM;
	}

	cursor = c->data;
	for (i = 0; i < c->n_pages; i++) {
		uint64_t pgno;
		uint64__decode(&cursor, &pgno);
		(*page_numbers)[i] = pgno;
	}

	return 0;
}

void command_frames__pages(const struct command_frames *c, void **pages) {
	*pages = (void*)(c->data + (sizeof(uint64_t) * c->n_pages));
}
