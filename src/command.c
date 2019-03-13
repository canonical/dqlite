#include <sqlite3.h>

#include "../include/dqlite.h"

#include "./lib/serialization.h"

#include "command.h"

#define FORMAT 1 /* Format version */

#define HEADER(X, ...)                    \
	X(uint8, format, ##__VA_ARGS__)   \
	X(uint8, type, ##__VA_ARGS__)     \
	X(uint8, _unused1, ##__VA_ARGS__) \
	X(uint8, _unused2, ##__VA_ARGS__) \
	X(uint32, _unused3, ##__VA_ARGS__)

SERIALIZATION__DEFINE(header, HEADER);
SERIALIZATION__IMPLEMENT(header, HEADER);

#define COMMAND__IMPLEMENT(LOWER, UPPER, _)                             \
	SERIALIZATION__IMPLEMENT(command__##LOWER, COMMAND___##UPPER);  \
	int command__encode_##LOWER(struct command__##LOWER *c,         \
				    struct raft_buffer *buf)            \
	{                                                               \
		struct header h;                                        \
		size_t header_size = header__sizeof(&h);                \
		buf->len = header__sizeof(&h);                          \
		buf->len += command__##LOWER##__sizeof(c);              \
		buf->base = sqlite3_malloc(buf->len);                   \
		if (buf->base == NULL) {                                \
			return DQLITE_NOMEM;                            \
		}                                                       \
		h.format = FORMAT;                                      \
		h.type = COMMAND__##UPPER;                              \
		header__encode(&h, buf->base);                          \
		command__##LOWER##__encode(c, buf->base + header_size); \
		return 0;                                               \
	}

#define TYPES(X, ...) X(open, OPEN, __VA_ARGS__)

TYPES(COMMAND__IMPLEMENT, );

#define DECODE(LOWER, UPPER, _)                                                \
	case COMMAND__##UPPER:                                                 \
		*command = sqlite3_malloc(sizeof(struct command__##LOWER));    \
		if (*command == NULL) {                                        \
			return DQLITE_NOMEM;                                   \
		}                                                              \
		command__##LOWER##__decode(buf->base + header_size, *command); \
		break;

int command__decode(const struct raft_buffer *buf, int *type, void **command)
{
	struct header h;
	size_t header_size;
	header__decode(buf->base, &h);
	if (h.format != FORMAT) {
		return DQLITE_PROTO;
	}
	header_size = header__sizeof(&h);
	switch (h.type) {
		TYPES(DECODE, )
		default:
			return DQLITE_PROTO;
	};
	*type = h.type;
	return 0;
}
