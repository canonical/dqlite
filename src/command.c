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

#define COMMAND__IMPLEMENT(LOWER, UPPER, _) \
	SERIALIZATION__IMPLEMENT(command_##LOWER, COMMAND__##UPPER);

#define TYPES(X, ...) X(open, OPEN, __VA_ARGS__)

TYPES(COMMAND__IMPLEMENT, );

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

int command__encode(int type, const void *command, struct raft_buffer *buf)
{
	struct header h;
	size_t header_size;
	h.format = FORMAT;
	switch (type) {
		TYPES(ENCODE, )
	};
	return 0;
}

#define DECODE(LOWER, UPPER, _)                                               \
	case COMMAND_##UPPER:                                                 \
		*command = raft_malloc(sizeof(struct command_##LOWER));       \
		if (*command == NULL) {                                       \
			return DQLITE_NOMEM;                                  \
		}                                                             \
		command_##LOWER##__decode(buf->base + header_size, *command); \
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

int command__apply(struct raft *raft,
		   int type,
		   const void *command,
		   void *data,
		   raft_apply_cb cb)
{
	struct raft_apply *req;
	struct raft_buffer buf;
	int rc;

	req = raft_malloc(sizeof *req);
	if (req == NULL) {
		rc = DQLITE_NOMEM;
		goto err;
	}
	req->data = data;

	rc = command__encode(type, command, &buf);
	if (rc != 0) {
		goto err_after_req_alloc;
	}

	rc = raft_apply(raft, req, &buf, 1, cb);
	if (rc != 0) {
		goto err_after_command_encode;
	}

	return 0;

err_after_command_encode:
	raft_free(buf.base);
err_after_req_alloc:
	raft_free(req);
err:
	return rc;
}
