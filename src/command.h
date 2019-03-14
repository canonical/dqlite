#ifndef COMMAND_H_
#define COMMAND_H_

#include <raft.h>

#include "./lib/serialization.h"

/* Command type codes */
enum { COMMAND_OPEN = 1, COMMAND_FRAMES, COMMAND_UNDO };

/* Serialization definitions for a raft FSM command. */
#define COMMAND__DEFINE(LOWER, UPPER, _) \
	SERIALIZATION__DEFINE_STRUCT(command_##LOWER, COMMAND__##UPPER);

#define COMMAND__OPEN(X, ...) X(text, filename, ##__VA_ARGS__)
#define COMMAND__FRAMES(X, ...)             \
	X(uint64, tx_id, ##__VA_ARGS__)     \
	X(uint32, truncate, ##__VA_ARGS__)  \
	X(uint16, page_size, ##__VA_ARGS__) \
	X(uint8, is_commit, ##__VA_ARGS__)  \
	X(uint8, _unused, ##__VA_ARGS__)    \
	X(text, filename, ##__VA_ARGS__)
#define COMMAND__UNDO(X, ...) X(uint64, tx_id, ##__VA_ARGS__)

#define COMMAND__TYPES(X, ...)         \
	X(open, OPEN, __VA_ARGS__)     \
	X(frames, FRAMES, __VA_ARGS__) \
	X(undo, UNDO, __VA_ARGS__)

COMMAND__TYPES(COMMAND__DEFINE);

int command__encode(int type, const void *command, struct raft_buffer *buf);

int command__decode(const struct raft_buffer *buf, int *type, void **command);

#endif /* COMMAND_H_*/
