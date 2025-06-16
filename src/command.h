/**
 * Encode and decode dqlite Raft FSM commands.
 */

#ifndef COMMAND_H_
#define COMMAND_H_

#include "../include/dqlite.h"

#include "lib/serialize.h"
#include "raft.h"

/* Command type codes */
enum { COMMAND_OPEN = 1, COMMAND_FRAMES, COMMAND_UNDO, COMMAND_CHECKPOINT };

/* Hold information about an array of WAL frames. */
struct frames
{
	uint32_t   n_pages;
	uint16_t   page_size;
	uint16_t   __unused__;
	uint64_t  *page_numbers;
	void     **pages;
};

typedef struct frames frames_t;

/* Serialization definitions for a raft FSM command. */
#define COMMAND__DEFINE(LOWER, UPPER, _) \
	SERIALIZE__DEFINE_STRUCT(command_##LOWER, COMMAND__##UPPER);

#define COMMAND__FRAMES(X, ...)               \
	X(text, filename, ##__VA_ARGS__)      \
	X(uint64, tx_id, ##__VA_ARGS__)       \
	X(uint32, truncate, ##__VA_ARGS__)    \
	X(uint8, is_commit, ##__VA_ARGS__)    \
	X(uint8, __unused1__, ##__VA_ARGS__)  \
	X(uint16, __unused2__, ##__VA_ARGS__) \
	X(frames, frames, ##__VA_ARGS__)

/* These commands are not used and are no-ops for now. */
#define COMMAND__OPEN(X, ...) X(text, filename, ##__VA_ARGS__)
#define COMMAND__UNDO(X, ...) X(uint64, tx_id, ##__VA_ARGS__)
#define COMMAND__CHECKPOINT(X, ...) X(text, filename, ##__VA_ARGS__)

#define COMMAND__TYPES(X, ...)         \
	X(open, OPEN, __VA_ARGS__)     \
	X(frames, FRAMES, __VA_ARGS__) \
	X(undo, UNDO, __VA_ARGS__)     \
	X(checkpoint, CHECKPOINT, __VA_ARGS__)

COMMAND__TYPES(COMMAND__DEFINE);

DQLITE_VISIBLE_TO_TESTS int command__encode(int type,
					    const void *command,
					    struct raft_buffer *buf);

DQLITE_VISIBLE_TO_TESTS int command__decode(const struct raft_buffer *buf,
					    int *type,
					    void **command);


#endif /* COMMAND_H_*/
