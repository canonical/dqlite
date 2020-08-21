/**
 * Encode and decode dqlite Raft FSM commands.
 */

#ifndef COMMAND_H_
#define COMMAND_H_

#include <raft.h>

#include "lib/serialize.h"

/* Command type codes */
enum { COMMAND_OPEN = 1, COMMAND_FRAMES, COMMAND_UNDO, COMMAND_CHECKPOINT };

/* Hold information about an array of WAL frames. */
struct frames
{
	uint32_t n_pages;
	uint16_t page_size;
	uint16_t __unused__;
	/* TODO: because the sqlite3 replication APIs are asymmetrics, the
	 * format differs between encode and decode. When encoding data is
	 * expected to be a sqlite3_wal_replication_frame* array, and when
	 * decoding it will be a pointer to raw memory which can be further
	 * decoded with the command_frames__page_numbers() and
	 * command_frames__pages() helpers. */
	const void *data;
};

typedef struct frames frames_t;

/* Serialization definitions for a raft FSM command. */
#define COMMAND__DEFINE(LOWER, UPPER, _) \
	SERIALIZE__DEFINE_STRUCT(command_##LOWER, COMMAND__##UPPER);

#define COMMAND__OPEN(X, ...) X(text, filename, ##__VA_ARGS__)
#define COMMAND__FRAMES(X, ...)               \
	X(text, filename, ##__VA_ARGS__)      \
	X(uint64, tx_id, ##__VA_ARGS__)       \
	X(uint32, truncate, ##__VA_ARGS__)    \
	X(uint8, is_commit, ##__VA_ARGS__)    \
	X(uint8, __unused1__, ##__VA_ARGS__)  \
	X(uint16, __unused2__, ##__VA_ARGS__) \
	X(frames, frames, ##__VA_ARGS__)
#define COMMAND__UNDO(X, ...) X(uint64, tx_id, ##__VA_ARGS__)
#define COMMAND__CHECKPOINT(X, ...) X(text, filename, ##__VA_ARGS__)

#define COMMAND__TYPES(X, ...)         \
	X(open, OPEN, __VA_ARGS__)     \
	X(frames, FRAMES, __VA_ARGS__) \
	X(undo, UNDO, __VA_ARGS__)     \
	X(checkpoint, CHECKPOINT, __VA_ARGS__)

COMMAND__TYPES(COMMAND__DEFINE);

int command__encode(int type, const void *command, struct raft_buffer *buf);

int command__decode(const struct raft_buffer *buf, int *type, void **command);

int command_frames__page_numbers(const struct command_frames *c,
				 unsigned long *page_numbers[]);

void command_frames__pages(const struct command_frames *c, void **pages);

#endif /* COMMAND_H_*/
