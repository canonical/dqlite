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
#define COMMAND__UNDO(X, ...) X(uint64, tx_id, ##__VA_ARGS__)

#define COMMAND__TYPES(X, ...)     \
	X(open, OPEN, __VA_ARGS__) \
	X(undo, UNDO, __VA_ARGS__)

COMMAND__TYPES(COMMAND__DEFINE);

/* The frames command is implemented by hand since it has dynamic data */
struct command_frames
{
	text_t filename;
	uint64_t tx_id;
	uint32_t truncate;
	uint16_t page_size;
	uint8_t is_commit;
	uint8_t _unused;
	uint64_t n_pages;
	const void *data; /* Format differs between encode and decode */
};

int command__encode(int type, const void *command, struct raft_buffer *buf);

int command__decode(const struct raft_buffer *buf, int *type, void **command);

int command_frames__page_numbers(const struct command_frames *c,
				 unsigned *page_numbers[]);

void command_frames__pages(const struct command_frames *c, void **pages);

#endif /* COMMAND_H_*/
