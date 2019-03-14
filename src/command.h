#ifndef COMMAND_H_
#define COMMAND_H_

#include <raft.h>

#include "./lib/serialization.h"

/* Command type codes */
enum { COMMAND_OPEN = 1 };

/* Serialization definitions for a raft FSM command. */
#define COMMAND__DEFINE(LOWER, UPPER, _) \
	SERIALIZATION__DEFINE_STRUCT(command_##LOWER, COMMAND__##UPPER);

#define COMMAND__OPEN(X, ...) X(text, filename, ##__VA_ARGS__)

#define COMMAND__TYPES(X, ...) X(open, OPEN, __VA_ARGS__)

COMMAND__TYPES(COMMAND__DEFINE);

int command__encode(int type, const void *command, struct raft_buffer *buf);

int command__decode(const struct raft_buffer *buf, int *type, void **command);

int command__apply(struct raft *raft,
		   int type,
		   const void *command,
		   void *data,
		   raft_apply_cb cb);

#endif /* COMMAND_H_*/
