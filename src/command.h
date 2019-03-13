#ifndef COMMAND_H_
#define COMMAND_H_

#include <raft.h>

#include "./lib/serialization.h"

/* Command type codes */
enum { COMMAND__OPEN = 1 };

/* Serialization definitions for a raft FSM command. */
#define COMMAND__DEFINE(LOWER, UPPER, _)                                   \
	SERIALIZATION__DEFINE_STRUCT(command__##LOWER, COMMAND___##UPPER); \
	int command__encode_##LOWER(struct command__##LOWER *c,            \
				    struct raft_buffer *buf);

#define COMMAND___OPEN(X, ...) X(text, filename, ##__VA_ARGS__)

#define COMMAND__TYPES(X, ...) X(open, OPEN, __VA_ARGS__)

COMMAND__TYPES(COMMAND__DEFINE);

int command__decode(const struct raft_buffer *buf, int *type, void **command);

#endif /* COMMAND_H_*/
