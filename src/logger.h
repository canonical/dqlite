#ifndef LOGGER_H_
#define LOGGER_H_

#include "../include/dqlite.h"

struct logger
{
	void *data;
	void (*emit)(void *data, int level, const char *fmt, va_list args);
};

void logger__emit(struct logger *l, int level, const char *fmt, ...);

/**
 * Emit a log message with a certain level.
 */
/* #define debugf(L, FORMAT, ...) \ */
/* 	logger__emit(L, DQLITE_DEBUG, FORMAT, ##__VA_ARGS__) */
#define debugf(C, FORMAT, ...) \
	C->gateway.raft->io->emit(C->gateway.raft->io, RAFT_DEBUG, FORMAT, ##__VA_ARGS__)

#endif /* LOGGER_H_ */
