#include <stdio.h>
#include <string.h>

#include "logger.h"

#define EMIT_BUF_LEN 1024

void loggerDefaultEmit(void *data, int level, const char *fmt, va_list args)
{
	char buf[EMIT_BUF_LEN];
	char *cursor = buf;
	int n;

	(void)data;

	/* First, render the logging level. */
	switch (level) {
		case DQLITE_DEBUG:
			sprintf(cursor, "[DEBUG]: ");
			break;
		case DQLITE_INFO:
			sprintf(cursor, "[INFO ]: ");
			break;
		case DQLITE_WARN:
			sprintf(cursor, "[WARN ]: ");
			break;
		case DQLITE_ERROR:
			sprintf(cursor, "[ERROR]: ");
			break;
		default:
			sprintf(cursor, "[     ]: ");
			break;
	};

	cursor = buf + strlen(buf);

	/* Then render the message, possibly truncating it. */
	n = EMIT_BUF_LEN - strlen(buf) - 1;
	vsnprintf(cursor, n, fmt, args);

	fprintf(stderr, "%s\n", buf);
}

void loggerRaftEmit(struct raft_logger *l,
		    int level,
		    raft_time time,
		    const char *file,
		    int line,
		    const char *format,
		    ...)
{
	struct logger *logger = l->impl;
	va_list args;

	(void)file;
	(void)line;
	(void)time;

	/* TODO: properly setup raft logging */
	return;

	va_start(args, format);
	logger->emit(logger->data, level, format, args);
	va_end(args);
}
