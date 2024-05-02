/* Tracing functionality for dqlite */

#ifndef DQLITE_TRACING_H_
#define DQLITE_TRACING_H_

#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <time.h>

#include "../include/dqlite.h"

#include "utils.h"

/* This global variable is only written once at startup and is only read
 * from there on. Users should not manipulate the value of this variable. */
DQLITE_VISIBLE_TO_TESTS extern bool _dqliteTracingEnabled;
DQLITE_VISIBLE_TO_TESTS void stderrTracerEmit(const char *file,
					      int line,
					      const char *func,
					      int level,
					      const char *message);

DQLITE_VISIBLE_TO_TESTS void tracef0(int level, const char *file, int line, const char *func, const char *fmt, ...);

enum dqlite_trace_level {
	/** Represents an invalid trace level */
	TRACE_NONE,
	/** Lower-level information to debug and analyse incorrect behavior */
	TRACE_DEBUG,
	/** Information about current system's state */
	TRACE_INFO,
	/**
	 * Condition which requires a special handling, something which doesn't
	 * happen normally
	 */
	TRACE_WARN,
	/** Resource unavailable, no connectivity, invalid value, etc. */
	TRACE_ERROR,
	/** System is not able to continue performing its basic function */
	TRACE_FATAL,
	TRACE_NR,
};

#define tracef(fmt, ...) tracef0(TRACE_DEBUG, __FILE__, __LINE__, __func__, (fmt), ##__VA_ARGS__)

/* Enable tracing if the appropriate env variable is set, or disable tracing. */
DQLITE_VISIBLE_TO_TESTS void dqliteTracingMaybeEnable(bool enabled);

#endif /* DQLITE_TRACING_H_ */
