/* Tracing functionality for dqlite */

#ifndef DQLITE_TRACING_H_
#define DQLITE_TRACING_H_

#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>

#include "../include/dqlite.h"

#include "utils.h"

/* This global variable is only written once at startup and is only read
 * from there on. Users should not manipulate the value of this variable. */
DQLITE_VISIBLE_TO_TESTS extern bool _dqliteTracingEnabled;
DQLITE_VISIBLE_TO_TESTS void stderrTracerEmit(const char *file,
					      unsigned int line,
					      const char *func,
					      unsigned int level,
					      const char *message);

DQLITE_VISIBLE_TO_TESTS NOINLINE
void _tracef0(const char *file, unsigned int line, const char *func, unsigned int level, const char *fmt, ...);

#define tracef0(LEVEL, ...) _tracef0(__FILE__, __LINE__, __func__, LEVEL, __VA_ARGS__)

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

#define tracef(...) tracef0(TRACE_DEBUG, __VA_ARGS__)

/* Enable tracing if the appropriate env variable is set, or disable tracing. */
DQLITE_VISIBLE_TO_TESTS void dqliteTracingMaybeEnable(bool enabled);

#endif /* DQLITE_TRACING_H_ */
