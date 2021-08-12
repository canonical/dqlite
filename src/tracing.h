/* Tracing functionality for dqlite */

#ifndef DQLITE_TRACING_H_
#define DQLITE_TRACING_H_

#include <stdbool.h>

/* This global variable is only written once at startup and is only read
 * from there on. Users should not manipulate the value of this variable. */
extern bool _dqliteTracingEnabled;

#define tracef(...) do {                                                   \
    if (_dqliteTracingEnabled) {                                           \
	static char _msg[1024];                                            \
	snprintf(_msg, sizeof(_msg), __VA_ARGS__);                         \
	fprintf(stderr, "LIBDQLITE %s:%d %s\n", __func__, __LINE__, _msg); \
    }                                                                      \
} while(0)                                                                 \

/* Enable tracing if the appropriate env variable is set, or disable tracing. */
void dqliteTracingMaybeEnable(bool enabled);

#endif /* DQLITE_TRACING_H_ */
