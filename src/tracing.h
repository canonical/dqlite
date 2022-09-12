/* Tracing functionality for dqlite */

#ifndef DQLITE_TRACING_H_
#define DQLITE_TRACING_H_

#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <time.h>

#include "utils.h"

/* This global variable is only written once at startup and is only read
 * from there on. Users should not manipulate the value of this variable. */
extern bool _dqliteTracingEnabled;

#define tracef(...) do {                                                                   \
    if (UNLIKELY(_dqliteTracingEnabled)) {                                                 \
        static char _msg[1024];                                                            \
        snprintf(_msg, sizeof(_msg), __VA_ARGS__);                                         \
        struct timespec ts = {0};                                                          \
        /* Ignore errors */                                                                \
        clock_gettime(CLOCK_REALTIME, &ts);                                                \
        int64_t ns = ts.tv_sec * 1000000000 + ts.tv_nsec;                                  \
        fprintf(stderr, "LIBDQLITE %" PRId64 " %s:%d %s\n", ns, __func__, __LINE__, _msg); \
    }                                                                                      \
} while(0)                                                                                 \

/* Enable tracing if the appropriate env variable is set, or disable tracing. */
void dqliteTracingMaybeEnable(bool enabled);

#endif /* DQLITE_TRACING_H_ */
