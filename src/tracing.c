#include "tracing.h"

#include <stdlib.h>

#define LIBDQLITE_TRACE "LIBDQLITE_TRACE"

bool _dqliteTracingEnabled = false;

void dqliteTracingMaybeEnable(bool enable)
{
        if (getenv(LIBDQLITE_TRACE) != NULL) {
                _dqliteTracingEnabled = enable;
        }
}
