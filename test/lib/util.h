/**
 * Utility macros and functions.
 */

#ifndef TEST_UTIL_H
#define TEST_UTIL_H

#include "munit.h"

#include <time.h>

/* Wait a bounded time in seconds until a condition is true. */
#define AWAIT_TRUE(FN, ARG, SEC)                                            \
	do {                                                                \
		struct timespec _start = {0};                               \
		struct timespec _end = {0};                                 \
		clock_gettime(CLOCK_MONOTONIC, &_start);                    \
		clock_gettime(CLOCK_MONOTONIC, &_end);                      \
		while (!FN(ARG) && ((_end.tv_sec - _start.tv_sec) < SEC)) { \
			clock_gettime(CLOCK_MONOTONIC, &_end);              \
		}                                                           \
		if (!FN(ARG)) {                                             \
			return MUNIT_FAIL;                                  \
		}                                                           \
	} while (0)

static inline bool param_bool(const MunitParameter *params, const char *name)
{
	const char *param = munit_parameters_get(params, name);
	return param != NULL && (bool)atoi(param);
}

#endif /* TEST_UTIL_H */
