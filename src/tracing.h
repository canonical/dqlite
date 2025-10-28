/* Tracing functionality for dqlite */

#ifndef DQLITE_TRACING_H_
#define DQLITE_TRACING_H_

#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
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

DQLITE_VISIBLE_TO_TESTS DQLITE_NOINLINE DQLITE_PRINTF(5, 6)
void dqlite_tracef(const char *file, unsigned int line, const char *func, unsigned int level, const char *fmt, ...);

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

/* Enable tracing if the appropriate env variable is set, or disable tracing. */
DQLITE_VISIBLE_TO_TESTS void dqliteTracingMaybeEnable(bool enabled);

struct trace_def {
	const char *file;
	unsigned int line;
	const char *func;
	const char *fmt;
};

enum trace_arg_type {
	TRACE_ARG_STR,
	TRACE_ARG_PTR,
	TRACE_ARG_U8,
	TRACE_ARG_U16,
	TRACE_ARG_U32,
	TRACE_ARG_U64,
	TRACE_ARG_I8,
	TRACE_ARG_I16,
	TRACE_ARG_I32,
	TRACE_ARG_I64,
};

union trace_arg_value {
	uint8_t u8;
	uint16_t u16;
	uint32_t u32;
	uint64_t u64;
	size_t usize;
	int8_t i8;
	int16_t i16;
	int32_t i32;
	int64_t i64;
	char str[15];
	void *ptr;
} DQLITE_PACKED;

struct trace_arg {
	uint8_t type;
	union trace_arg_value value;
} DQLITE_PACKED;

DQLITE_API void dqlite_crash_trace(const struct trace_def *trace_def,
			const size_t argc,
			const struct trace_arg *argv);

static inline struct trace_arg dqlite_i8_arg(int8_t value)
{
	return (struct trace_arg){
		.type = TRACE_ARG_I8,
		.value = { .i8 = value },
	};
}

static inline struct trace_arg dqlite_i16_arg(int16_t value)
{
	return (struct trace_arg){
		.type = TRACE_ARG_I16,
		.value = { .i16 = value },
	};
}

static inline struct trace_arg dqlite_i32_arg(int32_t value)
{
	return (struct trace_arg){
		.type = TRACE_ARG_I32,
		.value = { .i32 = value },
	};
}

static inline struct trace_arg dqlite_i64_arg(int64_t value)
{
	return (struct trace_arg){
		.type = TRACE_ARG_I64,
		.value = { .i64 = value },
	};
}
static inline struct trace_arg dqlite_u8_arg(uint8_t value)
{
	return (struct trace_arg){
		.type = TRACE_ARG_U8,
		.value = { .u8 = value },
	};
}

static inline struct trace_arg dqlite_u16_arg(uint16_t value)
{
	return (struct trace_arg){
		.type = TRACE_ARG_U16,
		.value = { .u16 = value },
	};
}

static inline struct trace_arg dqlite_u32_arg(uint32_t value)
{
	return (struct trace_arg){
		.type = TRACE_ARG_U32,
		.value = { .u32 = value },
	};
}

static inline struct trace_arg dqlite_u64_arg(uint64_t value)
{
	return (struct trace_arg){
		.type = TRACE_ARG_U64,
		.value = { .u64 = value },
	};
}

static inline struct trace_arg dqlite_string_arg(const char *str)
{
	struct trace_arg arg = { .type = TRACE_ARG_STR };
#pragma GCC diagnostic push
#if (defined(__GNUC__) && !defined(__clang__))
# pragma GCC diagnostic ignored "-Wstringop-truncation"
#endif
	strncpy(arg.value.str, str, sizeof(arg.value.str)-1);
	arg.value.str[sizeof(arg.value.str)-1] = '\0';
#pragma GCC diagnostic pop
	return arg;
}

static inline struct trace_arg dqlite_pointer_arg(const void *ptr)
{
	return (struct trace_arg){
		.type = TRACE_ARG_PTR,
		.value = { .ptr = (void *)ptr },
	};
}

#define TRACE_ARG(X)                            \
	_Generic((X),                           \
	    bool: dqlite_i32_arg,               \
	    int8_t: dqlite_i8_arg,              \
	    uint8_t: dqlite_u8_arg,             \
	    int16_t: dqlite_i16_arg,            \
	    uint16_t: dqlite_u16_arg,           \
	    int32_t: dqlite_i32_arg,            \
	    uint32_t: dqlite_u32_arg,           \
	    int64_t: dqlite_i64_arg,            \
	    uint64_t: dqlite_u64_arg,           \
	    const char *: dqlite_string_arg,    \
	    char *: dqlite_string_arg,          \
	    const void *: dqlite_pointer_arg,   \
	    void *: dqlite_pointer_arg)((X))

#define TRACE_MAX_ARGS 9
#define TRACE_ARGS_1(_)
#define TRACE_ARGS_2(_, X) TRACE_ARG(X)
#define TRACE_ARGS_3(_, X, Y) TRACE_ARG(X), TRACE_ARG(Y)
#define TRACE_ARGS_4(_, X, ...) TRACE_ARG(X), TRACE_ARGS_3(_, __VA_ARGS__)
#define TRACE_ARGS_5(_, X, ...) TRACE_ARG(X), TRACE_ARGS_4(_, __VA_ARGS__)
#define TRACE_ARGS_6(_, X, ...) TRACE_ARG(X), TRACE_ARGS_5(_, __VA_ARGS__)
#define TRACE_ARGS_7(_, X, ...) TRACE_ARG(X), TRACE_ARGS_6(_, __VA_ARGS__)
#define TRACE_ARGS_8(_, X, ...) TRACE_ARG(X), TRACE_ARGS_7(_, __VA_ARGS__)
#define TRACE_ARGS_9(_, X, ...) TRACE_ARG(X), TRACE_ARGS_8(_, __VA_ARGS__)
#define TRACE_ARGS_10(_, X, ...) TRACE_ARG(X), TRACE_ARGS_9(_, __VA_ARGS__)
#define TRACE_ARGS_N(N, ...) MACRO_CAT(TRACE_ARGS_, N)(__VA_ARGS__)

#define TRACE_ARGS(...)  \
  MACRO_CAT(TRACE_ARGS_, COUNT_ARGS(__VA_ARGS__)) (__VA_ARGS__)

#define TRACE_FORMAT(FORMAT, ...) FORMAT

#define CRASH_TRACE(...)                                                     \
	do {                                                                 \
		static const struct trace_def crash_trace_def = {            \
			.file = __FILE__,                                    \
			.line = __LINE__,                                    \
			.func = __func__,                                    \
			.fmt = TRACE_FORMAT(__VA_ARGS__),                    \
		};                                                           \
		const struct trace_arg crash_trace_argv[] = { TRACE_ARGS(    \
		    __VA_ARGS__) };                                          \
		dqlite_crash_trace(&crash_trace_def, \
		    sizeof(crash_trace_argv) / sizeof(crash_trace_argv[0]),  \
		    crash_trace_argv);                                       \
	} while (0)

#define tracef0(LEVEL, ...)                                                \
	do {                                                               \
		if (UNLIKELY(_dqliteTracingEnabled)) {                     \
			dqlite_tracef(__FILE__, __LINE__, __func__, LEVEL, \
				      __VA_ARGS__);                        \
		}                                                          \
		CRASH_TRACE(__VA_ARGS__);                                  \
	} while (0)

#define tracef(...) tracef0(TRACE_DEBUG, __VA_ARGS__)

#endif /* DQLITE_TRACING_H_ */
