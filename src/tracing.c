#include <stdatomic.h>
#include <stdio.h> /* stderr */
#include <stdint.h>
#include <stdlib.h>
#include <string.h>      /* strstr, strlen */
#include <sys/syscall.h> /* syscall */
#include <sys/types.h>
#include <unistd.h>      /* syscall, getpid */

#include "tracing.h"
#include "lib/assert.h"
#include "lib/byte.h"    /* ARRAY_SIZE */

#define LIBDQLITE_TRACE "LIBDQLITE_TRACE"

bool _dqliteTracingEnabled = false;
static unsigned tracer__level;
static pid_t tracerPidCached;

void dqliteTracingMaybeEnable(bool enable)
{
	const char *trace_level = getenv(LIBDQLITE_TRACE);

	if (trace_level != NULL) {
		tracerPidCached = getpid();
		_dqliteTracingEnabled = enable;

		tracer__level = (unsigned)atoi(trace_level);
		tracer__level =
		    tracer__level < TRACE_NR ? tracer__level : TRACE_NONE;
	}
}

static inline const char *tracerShortFileName(const char *fname)
{
	static const char top_src_dir[] = "dqlite/";
	const char *p;

	p = strstr(fname, top_src_dir);
	return p != NULL ? p + strlen(top_src_dir) : fname;
}

static inline const char *tracerTraceLevelName(unsigned int level)
{
	static const char *levels[] = {
	    "NONE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL",
	};

	return level < ARRAY_SIZE(levels) ? levels[level] : levels[0];
}

static pid_t tracerPidCached;

/* NOTE: on i386 and other platforms there're no specifically imported gettid()
   functions in unistd.h
*/
static inline pid_t gettidImpl(void)
{
	return (pid_t)syscall(SYS_gettid);
}

static inline void tracerEmit(const char *file,
			      unsigned int line,
			      const char *func,
			      unsigned int level,
			      const char *message)
{
	struct timespec ts = {0};
	struct tm tm;
	pid_t tid = gettidImpl();


	clock_gettime(CLOCK_REALTIME, &ts);
	gmtime_r(&ts.tv_sec, &tm);

	/*
	  Example:
	  LIBDQLITE[182942] 2023-11-27T14:46:24.912050507 001132 INFO
	  uvClientSend  src/uv_send.c:218 connection available...
	*/
	fprintf(stderr,
		"LIBDQLITE[%6.6u] %04d-%02d-%02dT%02d:%02d:%02d.%09lu "
		"%6.6u %-7s %-20s %s:%-3i %s\n",
		tracerPidCached,

		tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour,
		tm.tm_min, tm.tm_sec, (unsigned long)ts.tv_nsec,

		(unsigned)tid, tracerTraceLevelName(level), func,
		tracerShortFileName(file), line, message);
}

void stderrTracerEmit(const char *file,
		      unsigned int line,
		      const char *func,
		      unsigned int level,
		      const char *message)
{
	dqlite_assert(tracer__level < TRACE_NR);

	if (level >= tracer__level)
		tracerEmit(file, line, func, level, message);
}

void dqlite_tracef(const char *file, unsigned int line, const char *func, unsigned int level, const char *fmt, ...)
{
	va_list args;
	char msg[1024];
	va_start(args, fmt);
	vsnprintf(msg, sizeof msg, fmt, args);
	va_end(args);
	stderrTracerEmit(file, line, func, level, msg);
}

#define DQLITE_MAX_CRASH_TRACE 8192

#include <stdatomic.h>

struct trace_record {
	_Atomic(uint64_t) id;
	uint64_t tid;
	uint64_t ns;
	const struct trace_def *trace_def;
	size_t argc;
	struct trace_arg argv[TRACE_MAX_ARGS];
};

static atomic_size_t trace_id_generator = 0;
static struct trace_record trace_records[DQLITE_MAX_CRASH_TRACE];

void dqlite_crash_trace(const struct trace_def *trace_def,
			const size_t argc,
			const struct trace_arg *argv)
{
	assert(argc <= TRACE_MAX_ARGS);

	pid_t tid = gettidImpl();
	uint64_t ns = 0;
	struct timespec ts;
	clock_gettime(CLOCK_REALTIME, &ts);
	ns = (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;

	size_t id = atomic_fetch_add(&trace_id_generator, 1);
	size_t index = id % DQLITE_MAX_CRASH_TRACE;

	struct trace_record *record = &trace_records[index];

	/* Mark as incomplete */
	atomic_store_explicit(&record->id, UINT64_MAX, memory_order_relaxed);

	record->tid = (uint64_t)tid;
	record->ns = ns;
	record->trace_def = trace_def;
	record->argc = argc;
	memcpy(record->argv, argv, sizeof(struct trace_arg) * argc);
	record->id = id; /* Mark as incomplete */
	atomic_thread_fence(memory_order_release);
}

struct trace_buffer {
	int fd;
	char buf[4096];
	size_t pos;
};

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result"

static void char_out(struct trace_buffer *writer, char c)
{
	if (writer->pos < sizeof(writer->buf)) {
		writer->buf[writer->pos++] = c;
	} else {
		write(writer->fd, writer->buf, writer->pos);
		writer->buf[0] = c;
		writer->pos = 1;
	}
}

static void str_out(struct trace_buffer *writer, const char *str)
{
	while (*str != '\0') {
		char_out(writer, *str++);
	}
}

/* Helper function to convert unsigned integer to string (async-signal-safe) */
static void uint_out(struct trace_buffer *writer, uint64_t value)
{
	if (value == 0) {
		return char_out(writer, '0');
	}

	char temp[32]; /* Enough for 64-bit numbers */
	size_t i = 0;
	while (value > 0 && i < sizeof(temp) - 1) {
		temp[i++] = (char)('0' + (value % 10));
		value /= 10;
	}

	while (i --> 0) {
		char_out(writer, temp[i]);
	}
}

static void ptr_out(struct trace_buffer *writer, const void *ptr)
{
	uintptr_t value = (uintptr_t)ptr;
	str_out(writer, "0x");

	bool leading_zero = true;
	for (size_t i = 0; i < sizeof(uintptr_t) * 2; i++) {
		uint8_t nibble = (value >> ((sizeof(uintptr_t) * 2 - 1 - i) * 4)) & 0xF;
		if (nibble == 0 && leading_zero) {
			continue;
		}
		leading_zero = false;
		if (nibble < 10) {
			char_out(writer, (char)('0' + nibble));
		} else {
			char_out(writer, (char)('a' + (nibble - 10)));
		}
	}
}

static void flush_out(struct trace_buffer *writer)
{
	if (writer->pos > 0) {
		write(writer->fd, writer->buf, writer->pos);
		writer->pos = 0;
	}
}

/* Helper function to convert signed integer to string (async-signal-safe) */
static void int_out(struct trace_buffer *writer, int64_t value)
{
	if (value == 0) {
		return char_out(writer, '0');
	}
	if (value == INT64_MIN) {
		/* Handle INT64_MIN edge case */
		str_out(writer, "9223372036854775808");
		return;
	}

	if (value < 0) {
		char_out(writer, '-');
		value = -value;
	}
	uint_out(writer, (uint64_t)value);
}

/* This function is a reentrant and async-signal-safe version of printf, even if
 * it is simplified to implement only the `tracef` useful subset. As such:
 * - it does not allocate memory
 * - it does not use locks
 * - it does not call functions that are not async-signal-safe
 * - it ignores locales
 * - it only supports integers and strings
 * - it does not support modifiers (like width, precision, etc)
 */
static void dqlite_sigsafe_fprintf(struct trace_buffer *writer, const char *fmt, const struct trace_arg *argv)
{
	const char *p = fmt;
	size_t arg_index = 0;
	while (*p != '\0') {
		if (*p == '%' && *(p + 1) != '\0') {
			p++; /* Skip '%' */
			
			if (*p == '%') {
				/* Literal '%' */
				char_out(writer, '%');
				p++;
				continue;
			}

			const struct trace_arg *arg = &argv[arg_index];
			switch (arg->type) {
			case TRACE_ARG_PTR:
				if (!(*p == 'p')) {
					str_out(writer, "<formatting error>");
					return;
				}

				ptr_out(writer, arg->value.ptr);
				break;
			case TRACE_ARG_I8:
				if (!(strncmp(p, PRId8, sizeof(PRId8)-1) == 0)) {
					str_out(writer, "<formatting error>");
					return;
				}

				int_out(writer, arg->value.i8);
				p += strlen(PRId8) - 1;
				break;
			case TRACE_ARG_U8:
				if (!(strncmp(p, PRIu8, sizeof(PRIu8)-1) == 0)) {
					str_out(writer, "<formatting error>");
					return;
				}

				uint_out(writer, arg->value.u8);
				p += strlen(PRIu8) - 1;
				break;
			case TRACE_ARG_I16:
				if (!(strncmp(p, PRId16, sizeof(PRId16)-1) == 0)) {
					str_out(writer, "<formatting error>");
					return;
				}

				int_out(writer, arg->value.i16);
				p += strlen(PRId16) - 1;
				break;
			case TRACE_ARG_U16:
				if (!(strncmp(p, PRIu16, sizeof(PRIu16)-1) == 0)) {
					str_out(writer, "<formatting error>");
					return;
				}

				uint_out(writer, arg->value.u16);
				p += strlen(PRIu16) - 1;
				break;
			case TRACE_ARG_I32:
				if (!(strncmp(p, PRId32, sizeof(PRId32)-1) == 0)) {
					str_out(writer, "<formatting error>");
					return;
				}

				int_out(writer, arg->value.i32);
				p += strlen(PRId32) - 1;
				break;
			case TRACE_ARG_U32:
				if (!(strncmp(p, PRIu32, sizeof(PRIu32)-1) == 0)) {
					str_out(writer, "<formatting error>");
					return;
				}

				uint_out(writer, arg->value.u32);
				p += strlen(PRIu32) - 1;
				break;
			case TRACE_ARG_I64:
				if (!(strncmp(p, PRId64, sizeof(PRId64)-1) == 0)) {
					str_out(writer, "<formatting error>");
					return;
				}

				int_out(writer, arg->value.i64);
				p += strlen(PRId64) - 1;
				break;
			case TRACE_ARG_U64:
				if (!(strncmp(p, PRIu64, sizeof(PRIu64)-1) == 0)) {
					str_out(writer, "<formatting error>");
					return;
				}

				uint_out(writer, arg->value.u64);
				p += strlen(PRIu64) - 1;
				break;
			case TRACE_ARG_STR:
				if (!(*p == 's')) {
					str_out(writer, "<formatting error>");
					return;
				}

				str_out(writer, arg->value.str);
				break;
			default:
				str_out(writer, "<unsupported arg type>");
				return;
			}
			arg_index++;
		} else {
			/* Regular character */
			char_out(writer, *p);
		}
		p++;
	}
}

#define WRITE_ONE(X)                          \
	_Generic((X),                         \
	    bool: uint_out,              \
	    int8_t: int_out,            \
	    uint8_t: uint_out,           \
	    int16_t: int_out,          \
	    uint16_t: uint_out,         \
	    int32_t: int_out,          \
	    uint32_t: uint_out,         \
	    int64_t: int_out,          \
	    uint64_t: uint_out,         \
	    const char *: str_out,  \
	    char *: str_out  \
	)(&writer, (X))

#define WRITE_OUT_0()
#define WRITE_OUT_1(X) WRITE_ONE(X)
#define WRITE_OUT_2(X, Y) WRITE_ONE(X), WRITE_ONE(Y)
#define WRITE_OUT_3(X, ...) WRITE_ONE(X), WRITE_OUT_2(__VA_ARGS__)
#define WRITE_OUT_4(X, ...) WRITE_ONE(X), WRITE_OUT_3(__VA_ARGS__)
#define WRITE_OUT_5(X, ...) WRITE_ONE(X), WRITE_OUT_4(__VA_ARGS__)
#define WRITE_OUT_6(X, ...) WRITE_ONE(X), WRITE_OUT_5(__VA_ARGS__)
#define WRITE_OUT_7(X, ...) WRITE_ONE(X), WRITE_OUT_6(__VA_ARGS__)
#define WRITE_OUT_8(X, ...) WRITE_ONE(X), WRITE_OUT_7(__VA_ARGS__)
#define WRITE_OUT_9(X, ...) WRITE_ONE(X), WRITE_OUT_8(__VA_ARGS__)
#define WRITE_OUT_10(X, ...) WRITE_ONE(X), WRITE_OUT_9(__VA_ARGS__)
#define WRITE_OUT(...) MACRO_CAT(WRITE_OUT_, COUNT_ARGS(__VA_ARGS__))(__VA_ARGS__)

DQLITE_VISIBLE_TO_TESTS DQLITE_NOINLINE
void dqlite_print_crash_trace(int fd) {
	uint64_t next_id = atomic_load(&trace_id_generator);

	/* Iterate over trace records in order of their IDs */
	uint64_t n_records = DQLITE_MAX_CRASH_TRACE;
	if (next_id < DQLITE_MAX_CRASH_TRACE) {
		n_records = next_id;
	}

	struct trace_buffer writer = {
		.fd = fd,
	};

	WRITE_OUT("Tentatively showing last ", n_records, " crash trace records:\n");

	for (uint64_t id = next_id - n_records; id < next_id; id++) {
		size_t index = id % DQLITE_MAX_CRASH_TRACE;

		atomic_thread_fence(memory_order_acquire);
		struct trace_record record = {
			.id = trace_records[index].id,
		};
		if (record.id != id) {
			/* This record has not been written yet or is from a previous iteration */
			continue;
		}
		record = trace_records[index];

		/* Print a simplified header for crashes. 
		 * Example:
		 *   91205050700 001132 		uvClientSend  src/uv_send.c:218 append entries
		 */
		WRITE_OUT("\t ", record.ns, " ", record.tid, " ");
		WRITE_OUT(tracerShortFileName(record.trace_def->file), ":",
			  record.trace_def->line, " ", record.trace_def->func,
			  " \t");

		/* Print the trace record */
		const struct trace_def *def = record.trace_def;
		dqlite_sigsafe_fprintf(&writer, def->fmt, record.argv);
		char_out(&writer, '\n');
		flush_out(&writer);
	}
}

#pragma GCC diagnostic pop
