#include "tracing.h"
#include <stdio.h> /* stderr */
#include <stdlib.h>
#include <string.h>      /* strstr, strlen */
#include <sys/syscall.h> /* syscall */
#include <unistd.h>      /* syscall, getpid */
#include "assert.h"      /* assert */
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
	assert(tracer__level < TRACE_NR);

	if (level >= tracer__level)
		tracerEmit(file, line, func, level, message);
}
