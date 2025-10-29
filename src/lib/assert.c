#ifdef DQLITE_ASSERT_WITH_BACKTRACE

#include "assert.h"
#include <unistd.h>

void dqlite_print_crash_trace(int fd); // defined in tracing.c
void dqlite_print_trace(int skip);

/* This is necessary as dqlite is using -Werror, but glibc defines __assert_fail
 * with an unsigned __line argument while musl with an int. On one of them there
 * would be then a conversion which will generate a warning (turned into an
 * error by -Werror). */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-conversion"

void dqlite_fail(const char *__assertion,
		 const char *__file,
		 unsigned int __line,
		 const char *__function)
{
	dqlite_print_trace(1);
	__assert_fail(__assertion, __file, __line, __function);
}

#pragma GCC diagnostic pop

#if defined(HAVE_BACKTRACE_H)
#include <backtrace.h>
#include <stdio.h>

void dqlite_print_trace(int skip)
{
	struct backtrace_state *state_;
	state_ = backtrace_create_state(NULL, skip, NULL, NULL);
	backtrace_print(state_, 0, stderr);

	dqlite_print_crash_trace(STDERR_FILENO);
}

#elif defined(HAVE_EXECINFO_H) /* HAVE_BACKTRACE_H */
#include <execinfo.h>

void dqlite_print_trace(int skip)
{
	void *buffer[100];
	int nptrs = backtrace(buffer, 100);
	if (nptrs > skip) {
		backtrace_symbols_fd(buffer + skip, nptrs - skip, STDERR_FILENO);
	}

	dqlite_print_crash_trace(STDERR_FILENO);
}

#elif defined(HAVE_LIBUNWIND_H)
#include <libunwind.h>
#include <stdio.h>

void dqlite_print_trace(int skip)
{
	unw_cursor_t cursor;
	unw_context_t context;
	unw_getcontext(&context);
	unw_init_local(&cursor, &context);


	while (unw_step(&cursor) > 0) {
		if (skip > 0) {
			skip--;
			continue;
		}

		unw_word_t offset, pc;
		char sym[256];

		unw_get_reg(&cursor, UNW_REG_IP, &pc);
		if (pc == 0) {
			break;
		}
		fprintf(stderr, "0x%lx: ", (long) pc);

		if (unw_get_proc_name(&cursor, sym, sizeof(sym), &offset) == 0) {
			fprintf(stderr, "(%s+0x%lx)\n", sym, (long) offset);
		} else {
			fprintf(stderr, "??\n");
		}
	}

	dqlite_print_crash_trace(STDERR_FILENO);
}

#endif /* HAVE_EXECINFO_H */
#endif /* DQLITE_ASSERT_WITH_BACKTRACE */
