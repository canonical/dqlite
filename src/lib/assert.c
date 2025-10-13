#ifdef DQLITE_ASSERT_WITH_BACKTRACE

#include "assert.h"

#if defined(HAVE_BACKTRACE_H)
#include <backtrace.h>
#include <stdio.h>

void dqlite_fail(const char *__assertion,
		 const char *__file,
		 unsigned int __line,
		 const char *__function)
{
	struct backtrace_state *state_;
	state_ = backtrace_create_state(NULL, 1, NULL, NULL);
	backtrace_print(state_, 0, stderr);
	__assert_fail(__assertion, __file, __line, __function);
}

#elif defined(HAVE_EXECINFO_H) /* HAVE_BACKTRACE_H */
#include <execinfo.h>
#include <unistd.h>

void dqlite_fail(const char *__assertion,
		 const char *__file,
		 unsigned int __line,
		 const char *__function)
{
	void *buffer[100];
	int nptrs = backtrace(buffer, 100);
	backtrace_symbols_fd(buffer, nptrs, STDERR_FILENO);
	__assert_fail(__assertion, __file, __line, __function);
}

#endif /* HAVE_EXECINFO_H */
#endif /* DQLITE_ASSERT_WITH_BACKTRACE */
