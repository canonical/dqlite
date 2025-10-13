#ifndef LIB_ASSERT_H_
#define LIB_ASSERT_H_

#include <assert.h>

#ifdef HAVE_BACKTRACE_H

# include <backtrace.h>
# include <stdio.h>
# define dqlite_assert(x)                                                             \
	do {                                                                  \
		struct backtrace_state *state_;                               \
		if (!(x)) {                                                   \
			state_ = backtrace_create_state(NULL, 0, NULL, NULL); \
			backtrace_print(state_, 0, stderr);                   \
			__assert_fail(#x, __FILE__, __LINE__, __func__);      \
		}                                                             \
	} while (0)
#else
# define dqlite_assert(x) assert(x)
#endif

#endif /* LIB_ASSERT_H_ */
