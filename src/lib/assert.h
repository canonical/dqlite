#ifndef LIB_ASSERT_H_
#define LIB_ASSERT_H_

#include <assert.h>

#ifdef DQLITE_ASSERT_WITH_BACKTRACE

#define dqlite_assert(x)                                               \
	do {                                                           \
		if (!(x)) {                                            \
			dqlite_fail(#x, __FILE__, __LINE__, __func__); \
		}                                                      \
	} while (0)

/* This symbol is weak to allow test switching this off and provide their own
 * global trace implementation. */
void dqlite_fail (const char *__assertion, const char *__file,
			   unsigned int __line, const char *__function)
     __attribute__ ((__noreturn__, weak, visibility("default")));

#else
# define dqlite_assert(x) assert(x)
#endif

#endif /* LIB_ASSERT_H_ */
