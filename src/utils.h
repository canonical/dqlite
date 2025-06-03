#ifndef DQLITE_UTILS_H_
#define DQLITE_UTILS_H_

#include <assert.h>
#include <stdbool.h>
#include <stdint.h>

/* Various utility functions and macros */

#define PTR_TO_UINT64(p) ((uint64_t)((uintptr_t)(p)))
#define UINT64_TO_PTR(u, ptr_type) ((ptr_type)((uintptr_t)(u)))

#define LIKELY(x) __builtin_expect(!!(x), 1)
#define UNLIKELY(x) __builtin_expect(!!(x), 0)
#define IMPOSSIBLE(why) assert(false && why)

#define DBG() fprintf(stderr, "%s:%d\n", __func__, __LINE__)

#define CONTAINER_OF(e, type, field) \
	((type *)(uintptr_t)((char *)(e)-offsetof(type, field)))

#define PRE(cond) assert((cond))
#define POST(cond) assert((cond))
#define ERGO(a, b) (!(a) || (b))

#define IN_1(E, X) E == X
#define IN_2(E, X, ...) E == X || IN_1(E,__VA_ARGS__)
#define IN_3(E, X, ...) E == X || IN_2(E,__VA_ARGS__)
#define IN_4(E, X, ...) E == X || IN_3(E,__VA_ARGS__)
#define IN_5(E, X, ...) E == X || IN_4(E,__VA_ARGS__)
#define IN_6(E, X, ...) E == X || IN_5(E,__VA_ARGS__)
#define IN_7(E, X, ...) E == X || IN_6(E,__VA_ARGS__)
#define IN_8(E, X, ...) E == X || IN_7(E,__VA_ARGS__)
#define IN_9(E, X, ...) E == X || IN_8(E,__VA_ARGS__)

#define GET_IN_MACRO(_1,_2,_3,_4,_5,_6,_7,_8,_9,NAME,...) NAME
#define IN(E, ...) \
  (GET_IN_MACRO(__VA_ARGS__,IN_9,IN_8,IN_7,IN_6,IN_5,IN_4,IN_3,IN_2,IN_1)(E,__VA_ARGS__))

#if defined(__has_attribute) && __has_attribute (musttail)
# define TAIL __attribute__ ((musttail))
#else
# define TAIL
#endif

#if defined(__has_attribute) && __has_attribute (noinline)
# define NOINLINE __attribute__ ((noinline))
#else
# define NOINLINE
#endif

static inline bool is_po2(unsigned long n) {
	return n > 0 && (n & (n - 1)) == 0;
}

#endif /* DQLITE_UTILS_H_ */
