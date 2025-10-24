#ifndef DQLITE_UTILS_H_
#define DQLITE_UTILS_H_

#include <stdbool.h>
#include <stdint.h>

#include "lib/assert.h"

/* Various utility functions and macros */

#define PTR_TO_UINT64(p) ((uint64_t)((uintptr_t)(p)))
#define UINT64_TO_PTR(u, ptr_type) ((ptr_type)((uintptr_t)(u)))

#define LIKELY(x) __builtin_expect(!!(x), 1)
#define UNLIKELY(x) __builtin_expect(!!(x), 0)
#define IMPOSSIBLE(why) dqlite_assert(false && why)

#define DBG() fprintf(stderr, "%s:%d\n", __func__, __LINE__)

#define CONTAINER_OF(e, type, field) \
	((type *)(uintptr_t)((char *)(e)-offsetof(type, field)))

#define PRE(cond) dqlite_assert((cond))
#define POST(cond) dqlite_assert((cond))
#define ERGO(a, b) (!(a) || (b))


#define MACRO_CAT_HELPER(a, b) a##b
#define MACRO_CAT(a, b) MACRO_CAT_HELPER(a, b)

#define IN_1(E, X) E == X
#define IN_2(E, X, ...) E == X || IN_1(E,__VA_ARGS__)
#define IN_3(E, X, ...) E == X || IN_2(E,__VA_ARGS__)
#define IN_4(E, X, ...) E == X || IN_3(E,__VA_ARGS__)
#define IN_5(E, X, ...) E == X || IN_4(E,__VA_ARGS__)
#define IN_6(E, X, ...) E == X || IN_5(E,__VA_ARGS__)
#define IN_7(E, X, ...) E == X || IN_6(E,__VA_ARGS__)
#define IN_8(E, X, ...) E == X || IN_7(E,__VA_ARGS__)
#define IN_9(E, X, ...) E == X || IN_8(E,__VA_ARGS__)


#define COUNT_ARGS_HELPER(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, N, ...) N
#define COUNT_ARGS(...) COUNT_ARGS_HELPER(__VA_ARGS__, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1)

#define GET_IN_MACRO(_1,_2,_3,_4,_5,_6,_7,_8,_9,NAME,...) NAME
#define IN(E, ...) \
  (MACRO_CAT(IN_, COUNT_ARGS(__VA_ARGS__)) (E, __VA_ARGS__))

#if defined(__has_attribute) && __has_attribute (noinline)
# define DQLITE_NOINLINE __attribute__ ((noinline))
#else
# define DQLITE_NOINLINE
#endif

#if defined(__has_attribute) && __has_attribute(format)
# define DQLITE_PRINTF(string_index, first_to_check) \
	__attribute__((format(printf, string_index, first_to_check)))
#else
# define DQLITE_PRINTF(string_index, first_to_check)
#endif

#if defined(__has_attribute) && __has_attribute(packed)
# define DQLITE_PACKED __attribute__((packed))
#else
# define DQLITE_PACKED
#endif

static inline bool is_po2(unsigned long n) {
	return n > 0 && (n & (n - 1)) == 0;
}

#endif /* DQLITE_UTILS_H_ */
