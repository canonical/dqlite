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

static inline bool is_po2(unsigned long n) {
	return n > 0 && (n & (n - 1)) == 0;
}

#endif /* DQLITE_UTILS_H_ */
