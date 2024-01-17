#ifndef DQLITE_UTILS_H_
#define DQLITE_UTILS_H_

#include <stdbool.h>
#include <stdint.h>

/* Various utility functions and macros */

#define PTR_TO_UINT64(p) ((uint64_t)((uintptr_t)(p)))
#define UINT64_TO_PTR(u, ptr_type) ((ptr_type)((uintptr_t)(u)))

#define LIKELY(x) __builtin_expect(!!(x), 1)
#define UNLIKELY(x) __builtin_expect(!!(x), 0)

#define DBG() fprintf(stderr, "%s:%d\n", __func__, __LINE__)

#define UNUSED __attribute__((unused))

#define container_of(e, type, field) \
	((type *)(uintptr_t)((char *)(e)-offsetof(type, field)))

static inline bool _is0(const char *p, unsigned int s)
{
	unsigned int i;

	for (i = 0; i < s; ++i) {
		if (p[i] != 0) {
			return false;
		}
	}

	return true;
}

#define ARE0(s, n) _is0((const char *)(s), (n))
#define IS0(s) _is0((const char *)(s), sizeof(*(s)))

#define PRE(cond) assert((cond))
#define POST(cond) assert((cond))

#endif /* DQLITE_UTILS_H_ */
