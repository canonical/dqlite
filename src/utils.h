#ifndef DQLITE_UTILS_H_
#define DQLITE_UTILS_H_

#include <stdint.h>

/* Various utility functions and macros */

#define PTR_TO_UINT64(p) ((uint64_t)((uintptr_t)(p)))
#define UINT64_TO_PTR(u, ptr_type) ((ptr_type)((uintptr_t)(u)))

#define LIKELY(x)       __builtin_expect(!!(x),1)
#define UNLIKELY(x)     __builtin_expect(!!(x),0)

#define DBG()           fprintf(stderr, "%s:%d\n", __func__, __LINE__)

#endif /* DQLITE_UTILS_H_ */
