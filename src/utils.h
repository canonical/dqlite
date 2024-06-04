#ifndef DQLITE_UTILS_H_
#define DQLITE_UTILS_H_

#include <assert.h>
#include <stdbool.h>
#include <stdint.h>

#include <sqlite3.h>

/* Various utility functions and macros */

#define PTR_TO_UINT64(p) ((uint64_t)((uintptr_t)(p)))
#define UINT64_TO_PTR(u, ptr_type) ((ptr_type)((uintptr_t)(u)))

#define LIKELY(x) __builtin_expect(!!(x), 1)
#define UNLIKELY(x) __builtin_expect(!!(x), 0)

#define DBG() fprintf(stderr, "%s:%d\n", __func__, __LINE__)

#define CONTAINER_OF(e, type, field) \
	((type *)(uintptr_t)((char *)(e)-offsetof(type, field)))

#define PRE(cond) assert((cond))
#define POST(cond) assert((cond))
#define ERGO(a, b) (!(a) || (b))

#define UNHANDLED(expr) if (expr) assert(0)

/* XXX this is silly, take care of it in the build system */
#ifdef DQLITE_NEXT
#define NEXT 1
#else
#define NEXT 0
#endif

static inline bool is_po2(unsigned long n) {
	return n > 0 && (n & (n - 1)) == 0;
}

static inline sqlite3_file *main_file(sqlite3 *conn)
{
	PRE(conn != NULL);
	sqlite3_file *fp;
	int rv = sqlite3_file_control(conn, "main", SQLITE_FCNTL_FILE_POINTER, &fp);
	assert(rv == SQLITE_OK);
	POST(fp != NULL);
	return fp;
}

#endif /* DQLITE_UTILS_H_ */
