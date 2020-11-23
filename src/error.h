#ifndef DQLITE_ERROR_H
#define DQLITE_ERROR_H

#include <string.h>

#include <sqlite3.h>

/* A message describing the last error occurred on an object */
typedef char *dqliteError;

/* Initialize the error with an empty message */
void dqliteError_init(dqliteError *e);

/* Release the memory of the error message, if any is set */
void dqliteError_close(dqliteError *e);

/* Set the error message */
void dqliteError_printf(dqliteError *e, const char *fmt, ...);

/* Wrap an error with an additional message */
void dqliteError_wrapf(dqliteError *e,
		       const dqliteError *cause,
		       const char *fmt,
		       ...);

/* Out of memory error */
void dqliteError_oom(dqliteError *e, const char *msg, ...);

/* Wrap a system error */
void dqliteError_sys(dqliteError *e, const char *msg);

/* Wrap an error from libuv */
void dqliteError_uv(dqliteError *e, int err, const char *msg);

/* Copy the underlying error message.
 *
 * Client code is responsible of invoking sqlite3_free to deallocate the
 * returned string.
 */
int dqliteError_copy(dqliteError *e, char **msg);

/* Whether the error is not set */
int dqliteError_is_null(dqliteError *e);

/* Whether the error is due to client disconnection */
int dqliteError_isDisconnect(dqliteError *e);

#endif /* DQLITE_ERROR_H */
