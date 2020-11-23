#ifndef DQLITE_ERROR_H
#define DQLITE_ERROR_H

#include <string.h>

#include <sqlite3.h>

/* A message describing the last error occurred on an object */
typedef char *dqlite_error;

/* Initialize the error with an empty message */
void dqlite_error_init(dqlite_error *e);

/* Release the memory of the error message, if any is set */
void dqlite_error_close(dqlite_error *e);

/* Set the error message */
void dqlite_error_printf(dqlite_error *e, const char *fmt, ...);

/* Wrap an error with an additional message */
void dqlite_error_wrapf(dqlite_error *e,
			const dqlite_error *cause,
			const char *fmt,
			...);

/* Out of memory error */
void dqlite_error_oom(dqlite_error *e, const char *msg, ...);

/* Wrap a system error */
void dqlite_error_sys(dqlite_error *e, const char *msg);

/* Wrap an error from libuv */
void dqlite_error_uv(dqlite_error *e, int err, const char *msg);

/* Copy the underlying error message.
 *
 * Client code is responsible of invoking sqlite3_free to deallocate the
 * returned string.
 */
int dqlite_error_copy(dqlite_error *e, char **msg);

/* Whether the error is not set */
int dqlite_error_is_null(dqlite_error *e);

/* Whether the error is due to client disconnection */
int dqlite_error_is_disconnect(dqlite_error *e);

#endif /* DQLITE_ERROR_H */
