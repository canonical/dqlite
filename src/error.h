#ifndef DQLITE_ERROR_H
#define DQLITE_ERROR_H

#include <string.h>

#include <sqlite3.h>

/* A message describing the last error occurred on an object */
typedef char *dqlite__error;

/* Initialize the error with an empty message */
void dqlite__error_init(dqlite__error *e);

/* Release the memory of the error message, if any is set */
void dqlite__error_close(dqlite__error *e);

/* Set the error message */
void dqlite__error_printf(dqlite__error *e, const char *fmt, ...);

/* Wrap an error with an additional message */
void dqlite__error_wrapf(dqlite__error *      e,
                         const dqlite__error *cause,
                         const char *         fmt,
                         ...);

/* Out of memory error */
void dqlite__error_oom(dqlite__error *e, const char *msg, ...);

/* Wrap a system error */
void dqlite__error_sys(dqlite__error *e, const char *msg);

/* Wrap an error from libuv */
void dqlite__error_uv(dqlite__error *e, int err, const char *msg);

/* Copy the underlying error message.
 *
 * Client code is responsible of invoking sqlite3_free to deallocate the
 * returned string.
 */
int dqlite__error_copy(dqlite__error *e, char **msg);

/* Whether the error is not set */
int dqlite__error_is_null(dqlite__error *e);

/* Whether the error is due to client disconnection */
int dqlite__error_is_disconnect(dqlite__error *e);

#endif /* DQLITE_ERROR_H */
