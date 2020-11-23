#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sqlite3.h>
#include <uv.h>

#include "../include/dqlite.h"

#include "./lib/assert.h"

#include "error.h"

/* Fallback message returned when failing to allocate the error message
 * itself. */
static char *dqlite_error_oom_msg = "error message unavailable (out of memory)";

void dqlite_error_init(dqlite_error *e)
{
	*e = NULL;
}

void dqlite_error_close(dqlite_error *e)
{
	if (*e != NULL && *e != dqlite_error_oom_msg) {
		sqlite3_free(*e);
	}
}

/* Set an error message by rendering the given format against the given
 * parameters.
 *
 * Any previously set error message will be cleared. */
static void dqlite_error_vprintf(dqlite_error *e, const char *fmt, va_list args)
{
	assert(fmt != NULL);

	/* If a previous error was set (other than the hard-coded OOM fallback
	 * fallback), let's free it. */
	if (*e != NULL && *e != dqlite_error_oom_msg) {
		sqlite3_free(*e);
	}

	/* Render the message. In case of error we fallback to the hard-coded
	 * OOM fallback message. */
	*e = sqlite3_vmprintf(fmt, args);
	if (*e == NULL) {
		*e = dqlite_error_oom_msg;
	}
}

void dqlite_error_printf(dqlite_error *e, const char *fmt, ...)
{
	va_list args;

	va_start(args, fmt);
	dqlite_error_vprintf(e, fmt, args);
	va_end(args);
}

static void dqlite_error_vwrapf(dqlite_error *e,
				const char *cause,
				const char *fmt,
				va_list args)
{
	dqlite_error tmp;
	char *        msg;

	/* First, print the format and arguments, using a temporary error. */
	dqlite_error_init(&tmp);

	dqlite_error_vprintf(&tmp, fmt, args);

	if (cause == NULL) {
		/* Special case the cause error being empty. */
		dqlite_error_printf(e, "%s: (null)", tmp);
	} else if (cause == *e) {
		/* When the error is wrapping itself, we need to make a copy */
		dqlite_error_copy(e, &msg);
		dqlite_error_printf(e, "%s: %s", tmp, msg);
		sqlite3_free(msg);
	} else {
		dqlite_error_printf(e, "%s: %s", tmp, cause);
	}

	dqlite_error_close(&tmp);
}

void dqlite_error_wrapf(dqlite_error *e,
			const dqlite_error *cause,
			const char *fmt,
			...)
{
	va_list args;

	va_start(args, fmt);
	dqlite_error_vwrapf(e, (const char *)(*cause), fmt, args);
	va_end(args);
}

void dqlite_error_oom(dqlite_error *e, const char *msg, ...)
{
	va_list args;

	va_start(args, msg);
	dqlite_error_vwrapf(e, "out of memory", msg, args);
	va_end(args);
}

void dqlite_error_sys(dqlite_error *e, const char *msg)
{
	dqlite_error_printf(e, "%s: %s", msg, strerror(errno));
}

void dqlite_error_uv(dqlite_error *e, int err, const char *msg)
{
	dqlite_error_printf(e, "%s: %s (%s)", msg, uv_strerror(err),
			    uv_err_name(err));
}

int dqlite_error_copy(dqlite_error *e, char **msg)
{
	char * copy;
	size_t len;

	assert(e != NULL);
	assert(msg != NULL);

	/* Trying to copy an empty error message is an error. */
	if (*e == NULL) {
		*msg = NULL;
		return DQLITE_ERROR;
	}

	len = strlen(*e) + 1;

	copy = sqlite3_malloc((int)(len * sizeof *copy));
	if (copy == NULL) {
		*msg = NULL;
		return DQLITE_NOMEM;
	}

	memcpy(copy, *e, len);

	*msg = copy;

	return 0;
}

int dqlite_error_is_null(dqlite_error *e)
{
	return *e == NULL;
}

int dqlite_error_is_disconnect(dqlite_error *e)
{
	if (*e == NULL)
		return 0;

	if (strstr(*e, uv_err_name(UV_EOF)) != NULL)
		return 1;

	if (strstr(*e, uv_err_name(UV_ECONNRESET)) != NULL)
		return 1;

	return 0;
}
