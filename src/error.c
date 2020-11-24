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
static char *dqliteErrorOom_msg = "error message unavailable (out of memory)";

void dqliteErrorInit(dqliteError *e)
{
	*e = NULL;
}

void dqliteErrorClose(dqliteError *e)
{
	if (*e != NULL && *e != dqliteErrorOom_msg) {
		sqlite3_free(*e);
	}
}

/* Set an error message by rendering the given format against the given
 * parameters.
 *
 * Any previously set error message will be cleared. */
static void dqliteError_vprintf(dqliteError *e, const char *fmt, va_list args)
{
	assert(fmt != NULL);

	/* If a previous error was set (other than the hard-coded OOM fallback
	 * fallback), let's free it. */
	if (*e != NULL && *e != dqliteErrorOom_msg) {
		sqlite3_free(*e);
	}

	/* Render the message. In case of error we fallback to the hard-coded
	 * OOM fallback message. */
	*e = sqlite3_vmprintf(fmt, args);
	if (*e == NULL) {
		*e = dqliteErrorOom_msg;
	}
}

void dqliteErrorPrintf(dqliteError *e, const char *fmt, ...)
{
	va_list args;

	va_start(args, fmt);
	dqliteError_vprintf(e, fmt, args);
	va_end(args);
}

static void dqliteError_vwrapf(dqliteError *e,
			       const char *cause,
			       const char *fmt,
			       va_list args)
{
	dqliteError tmp;
	char *        msg;

	/* First, print the format and arguments, using a temporary error. */
	dqliteErrorInit(&tmp);

	dqliteError_vprintf(&tmp, fmt, args);

	if (cause == NULL) {
		/* Special case the cause error being empty. */
		dqliteErrorPrintf(e, "%s: (null)", tmp);
	} else if (cause == *e) {
		/* When the error is wrapping itself, we need to make a copy */
		dqliteErrorCopy(e, &msg);
		dqliteErrorPrintf(e, "%s: %s", tmp, msg);
		sqlite3_free(msg);
	} else {
		dqliteErrorPrintf(e, "%s: %s", tmp, cause);
	}

	dqliteErrorClose(&tmp);
}

void dqliteError_wrapf(dqliteError *e,
		       const dqliteError *cause,
		       const char *fmt,
		       ...)
{
	va_list args;

	va_start(args, fmt);
	dqliteError_vwrapf(e, (const char *)(*cause), fmt, args);
	va_end(args);
}

void dqliteErrorOom(dqliteError *e, const char *msg, ...)
{
	va_list args;

	va_start(args, msg);
	dqliteError_vwrapf(e, "out of memory", msg, args);
	va_end(args);
}

void dqliteErrorSys(dqliteError *e, const char *msg)
{
	dqliteErrorPrintf(e, "%s: %s", msg, strerror(errno));
}

void dqliteError_uv(dqliteError *e, int err, const char *msg)
{
	dqliteErrorPrintf(e, "%s: %s (%s)", msg, uv_strerror(err),
			  uv_err_name(err));
}

int dqliteErrorCopy(dqliteError *e, char **msg)
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

int dqliteErrorIsNull(dqliteError *e)
{
	return *e == NULL;
}

int dqliteErrorIsDisconnect(dqliteError *e)
{
	if (*e == NULL)
		return 0;

	if (strstr(*e, uv_err_name(UV_EOF)) != NULL)
		return 1;

	if (strstr(*e, uv_err_name(UV_ECONNRESET)) != NULL)
		return 1;

	return 0;
}
