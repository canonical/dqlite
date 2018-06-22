#define _GNU_SOURCE

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sqlite3.h>
#include <uv.h>

#include "dqlite.h"
#include "error.h"
#include "lifecycle.h"

static char *dqlite__error_oom_msg = "error message unavailable (out of memory)";

void dqlite__error_init(dqlite__error *e)
{
	dqlite__lifecycle_init(DQLITE__LIFECYCLE_ERROR);

	*e = 0;
}

void dqlite__error_close(dqlite__error *e)
{
	if (*e != NULL && *e != dqlite__error_oom_msg)
		sqlite3_free(*e);

	dqlite__lifecycle_close(DQLITE__LIFECYCLE_ERROR);
}

static void dqlite__error_vprintf(dqlite__error *e, const char *fmt, va_list args)
{
	int err;
	int n;
	char *msg;

	assert(fmt != NULL);

	if (*e != NULL && *e != dqlite__error_oom_msg)
		sqlite3_free(*e);

	err = vasprintf(&msg, fmt, args);
	if (err < 0) {
		/* Fallback to an hard-coded message */
		*e = dqlite__error_oom_msg;
		return;
	}

	/* FIXME: we use sqlite3_malloc here so we can catch memory leaks in
	 * tests */
	n = strlen(msg) + 1;
	*e = sqlite3_malloc(n);
	if (*e == NULL) {
		/* Fallback to an hard-coded message */
		*e = "error message unavailable (out of memory)";
		return;
	}

	memcpy(*e, msg, n);

	free(msg); /* Allocated by the vasprintf call above */
}

void dqlite__error_printf(dqlite__error *e, const char *fmt, ...)
{
	va_list args;

	va_start(args, fmt);

	dqlite__error_vprintf(e, fmt, args);

	va_end(args);
}

void dqlite__error_wrapf(dqlite__error *e, const dqlite__error *cause, const char *fmt, ...)
{
	dqlite__error tmp;
	va_list args;
	char *msg;

	/* First, print the format and arguments, using a temporary error. */
	dqlite__error_init(&tmp);

	va_start(args, fmt);
	dqlite__error_vprintf(&tmp, fmt, args);
	va_end(args);

	if (cause == e) {
		/* When the error is wrapping itself, we need to make a copy */
		dqlite__error_copy(e, &msg);
		dqlite__error_printf(e, "%s: %s", tmp, msg);
		sqlite3_free(msg);
	} else {
		dqlite__error_printf(e, "%s: %s", tmp, *cause);
	}

	dqlite__error_close(&tmp);
}

void dqlite__error_oom(dqlite__error *e, const char *msg)
{
	dqlite__error_printf(e, "%s: %s", msg, "out of memory");
}

void dqlite__error_sys(dqlite__error *e, const char *msg)
{
	dqlite__error_printf(e, "%s: %s", msg, strerror(errno));
}

void dqlite__error_uv(dqlite__error *e, int err, const char *msg)
{
	dqlite__error_printf(e, "%s: %s (%s)", msg, uv_strerror(err), uv_err_name(err));
}

int dqlite__error_copy(dqlite__error *e, char **msg)
{
	char *copy;
	int len;

	assert(e != NULL);
	assert(msg != NULL);

	if (*e == NULL) {
		*msg = NULL;
		return DQLITE_ERROR;
	}

	len = strlen(*e) + 1;

	copy = (char*)sqlite3_malloc(len);
	if (copy == NULL) {
		*msg = NULL;
		return DQLITE_NOMEM;
	}

	memcpy(copy, *e, len);

	*msg = copy;

	return 0;
}

int dqlite__error_is_null(dqlite__error *e)
{
	return *e == NULL;
}

int dqlite__error_is_disconnect(dqlite__error *e)
{
	if (*e == NULL)
		return 0;

	if (strstr(*e, uv_err_name(UV_EOF)) != NULL)
		return 1;

	if (strstr(*e, uv_err_name(UV_ECONNRESET)) != NULL)
		return 1;

	return 0;
}
