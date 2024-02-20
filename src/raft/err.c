#include "err.h"

#include <string.h>

#include "../raft.h"
#include "assert.h"

#define WRAP_SEP ": "
#define WRAP_SEP_LEN ((size_t)strlen(WRAP_SEP))

void errMsgWrap(char *e, const char *format)
{
	size_t n = RAFT_ERRMSG_BUF_SIZE;
	size_t prefix_n;
	size_t prefix_and_sep_n;
	size_t trail_n;
	size_t i;

	/* Calculate the length of the prefix. */
	prefix_n = strlen(format);

	/* If there isn't enough space for the ": " separator and at least one
	 * character of the wrapped error message, then just print the prefix.
	 */
	if (prefix_n >= n - (WRAP_SEP_LEN + 1)) {
/* We explicitly allow truncation here + silence clang about unknown
 * warning-group "-Wformat-truncation" */
#ifdef __GNUC__
#ifndef __clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-truncation"
#endif
#endif
		ErrMsgPrintf(e, "%s", format);
#ifdef __GNUC__
#ifndef __clang__
#pragma GCC diagnostic pop
#endif
#endif
		return;
	}

	/* Right-shift the wrapped message, to make room for the prefix. */
	prefix_and_sep_n = prefix_n + WRAP_SEP_LEN;
	trail_n = strnlen(e, n - prefix_and_sep_n - 1);
	memmove(e + prefix_and_sep_n, e, trail_n);
	e[prefix_and_sep_n + trail_n] = 0;

	/* Print the prefix. */
	ErrMsgPrintf(e, "%s", format);

	/* Print the separator.
	 *
	 * Avoid using strncpy(e->msg + prefix_n, WRAP_SEP, WRAP_SEP_LEN) since
	 * it generates a warning. */
	for (i = 0; i < WRAP_SEP_LEN; i++) {
		e[prefix_n + i] = WRAP_SEP[i];
	}
}

#define ERR_CODE_TO_STRING_CASE(CODE, MSG) \
	case CODE:                         \
		return MSG;

const char *errCodeToString(int code)
{
	switch (code) {
		ERR_CODE_TO_STRING_MAP(ERR_CODE_TO_STRING_CASE);
		default:
			return "unknown error";
	}
}
