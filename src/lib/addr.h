#ifndef ADDR_H_
#define ADDR_H_

#include <sys/socket.h>

enum {
	DQLITE_ADDR_PARSE_UNIX = 1 << 0 /* Parse Unix socket addresses in @ notation */
};

/** Parse a socket address from the string @input.
 *
 * On success, the resulting address is placed in @addr, and its size is placed
 * in @addr_len. If @addr is not large enough (based on the initial value of
 * @addr_len) to hold the result, DQLITE_ERROR is returned.
 *
 * @service should be a string representing a port number, e.g. "8080".
 *
 * @flags customizes the behavior of the function. Currently the only flag is
 * DQLITE_ADDR_PARSE_UNIX: when this is ORed in @flags, AddrParse will also
 * parse Unix socket addresses in the form `@NAME`, where NAME may be empty.
 * This creates a socket address in the (Linux-specific) "abstract namespace".
 */
int AddrParse(const char *input,
	      struct sockaddr *addr,
	      socklen_t *addr_len,
	      const char *service,
	      int flags);

#endif
