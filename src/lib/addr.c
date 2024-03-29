#include "addr.h"

#include <netdb.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/un.h>

#include "../../include/dqlite.h"

int AddrParse(const char *input,
	      struct sockaddr *addr,
	      socklen_t *addr_len,
	      const char *service,
	      int flags)
{
	int rv;
	char *node = NULL;
	size_t input_len = strlen(input);
	char c = input[0];
	struct sockaddr_un *addr_un;
	const char *name, *addr_start, *close_bracket, *colon;
	size_t name_len;
	struct addrinfo hints, *res;

	if (c == '@') {
		/* Unix domain address.
		 * FIXME the use of the "abstract namespace" here is
		 * Linux-specific */
		if (!(flags & DQLITE_ADDR_PARSE_UNIX)) {
			return DQLITE_MISUSE;
		}
		addr_un = (struct sockaddr_un *)addr;
		if (*addr_len < sizeof(*addr_un)) {
			return DQLITE_ERROR;
		}
		name = input + 1;
		name_len = input_len - 1;
		if (name_len == 0) {
			/* Autogenerated abstract socket name */
			addr_un->sun_family = AF_UNIX;
			*addr_len = sizeof(addr_un->sun_family);
			return 0;
		}
		/* Leading null byte, no trailing null byte */
		if (name_len + 1 > sizeof(addr_un->sun_path)) {
			return DQLITE_ERROR;
		}
		memset(addr_un->sun_path, 0, sizeof(addr_un->sun_path));
		memcpy(addr_un->sun_path + 1, name, name_len);
		addr_un->sun_family = AF_UNIX;
		*addr_len = (socklen_t)offsetof(struct sockaddr_un, sun_path) +
			    (socklen_t)name_len + 1;
		return 0;
	} else if (c == '[') {
		/* IPv6 address with port */
		addr_start = input + 1;
		close_bracket = memchr(input, ']', input_len);
		if (!close_bracket) {
			return DQLITE_ERROR;
		}
		colon = close_bracket + 1;
		if (*colon != ':') {
			return DQLITE_ERROR;
		}
		service = colon + 1;
		node =
		    strndup(addr_start, (size_t)(close_bracket - addr_start));
	} else if (memchr(input, '.', input_len)) {
		/* IPv4 address */
		colon = memchr(input, ':', input_len);
		if (colon) {
			service = colon + 1;
			node = strndup(input, (size_t)(colon - input));
		} else {
			node = strdup(input);
		}
	} else {
		/* IPv6 address without port */
		node = strdup(input);
	}

	if (!node) {
		return DQLITE_NOMEM;
	}

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_NUMERICHOST | AI_NUMERICSERV;
	rv = getaddrinfo(node, service, &hints, &res);
	if (rv != 0) {
		rv = DQLITE_ERROR;
		goto err_after_strdup;
	}
	if (res->ai_addrlen > *addr_len) {
		rv = DQLITE_ERROR;
		goto err_after_getaddrinfo;
	}
	memcpy(addr, res->ai_addr, res->ai_addrlen);
	*addr_len = res->ai_addrlen;

err_after_getaddrinfo:
	freeaddrinfo(res);

err_after_strdup:
	free(node);

	return rv;
}
