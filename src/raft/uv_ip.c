#include <stdlib.h>
#include <string.h>

#include <uv.h>

#include "../raft.h"

#include "uv_ip.h"

static const char *strCpyUntil(char *target,
			       const char *source,
			       size_t target_size,
			       char separator)
{
	size_t i;
	for (i = 0; i < target_size; ++i) {
		if (!source[i] || source[i] == separator) {
			target[i] = 0;
			return source + i;
		} else {
			target[i] = source[i];
		}
	}
	return NULL;
}

int uvIpAddrSplit(const char *address,
		  char *host,
		  size_t host_size,
		  char *service,
		  size_t service_size)
{
	char colon = ':';
	const char *service_ptr = NULL;

	if (host) {
		service_ptr = strCpyUntil(host, address, host_size, colon);
		if (!service_ptr) {
			return RAFT_NAMETOOLONG;
		}
	}
	if (service) {
		if (!service_ptr) {
			service_ptr = strchr(address, colon);
		}
		if (!service_ptr || *service_ptr == 0 ||
		    *(++service_ptr) == 0) {
			service_ptr = "8080";
		}
		if (!strCpyUntil(service, service_ptr, service_size, 0)) {
			return RAFT_NAMETOOLONG;
		}
	}
	return 0;
}

/* Synchronoues resolve hostname to IP address */
int uvIpResolveBindAddresses(const char *address, struct addrinfo **ai_result)
{
	static struct addrinfo hints = {
	    .ai_flags = AI_PASSIVE | AI_NUMERICSERV,
	    .ai_family = AF_INET,
	    .ai_socktype = SOCK_STREAM,
	    .ai_protocol = 0};
	char hostname[NI_MAXHOST];
	char service[NI_MAXSERV];
	int rv;

	rv = uvIpAddrSplit(address, hostname, sizeof(hostname), service,
			   sizeof(service));
	if (rv != 0) {
		return rv;
	}

	if (hostname[0]) {
		rv = getaddrinfo(hostname, service, &hints, ai_result);
	} else {
		rv = getaddrinfo(NULL, service, &hints, ai_result);
	}

	if (rv != 0) {
		return RAFT_IOERR;
	}

	return 0;
}
