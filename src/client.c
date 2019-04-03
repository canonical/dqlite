#include <unistd.h>

#include "client.h"
#include "request.h"

void client__init(struct client *c, int fd)
{
	c->fd = fd;
}

int client__handshake(struct client *c)
{
	uint64_t protocol;
	int rv;

	protocol = byte__flip64(DQLITE_PROTOCOL_VERSION);

	rv = write(c->fd, &protocol, sizeof(protocol));
	if (rv < 0) {
		return DQLITE_ERROR;
	}

	return 0;
}

void client__close(struct client *c) {
	close(c->fd);
}
