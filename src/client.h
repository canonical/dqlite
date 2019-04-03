/**
 * Core dqlite client logic for encoding requests and decoding responses.
 */

#ifndef CLIENT_H_
#define CLIENT_H_

struct client {
	int fd; /* Connected socket */
};

/**
 * Initialize a new client, writing requests to fd.
 */
void client__init(struct client *c, int fd);

/**
 * Initialize the connection by writing the protocol version.
 */
int client__handshake(struct client *c);

/**
 * Release all memory used by the client, and close the client socket.
 */
void client__close(struct client *c);

#endif /* CLIENT_H_*/
