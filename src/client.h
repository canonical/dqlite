/**
 * Core dqlite client logic for encoding requests and decoding responses.
 */

#ifndef CLIENT_H_
#define CLIENT_H_

#include <stdint.h>

#include "lib/buffer.h"

struct client
{
	int fd;		     /* Connected socket */
	unsigned db_id;      /* Database ID provided by the server */
	struct buffer read;  /* Read buffer */
	struct buffer write; /* Write buffer */
};

/**
 * Initialize a new client, writing requests to fd.
 */
int client__init(struct client *c, int fd);

/**
 * Release all memory used by the client, and close the client socket.
 */
void client__close(struct client *c);

/**
 * Initialize the connection by writing the protocol version. This must be
 * called before using any other API.
 */
int client__send_handshake(struct client *c);

/**
 * Send a request to open a database
 */
int client__send_open(struct client *c, const char *name);

/**
 * Receive the response to an open request.
 */
int client__recv_db(struct client *c);

/**
 * Send a request to prepare a statement.
 */
int client__send_prepare(struct client *c, const char *sql);

/**
 * Receive the response to a prepare request.
 */
int client__recv_stmt(struct client *c, unsigned *stmt_id);


#endif /* CLIENT_H_*/
