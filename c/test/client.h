#ifndef DQLITE_TEST_CLIENT_H
#define DQLITE_TEST_CLIENT_H

#include <stdint.h>

struct test_client {
	int fd;
};

/*
 * Initialize a test client.
 *
 * @fd: The file descriptor for writing requests and reading responses.
 */
void test_client_init(struct test_client *c, int fd);

/*
 * Deallocate the memory used by the test client, if any.
 */
void test_client_close(struct test_client *c);

/*
 * Initialize the client, writing the protocol version.
 */
int test_client_handshake(struct test_client* c);

/*
 * Send a Helo request.
 */
int test_client_helo(struct test_client *c, char **leader, uint8_t *heartbeat);

#endif /* DQLITE_TEST_CLIENT_H */
