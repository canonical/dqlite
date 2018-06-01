#ifndef DQLITE_TEST_CLIENT_H
#define DQLITE_TEST_CLIENT_H

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
 * Send a Leader request.
 *
 * @address: The address of the current cluster leader.
 */
int test_client_leader(struct test_client *c, char **address);

#endif /* DQLITE_TEST_CLIENT_H */
