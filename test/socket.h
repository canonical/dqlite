/******************************************************************************
 *
 * Helper to create a connected pair of socket.
 *
 ******************************************************************************/

#ifndef DQLITE_TEST_SOCKET_H
#define DQLITE_TEST_SOCKET_H

struct test_socket_pair {
	int server; /* Server-side file descriptor */
	int client; /* Client-side file descriptor */

	int client_disconnected; /* Whether the client was disconnected by tests */
	int server_disconnected; /* Whether the server was disconnected by tests */

	/* Private */
	int listen; /* Listener file descriptor, for cleanup */
};

void test_socket_pair_init(struct test_socket_pair *p, const char *family);
void test_socket_pair_close(struct test_socket_pair *p);

void test_socket_pair_client_disconnect(struct test_socket_pair *p);
void test_socket_pair_server_disconnect(struct test_socket_pair *p);

#endif /* DQLITE_TEST_SOCKET_H */
