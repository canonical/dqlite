#ifndef DQLITE_TEST_SOCKET_H
#define DQLITE_TEST_SOCKET_H

struct test_socket_pair {
	int server; /* Server-side end of the pair */
	int client; /* Client-side end of the pair */

	int client_disconnected; /* Whether the client was disconnected by tests */
	int server_disconnected; /* Whether the server was disconnected by tests */

	/* Private */
	int listen;              /* Listener file descriptor, for cleanup */
};

int test_socket_pair_initialize(struct test_socket_pair *p);
int test_socket_pair_cleanup(struct test_socket_pair *p);

int test_socket_pair_client_disconnect(struct test_socket_pair *p);
int test_socket_pair_server_disconnect(struct test_socket_pair *p);

#endif /* DQLITE_TEST_SOCKET_H */
