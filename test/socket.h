/******************************************************************************
 *
 * Helper to create a connected pair of socket.
 *
 ******************************************************************************/

#ifndef DQLITE_TEST_SOCKET_H
#define DQLITE_TEST_SOCKET_H

#include "munit.h"

/* Munit parameter defining the socket type to use in test_socket_pair_setup. */
#define TEST_SOCKET_PARAM "socket-family"

extern char *test_socket_param_values[];

struct test_socket_pair {
	int server;              /* Server-side file descriptor */
	int client;              /* Client-side file descriptor */
	int server_disconnected; /* If the server was disconnected by tests */
	int client_disconnected; /* If the client was disconnected by tests */
	int listen;              /* Listener file descriptor, for cleanup */
};

void test_socket_pair_setup(const MunitParameter     params[],
                            struct test_socket_pair *p);

void test_socket_pair_tear_down(struct test_socket_pair *p);

void test_socket_pair_client_disconnect(struct test_socket_pair *p);
void test_socket_pair_server_disconnect(struct test_socket_pair *p);

#endif /* DQLITE_TEST_SOCKET_H */
