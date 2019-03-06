/******************************************************************************
 *
 * Helper to create a connected pair of socket.
 *
 ******************************************************************************/

#ifndef DQLITE_TEST_SOCKET_H
#define DQLITE_TEST_SOCKET_H

#include <stdbool.h>

#include "munit.h"

/* A few tests depend on knowing that certain reads and writes will not be short
 * and will happen immediately. */
#define TEST_SOCKET_MIN_BUF_SIZE 4096

/**
 * Munit parameter defining the socket type to use in test_socket_pair_setup.
 *
 * If set to "unix" a pair of unix abstract sockets will be created. If set to
 * "tcp" a pair of TCP sockets using the loopback interface will be created.
 */
#define TEST_SOCKET_FAMILY "socket-family"

/**
 * Null-terminated list of legal values for TEST_SOCKET_FAMILY. Currently "unix"
 * and "tcp".
 */
extern char *test_socket_param_values[];

struct test_socket_pair
{
	int server;		  /* Server-side file descriptor */
	int client;		  /* Client-side file descriptor */
	int listen;		  /* Listener file descriptor, for cleanup */
	bool server_disconnected; /* If the server was disconnected by tests */
	bool client_disconnected; /* If the client was disconnected by tests */
};

/**
 * Setup a socket pair.
 *
 * The server side of the socket will be available in p->server and the client
 * side in p->client. The listening socket that was used to create the
 * server/client pair will be available in p->listen.
 *
 * By default p->server will be set to non-blocking mode, while p->client won't.
 */
void test_socket_pair_setup(const MunitParameter params[],
			    struct test_socket_pair *p);

/**
 * Tear down a socket pair, closing all open file descriptors.
 *
 * If p->server_disconnected is true, then the tear down logic assumes that the
 * server socket has been closed by the test and will check that. Likewise for
 * the p->client_disconnected flag.
 */
void test_socket_pair_tear_down(struct test_socket_pair *p);

/**
 * Close the client socket.
 */
void test_socket_pair_client_disconnect(struct test_socket_pair *p);

/**
 * Close the server socket.
 */
void test_socket_pair_server_disconnect(struct test_socket_pair *p);

#endif /* DQLITE_TEST_SOCKET_H */
