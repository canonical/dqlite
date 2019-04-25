/* Helpers to create and connect Unix or TCP sockets. */

#ifndef TEST_ENDPOINT_H
#define TEST_ENDPOINT_H

#include <arpa/inet.h>
#include <sys/un.h>

#include "munit.h"

/* A few tests depend on knowing that certain reads and writes will not be short
 * and will happen immediately. */
#define TEST_SOCKET_MIN_BUF_SIZE 4096

/* Munit parameter defining the socket type to use in test_endpoint_setup.
 *
 * If set to "unix" a pair of unix abstract sockets will be created. If set to
 * "tcp" a pair of TCP sockets using the loopback interface will be created. */
#define TEST_ENDPOINT_FAMILY "endpoint-family"

/* Null-terminated list of legal values for TEST_ENDPOINT_FAMILY. Currently
 * "unix" and "tcp". */
extern char *test_endpoint_family_values[];

/* Listening socket endpoint. */
struct test_endpoint
{
	char address[256];  /* Rendered address string. */
	sa_family_t family; /* Address family (either AF_INET or AF_UNIX) */
	int fd;             /* Listening socket. */
	union {             /* Server  address (either a TCP or Unix) */
		struct sockaddr_in in_address;
		struct sockaddr_un un_address;
	};
};

/* Create a listening endpoint.
 *
 * This will bind a random address and start listening to it. */
void test_endpoint_setup(struct test_endpoint *e,
			 const MunitParameter params[]);

/* Tear down a listening endpoint. */
void test_endpoint_tear_down(struct test_endpoint *e);

/* Establish a new client connection. */
int test_endpoint_connect(struct test_endpoint *e);

/* Accept a new client connection. */
int test_endpoint_accept(struct test_endpoint *e);

/* Connect and accept a connection, returning the pair of connected sockets. */
void test_endpoint_pair(struct test_endpoint *e, int *server, int *client);

/* Return the endpoint address. */
const char *test_endpoint_address(struct test_endpoint *e);

#endif /* TEST_ENDPOINT_H */
