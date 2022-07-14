/* Setup fully blown servers running in standalone threads. */

#ifndef TEST_SERVER_H
#define TEST_SERVER_H

#include <pthread.h>
#include <sys/un.h>

#include "../../src/client.h"

#include "../../include/dqlite.h"

#include "endpoint.h"
#include "munit.h"

#define SNAPSHOT_THRESHOLD_PARAM "snapshot-threshold"

struct test_server
{
	unsigned id;                   /* Server ID. */
	char address[8];               /* Server address. */
	char *dir;                     /* Data directory. */
	dqlite_node *dqlite;           /* Dqlite instance. */
	struct client client;          /* Connected client. */
	struct test_server *others[5]; /* Other servers, by ID-1. */
};

/* Initialize the test server. */
void test_server_setup(struct test_server *s,
		       unsigned id,
		       const MunitParameter params[]);

/* Cleanup the test server. */
void test_server_tear_down(struct test_server *s);

/* Start the test server. */
void test_server_start(struct test_server *s, const MunitParameter params[]);

/* Stop the test server. */
void test_server_stop(struct test_server *s);

/* Connect all the given the servers to each other. */
void test_server_network(struct test_server *servers, unsigned n_servers);

/* Return a client connected to the server. */
struct client *test_server_client(struct test_server *s);

/* Closes and reopens a client connection to the server. */
void test_server_client_reconnect(struct test_server *s, struct client *c);

/* Opens a client connection to the server. */
void test_server_client_connect(struct test_server *s, struct client *c);

/* Closes a client connection to ther server. */
void test_server_client_close(struct test_server *s, struct client *c);

#endif /* TEST_SERVER_H */
