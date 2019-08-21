/* Setup fully blown servers running in standalone threads. */

#ifndef TEST_SERVER_H
#define TEST_SERVER_H

#include <pthread.h>
#include <sys/un.h>

#include "../../src/client.h"

#include "../../include/dqlite.h"

#include "endpoint.h"
#include "munit.h"

struct test_server
{
	unsigned id;                   /* Server ID. */
	char address[8];               /* Server address. */
	char *dir;                     /* Data directory. */
	struct test_endpoint endpoint; /* For network connections. */
	dqlite_task *dqlite;           /* Dqlite instance. */
	pthread_t run;                 /* Main run loop thread. */
	struct client client;          /* Connected client. */
	struct test_server *others[5]; /* Other servers, by ID-1. */
	int state;                     /* Current server state */
};

/* Initialize the test server. */
void test_server_setup(struct test_server *s,
		       unsigned id,
		       const MunitParameter params[]);

/* Cleanup the test server. */
void test_server_tear_down(struct test_server *s);

/* Start the test server. */
void test_server_start(struct test_server *s);

/* Connect all the given the servers to each other. */
void test_server_network(struct test_server *servers, unsigned n_servers);

/* Return a client connected to the server. */
struct client *test_server_client(struct test_server *s);

#define FIXTURE_THREAD pthread_t thread;

#define THREAD_START(T, FUNC, DATA)                       \
	{                                                 \
		int rv2;                                  \
		rv2 = pthread_create(&T, 0, &FUNC, DATA); \
		munit_assert_int(rv2, ==, 0);             \
	}

#define THREAD_JOIN(T)                          \
	{                                       \
		void *retval;                   \
		int rv2;                        \
		rv2 = pthread_join(T, &retval); \
		munit_assert_int(rv2, ==, 0);   \
		munit_assert_ptr_null(retval);  \
	}

#endif /* TEST_SERVER_H */
