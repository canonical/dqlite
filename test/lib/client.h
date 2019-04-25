/* Setup a test dqlite client. */

#include "endpoint.h"

#ifndef TEST_CLIENT_H
#define TEST_CLIENT_H

#define FIXTURE_CLIENT                 \
	struct client client;          \
	struct test_endpoint endpoint; \
	int server

#define SETUP_CLIENT                                                       \
	{                                                                  \
		int client_;                                               \
		test_endpoint_setup(&f->endpoint, params);                 \
		test_endpoint_connect(&f->endpoint, &f->server, &client_); \
		clientInit(&f->client, client_);                           \
	}

#define TEAR_DOWN_CLIENT         \
	clientClose(&f->client); \
	test_endpoint_tear_down(&f->endpoint)

#endif /* TEST_CLIENT_H */
