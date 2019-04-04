#ifndef DQLITE_TEST_SERVER_H
#define DQLITE_TEST_SERVER_H

#include <sys/un.h>

#include "./lib/cluster.h"

#include "client.h"

struct test_server
{
	pthread_t thread;
	FIXTURE_CLUSTER;
	struct uv_idle_s idle;
	char *dir;
	dqlite *service;
	int family;
	union {
		struct sockaddr_in in_address;
		struct sockaddr_un un_address;
	};
	int socket;
};

struct test_server *test_server_start(const char *family,
				      const MunitParameter params[]);

void test_server_stop(struct test_server *);

void test_server_connect(struct test_server *t, struct test_client **client);

#endif /* DQLITE_TEST_SERVER_H */
