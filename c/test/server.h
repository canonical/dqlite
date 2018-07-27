#ifndef DQLITE_TEST_SERVER_H
#define DQLITE_TEST_SERVER_H

#include "client.h"

struct test_server *test_server_start();

void test_server_stop(struct test_server *);

void test_server_connect(struct test_server *t, struct test_client **client);

#endif /* DQLITE_TEST_SERVER_H */
