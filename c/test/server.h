#ifndef DQLITE_TEST_SERVER_H
#define DQLITE_TEST_SERVER_H

#include "client.h"

typedef struct test_server test_server;

test_server *test_server_start();
int test_server_stop(test_server*);

int test_server_connect(test_server *t, struct test_client **client);

#endif /* DQLITE_TEST_SERVER_H */
