#ifndef DQLITE_TEST_CLUSTER_H
#define DQLITE_TEST_CLUSTER_H

#include "../include/dqlite.h"

dqlite_cluster *test_cluster();

/* Set the return code of the xServers method. */
void test_cluster_servers_rc(int rc);

#endif /* DQLITE_TEST_CLUSTER_H */
