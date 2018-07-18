#include <assert.h>

#include <sqlite3.h>

#include "../include/dqlite.h"

#include "replication.h"

static const char *test__cluster_replication(void *ctx) {
	(void)ctx;

	return test_replication()->zName;
}

static const char *test__cluster_leader(void *ctx) {
	(void)ctx;

	return "127.0.0.1:666";
}

static dqlite_server_info test__cluster_server_info_list[] = {
    {1, "1.2.3.4:666"},
    {2, "5.6.7.8:666"},
    {0, NULL},
};

static int test__cluster_servers_rc = SQLITE_OK;

static int test__cluster_servers(void *ctx, dqlite_server_info **servers) {
	(void)ctx;

	*servers = test__cluster_server_info_list;

	return test__cluster_servers_rc;
}

static void test__cluster_register(void *ctx, sqlite3 *db) {
	(void)ctx;
	(void)db;
}

static void test__cluster_unregister(void *ctx, sqlite3 *db) {
	(void)ctx;
	(void)db;
}

static int test__cluster_barrier(void *ctx) {
	(void)ctx;

	return 0;
}

static dqlite_cluster test__cluster = {
    NULL,
    test__cluster_replication,
    test__cluster_leader,
    test__cluster_servers,
    test__cluster_register,
    test__cluster_unregister,
    test__cluster_barrier,
    NULL,
};

dqlite_cluster *test_cluster() { return &test__cluster; }

void test_cluster_servers_rc(int rc) { test__cluster_servers_rc = rc; }
