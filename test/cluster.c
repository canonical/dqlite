#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include <sqlite3.h>

#include "../include/dqlite.h"
#include "../src/db.h"

#include "munit.h"

static struct test__cluster_ctx {
	sqlite3 **db_list; /* Track registered connections */
} test__cluster_ctx;

static const char *test__cluster_leader(void *ctx)
{
	char *address;

	(void)ctx;

	/* Allocate a string, as regular implementations of the cluster
	 * interface are expected to do. */
	address = malloc(strlen("127.0.0.1:666") + 1);
	if (address == NULL) {
		return NULL;
	}

	strcpy(address, "127.0.0.1:666");

	return address;
}

static int test__cluster_servers_rc = SQLITE_OK;

static int test__cluster_servers(void *ctx, dqlite_server_info **servers)
{
	(void)ctx;

	if (test__cluster_servers_rc != 0) {
		*servers = NULL;
		return test__cluster_servers_rc;
	}

	/* Allocate the servers array, as regular implementations of the cluster
	 * interface are expected to do. */
	*servers = malloc(3 * sizeof **servers);

	(*servers)[0].id      = 1;
	(*servers)[0].address = malloc(strlen("1.2.3.4:666") + 1);
	strcpy((char *)(*servers)[0].address, "1.2.3.4:666");

	(*servers)[1].id      = 2;
	(*servers)[1].address = malloc(strlen("5.6.7.8:666") + 1);
	strcpy((char *)(*servers)[1].address, "5.6.7.8:666");

	(*servers)[2].id      = 0;
	(*servers)[2].address = NULL;

	return 0;
}

static void test__cluster_register(void *arg, sqlite3 *db)
{
	struct test__cluster_ctx *ctx;
	sqlite3 **                cursor;
	sqlite3 **                new_db_list;
	int                       n = 1;

	munit_assert_ptr_not_null(arg);
	munit_assert_ptr_not_null(db);

	ctx = arg;

	munit_assert_ptr_not_null(ctx->db_list);

	/* Count all currently registered dbs and assert that the given db is
	 * not among them. */
	for (cursor = ctx->db_list; *cursor != NULL; cursor++) {
		munit_assert_ptr_not_equal(*cursor, db);
		n++;
	}

	/* Append the new db at the end of the list. */
	new_db_list = munit_malloc((n + 1) * sizeof(sqlite3 *));
	memcpy(new_db_list, ctx->db_list, n * sizeof(sqlite3 *));

	ctx->db_list = new_db_list;

	*(ctx->db_list + (n - 1)) = db;
	*(ctx->db_list + n)       = NULL;
}

static void test__cluster_unregister(void *arg, sqlite3 *db)
{
	struct test__cluster_ctx *ctx;
	sqlite3 **                cursor;
	sqlite3 **                new_db_list;
	int                       n = 1;
	int                       i = -1;

	munit_assert_ptr_not_null(arg);
	munit_assert_ptr_not_null(db);

	ctx = arg;

	munit_assert_ptr_not_null(ctx->db_list);

	/* Count all currently registered dbs and assert that the given db is
	 * among them. */
	for (cursor = ctx->db_list; *cursor != NULL; cursor++) {
		if (*cursor == db) {
			i = n - 1;
		}
		n++;
	}
	munit_assert_int(i, >=, 0);

	/* Create a new list and copy all dbs except the given one. */
	i           = 0;
	new_db_list = munit_malloc(n * sizeof(sqlite3 *));

	for (cursor = ctx->db_list; *cursor != NULL; cursor++) {
		if (*cursor == db) {
			continue;
		}
		*(new_db_list + i) = *cursor;
		i++;
	}

	*(new_db_list + n - 1) = NULL;

	ctx->db_list = new_db_list;
}

static int test__cluster_barrier(void *ctx)
{
	(void)ctx;

	return 0;
}

static int test__cluster_checkpoint(void *ctx, sqlite3 *db)
{
	int rc;
	int log;
	int ckpt;

	(void)ctx;

	rc = sqlite3_wal_checkpoint_v2(
	    db, "main", SQLITE_CHECKPOINT_TRUNCATE, &log, &ckpt);
	munit_assert_int(rc, ==, 0);

	munit_assert_int(log, ==, 0);
	munit_assert_int(ckpt, ==, 0);

	return 0;
}

static dqlite_cluster test__cluster = {
    &test__cluster_ctx,
    test__cluster_leader,
    test__cluster_servers,
    test__cluster_register,
    test__cluster_unregister,
    test__cluster_barrier,
    NULL,
    test__cluster_checkpoint,
};

dqlite_cluster *test_cluster()
{
	test__cluster_ctx.db_list = munit_malloc(sizeof(sqlite3 *));

	*test__cluster_ctx.db_list = NULL;

	return &test__cluster;
}

void test_cluster_servers_rc(int rc) { test__cluster_servers_rc = rc; }
