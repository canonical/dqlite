#include <assert.h>
#include <pthread.h>
#include <time.h>

#include "../include/dqlite.h"

#include "client.h"
#include "leak.h"
#include "munit.h"
#include "server.h"

/******************************************************************************
 *
 * Helpers
 *
 ******************************************************************************/

/* A worker that keeps inserting rows into a test table and fetching them back,
 * checking that they have been all inserted. */
struct worker {
	struct test_client *client; /* A connected client */
	int                 i;      /* Worker index */
	int                 a;      /* Start inserting from this number */
	int                 n;      /* Number of insertions to perform */
	pthread_t           thread; /* System thread we run in */
};

static void *__worker_run(void *arg)
{
	struct worker *w;
	char *         leader;
	uint64_t       heartbeat;
	uint32_t       db_id;
	int            b;
	int            i;

	munit_assert_ptr_not_null(arg);

	w = (struct worker *)arg;

	/* Initialize the connection and open a database. */
	test_client_handshake(w->client);
	test_client_leader(w->client, &leader);
	test_client_client(w->client, &heartbeat);
	test_client_open(w->client, "test.db", &db_id);

	b = w->a + w->n;

	for (i = w->a; i < b; i++) {
		uint32_t                  stmt_id;
		char                      sql[128];
		struct test_client_result result;
		struct test_client_rows   rows;
		struct test_client_row *  row;
		int                       j;

		/* Insert a row in the test table. */
		sprintf(sql, "INSERT INTO test(n) VALUES(%d)", i);

		test_client_prepare(w->client, db_id, sql, &stmt_id);
		test_client_exec(w->client, db_id, stmt_id, &result);

		munit_assert_int(result.rows_affected, ==, 1);

		test_client_finalize(w->client, db_id, stmt_id);

		/* Fetch all rows within our own working range. */
		sprintf(sql,
		        "SELECT n FROM test WHERE n >= %d AND n < %d",
		        w->a,
		        b);

		test_client_prepare(w->client, db_id, sql, &stmt_id);
		test_client_query(w->client, db_id, stmt_id, &rows);

		munit_assert_int(rows.column_count, ==, 1);
		munit_assert_string_equal(rows.column_names[0], "n");

		row = rows.next;
		for (j = w->a; j <= i; j++) {
			munit_assert_ptr_not_null(row);

			munit_assert_int(row->types[0], ==, SQLITE_INTEGER);
			munit_assert_int(*(int64_t *)row->values[0], ==, j);

			row = row->next;
		}

		test_client_rows_close(&rows);
		test_client_finalize(w->client, db_id, stmt_id);
	}

	return 0;
}

static void __worker_start(struct worker *     w,
                           struct test_server *server,
                           int                 i,
                           int                 a,
                           int                 n)
{
	int err;

	w->i = i;
	w->a = a;
	w->n = n;

	test_server_connect(server, &w->client);

	err = pthread_create(&w->thread, 0, &__worker_run, (void *)w);
	if (err) {
		munit_errorf("failed to spawn test worker thread: %s",
		             strerror(errno));
	}
}

static void __worker_wait(struct worker *w)
{
	int   err;
	void *retval;

	err = pthread_join(w->thread, &retval);
	if (err) {
		munit_errorf("failed to wait test worker thread: %s",
		             strerror(errno));
	}

	test_client_close(w->client);
}

/******************************************************************************
 *
 * Setup and tear down
 *
 ******************************************************************************/

static void *setup(const MunitParameter params[], void *user_data)
{
	struct test_server *server;
	const char *        errmsg;
	int                 err;

	(void)params;
	(void)user_data;

	err = dqlite_init(&errmsg);
	munit_assert_int(err, ==, 0);

	server = test_server_start("unix");

	return server;
}

static void tear_down(void *data)
{
	struct test_server *server = data;
	int                 rc;

	test_server_stop(server);

	rc = sqlite3_shutdown();
	munit_assert_int(rc, ==, 0);

	/* TODO: the instance tracking in lifecycle.c is not thread-safe, nor is
	 * the one in sqlite3_malloc/free (since we disable mutexes). */
	/* test_assert_no_leaks(); */
}

/******************************************************************************
 *
 * Tests
 *
 ******************************************************************************/

static MunitResult test_exec_and_query(const MunitParameter params[],
                                       void *               data)
{
	struct test_server *      server = data;
	struct test_client *      client;
	char *                    leader;
	uint64_t                  heartbeat;
	uint32_t                  db_id;
	uint32_t                  stmt_id;
	struct test_client_result result;
	struct test_client_rows   rows;

	(void)params;

	test_server_connect(server, &client);

	/* Initialize the connection and open a database. */
	test_client_handshake(client);
	test_client_leader(client, &leader);
	test_client_client(client, &heartbeat);
	test_client_open(client, "test.db", &db_id);
	munit_assert_int(db_id, ==, 0);

	/* Create a test table. */
	test_client_prepare(
	    client, db_id, "CREATE TABLE test (n INT)", &stmt_id);
	test_client_exec(client, db_id, stmt_id, &result);
	test_client_finalize(client, db_id, stmt_id);

	/* Insert a row in the test table. */
	test_client_prepare(
	    client, db_id, "INSERT INTO test VALUES(123)", &stmt_id);

	munit_assert_int(stmt_id, ==, 0);

	test_client_exec(client, db_id, stmt_id, &result);

	munit_assert_int(result.last_insert_id, ==, 1);
	munit_assert_int(result.rows_affected, ==, 1);

	test_client_finalize(client, db_id, stmt_id);

	/* Select rows from the test table. */
	test_client_prepare(client, db_id, "SELECT n FROM test", &stmt_id);

	munit_assert_int(stmt_id, ==, 0);

	test_client_query(client, db_id, stmt_id, &rows);

	munit_assert_int(rows.column_count, ==, 1);
	munit_assert_string_equal(rows.column_names[0], "n");

	munit_assert_ptr_not_null(rows.next);
	munit_assert_int(rows.next->types[0], ==, SQLITE_INTEGER);
	munit_assert_int(*(int64_t *)rows.next->values[0], ==, 123);

	test_client_rows_close(&rows);

	test_client_finalize(client, db_id, stmt_id);

	test_client_close(client);

	return MUNIT_OK;
}

static MunitResult test_query_large(const MunitParameter params[], void *data)
{
	struct test_server *      server = data;
	struct test_client *      client;
	char *                    leader;
	uint64_t                  heartbeat;
	uint32_t                  db_id;
	uint32_t                  stmt_id;
	struct test_client_result result;
	struct test_client_rows   rows;
	int                       i;

	(void)params;

	test_server_connect(server, &client);

	/* Initialize the connection and open a database. */
	test_client_handshake(client);
	test_client_leader(client, &leader);
	test_client_client(client, &heartbeat);
	test_client_open(client, "test.db", &db_id);
	munit_assert_int(db_id, ==, 0);

	/* Create a test table. */
	test_client_prepare(
	    client, db_id, "CREATE TABLE test (n INT)", &stmt_id);
	test_client_exec(client, db_id, stmt_id, &result);
	test_client_finalize(client, db_id, stmt_id);

	/* Insert lots of rows in the test table. */
	test_client_prepare(
	    client, db_id, "INSERT INTO test VALUES(123456789)", &stmt_id);

	for (i = 0; i < 256; i++) {

		munit_assert_int(stmt_id, ==, 0);

		test_client_exec(client, db_id, stmt_id, &result);

		munit_assert_int(result.rows_affected, ==, 1);
	}

	test_client_finalize(client, db_id, stmt_id);

	/* Select all rows from the test table. */
	test_client_prepare(client, db_id, "SELECT n FROM test", &stmt_id);

	munit_assert_int(stmt_id, ==, 0);

	test_client_query(client, db_id, stmt_id, &rows);

	munit_assert_int(rows.column_count, ==, 1);
	munit_assert_string_equal(rows.column_names[0], "n");

	munit_assert_ptr_not_null(rows.next);
	munit_assert_int(rows.next->types[0], ==, SQLITE_INTEGER);
	munit_assert_int(*(int64_t *)rows.next->values[0], ==, 123456789);

	test_client_rows_close(&rows);

	test_client_finalize(client, db_id, stmt_id);

	test_client_close(client);

	return MUNIT_OK;
}

static MunitResult test_multi_thread(const MunitParameter params[], void *data)
{
	struct test_server *      server = data;
	struct worker *           workers;
	struct test_client *      client;
	struct test_client_result result;
	char *                    leader;
	uint64_t                  heartbeat;
	uint32_t                  db_id;
	uint32_t                  stmt_id;

	(void)params;

	int n = 2;
	int i;

	test_server_connect(server, &client);

	/* Initialize the connection and open a database. */
	test_client_handshake(client);
	test_client_leader(client, &leader);
	test_client_client(client, &heartbeat);
	test_client_open(client, "test.db", &db_id);
	munit_assert_int(db_id, ==, 0);

	/* Create a test table and close this client. */
	test_client_prepare(
	    client, db_id, "CREATE TABLE test (n INT)", &stmt_id);
	test_client_exec(client, db_id, stmt_id, &result);
	test_client_finalize(client, db_id, stmt_id);

	test_client_close(client);

	/* Spawn the workers. */
	workers = munit_malloc(n * sizeof *workers);

	for (i = 0; i < n; i++) {
		__worker_start(&(workers[i]), server, i, i * 100000, 4);
	}

	/* Wait for the workers. */
	for (i = 0; i < n; i++) {
		__worker_wait(&(workers[i]));
	}

	return MUNIT_OK;
}

static MunitTest dqlite__integration_tests[] = {
    {"/exec-and-query", test_exec_and_query, setup, tear_down, 0, NULL},
    {"/query-large", test_query_large, setup, tear_down, 0, NULL},
    {"/multi-thread", test_multi_thread, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * Suite
 *
 ******************************************************************************/

MunitSuite dqlite__integration_suites[] = {
    {"", dqlite__integration_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE},
    {NULL, NULL, NULL, 0, 0},
};
