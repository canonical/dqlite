#include <pthread.h>
#include <time.h>

#include "../include/dqlite.h"

#include "./lib/runner.h"

TEST_MODULE(integration);

#if 0

/******************************************************************************
 *
 * Helpers
 *
 ******************************************************************************/

/* A worker that keeps inserting rows into a test table and fetching them back,
 * checking that they have been all inserted. */
struct worker
{
	struct testClient *client; /* A connected client */
	int i;			    /* Worker index */
	int a;			    /* Start inserting from this number */
	int n;			    /* Number of insertions to perform */
	pthread_t thread;	   /* System thread we run in */
};

static void *__worker_run(void *arg)
{
	struct worker *w;
	char *leader;
	uint64_t heartbeat;
	uint32_t dbId;
	int b;
	int i;

	munit_assert_ptr_not_null(arg);

	w = (struct worker *)arg;

	/* Initialize the connection and open a database. */
	testClient_handshake(w->client);
	testClient_leader(w->client, &leader);
	testClientClient(w->client, &heartbeat);
	testClient_open(w->client, "test.db", &dbId);

	b = w->a + w->n;

	for (i = w->a; i < b; i++) {
		uint32_t stmtId;
		char sql[128];
		struct testClient_result result;
		struct testClient_rows rows;
		struct testClient_row *row;
		int j;

		/* Insert a row in the test table. */
		sprintf(sql, "INSERT INTO test(n) VALUES(%d)", i);

		testClient_prepare(w->client, dbId, sql, &stmtId);
		testClient_exec(w->client, dbId, stmtId, &result);

		munit_assert_int(result.rowsAffected, ==, 1);

		testClient_finalize(w->client, dbId, stmtId);

		/* Fetch all rows within our own working range. */
		sprintf(sql, "SELECT n FROM test WHERE n >= %d AND n < %d",
			w->a, b);

		testClient_prepare(w->client, dbId, sql, &stmtId);
		testClient_query(w->client, dbId, stmtId, &rows);

		munit_assert_int(rows.column_count, ==, 1);
		munit_assert_string_equal(rows.columnNames[0], "n");

		row = rows.next;
		for (j = w->a; j <= i; j++) {
			munit_assert_ptr_not_null(row);

			munit_assert_int(row->types[0], ==, SQLITE_INTEGER);
			munit_assert_int(*(int64_t *)row->values[0], ==, j);

			row = row->next;
		}

		testClient_rows_close(&rows);
		testClient_finalize(w->client, dbId, stmtId);
	}

	return 0;
}

static void __worker_start(struct worker *w,
			   struct testServer *server,
			   int i,
			   int a,
			   int n)
{
	int err;

	w->i = i;
	w->a = a;
	w->n = n;

	testServer_connect(server, &w->client);

	err = pthread_create(&w->thread, 0, &__worker_run, (void *)w);
	if (err) {
		munit_errorf("failed to spawn test worker thread: %s",
			     strerror(errno));
	}
}

static void __worker_wait(struct worker *w)
{
	int err;
	void *retval;

	err = pthread_join(w->thread, &retval);
	if (err) {
		munit_errorf("failed to wait test worker thread: %s",
			     strerror(errno));
	}

	testClient_close(w->client);
	free(w->client);
}

/******************************************************************************
 *
 * Setup and tear down
 *
 ******************************************************************************/

static void *setup(const MunitParameter params[], void *userData)
{
	struct testServer *server;
	const char *errmsg;
	int err;

	(void)userData;
	(void)params;

	err = dqliteInit(&errmsg);
	munit_assert_int(err, ==, 0);

	server = testServer_start("unix", params);

	return server;
}

static void tear_down(void *data)
{
	struct testServer *server = data;
	int rc;

	testServer_stop(server);

	rc = sqlite3_shutdown();
	munit_assert_int(rc, ==, 0);
}

/******************************************************************************
 *
 * Tests
 *
 ******************************************************************************/

TEST_SUITE(exec);
TEST_SETUP(exec, setup);
TEST_TEAR_DOWN(exec, tear_down);

#include <unistd.h>

TEST_CASE(exec, singleQuery, NULL)
{
	struct testServer *server = data;
	struct testClient *client;
	char *leader;
	uint64_t heartbeat;
	uint32_t dbId;
	uint32_t stmtId;
	struct testClient_result result;
	struct testClient_rows rows;

	(void)params;

	testServer_connect(server, &client);

	/* Initialize the connection and open a database. */
	testClient_handshake(client);
	testClient_leader(client, &leader);
	testClientClient(client, &heartbeat);
	testClient_open(client, "test.db", &dbId);
	munit_assert_int(dbId, ==, 0);

	/* Create a test table. */
	testClient_prepare(client, dbId, "CREATE TABLE test (n INT)",
			    &stmtId);
	testClient_exec(client, dbId, stmtId, &result);
	testClient_finalize(client, dbId, stmtId);

	/* Insert a row in the test table. */
	testClient_prepare(client, dbId, "INSERT INTO test VALUES(123)",
			    &stmtId);

	munit_assert_int(stmtId, ==, 0);

	testClient_exec(client, dbId, stmtId, &result);

	munit_assert_int(result.lastInsertId, ==, 1);
	munit_assert_int(result.rowsAffected, ==, 1);

	testClient_finalize(client, dbId, stmtId);

	/* Select rows from the test table. */
	testClient_prepare(client, dbId, "SELECT n FROM test", &stmtId);

	munit_assert_int(stmtId, ==, 0);

	testClient_query(client, dbId, stmtId, &rows);

	munit_assert_int(rows.column_count, ==, 1);
	munit_assert_string_equal(rows.columnNames[0], "n");

	munit_assert_ptr_not_null(rows.next);
	munit_assert_int(rows.next->types[0], ==, SQLITE_INTEGER);
	munit_assert_int(*(int64_t *)rows.next->values[0], ==, 123);

	testClient_rows_close(&rows);

	testClient_finalize(client, dbId, stmtId);

	testClient_close(client);
	free(client);

	return MUNIT_OK;
}

TEST_CASE(exec, largeQuery, NULL)
{
	struct testServer *server = data;
	struct testClient *client;
	char *leader;
	uint64_t heartbeat;
	uint32_t dbId;
	uint32_t stmtId;
	struct testClient_result result;
	struct testClient_rows rows;
	int i;

	(void)params;

	testServer_connect(server, &client);

	/* Initialize the connection and open a database. */
	testClient_handshake(client);
	testClient_leader(client, &leader);
	testClientClient(client, &heartbeat);
	testClient_open(client, "test.db", &dbId);
	munit_assert_int(dbId, ==, 0);

	/* Create a test table. */
	testClient_prepare(client, dbId, "CREATE TABLE test (n INT)",
			    &stmtId);
	testClient_exec(client, dbId, stmtId, &result);
	testClient_finalize(client, dbId, stmtId);

	testClient_prepare(client, dbId, "BEGIN", &stmtId);
	testClient_exec(client, dbId, stmtId, &result);
	testClient_finalize(client, dbId, stmtId);

	/* Insert lots of rows in the test table. */
	testClient_prepare(client, dbId, "INSERT INTO test VALUES(123456789)",
			    &stmtId);

	for (i = 0; i < 256; i++) {
		munit_assert_int(stmtId, ==, 0);
		testClient_exec(client, dbId, stmtId, &result);
		munit_assert_int(result.rowsAffected, ==, 1);
	}

	testClient_finalize(client, dbId, stmtId);

	testClient_prepare(client, dbId, "COMMIT", &stmtId);
	testClient_exec(client, dbId, stmtId, &result);
	testClient_finalize(client, dbId, stmtId);

	/* Select all rows from the test table. */
	testClient_prepare(client, dbId, "SELECT n FROM test", &stmtId);

	munit_assert_int(stmtId, ==, 0);

	testClient_query(client, dbId, stmtId, &rows);

	munit_assert_int(rows.column_count, ==, 1);
	munit_assert_string_equal(rows.columnNames[0], "n");

	munit_assert_ptr_not_null(rows.next);
	munit_assert_int(rows.next->types[0], ==, SQLITE_INTEGER);
	munit_assert_int(*(int64_t *)rows.next->values[0], ==, 123456789);

	testClient_rows_close(&rows);

	testClient_finalize(client, dbId, stmtId);

	testClient_close(client);

	free(client);

	return MUNIT_OK;
}

TEST_CASE(exec, multiThread, NULL)
{
	struct testServer *server = data;
	struct worker *workers;
	struct testClient *client;
	struct testClient_result result;
	char *leader;
	uint64_t heartbeat;
	uint32_t dbId;
	uint32_t stmtId;

	(void)params;

	int n = 2;
	int i;

	testServer_connect(server, &client);

	/* Initialize the connection and open a database. */
	testClient_handshake(client);
	testClient_leader(client, &leader);
	testClientClient(client, &heartbeat);
	testClient_open(client, "test.db", &dbId);
	munit_assert_int(dbId, ==, 0);

	/* Create a test table and close this client. */
	testClient_prepare(client, dbId, "CREATE TABLE test (n INT)",
			    &stmtId);
	testClient_exec(client, dbId, stmtId, &result);
	testClient_finalize(client, dbId, stmtId);

	testClient_close(client);

	/* Spawn the workers. */
	workers = munit_malloc(n * sizeof *workers);

	for (i = 0; i < n; i++) {
		__worker_start(&(workers[i]), server, i, i * 100000, 4);
	}

	/* Wait for the workers. */
	for (i = 0; i < n; i++) {
		__worker_wait(&(workers[i]));
	}

	free(client);
	free(workers);

	return MUNIT_OK;
}

#endif
