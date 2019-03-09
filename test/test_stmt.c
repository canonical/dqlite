#include <stdint.h>
#include <time.h>

#include <sqlite3.h>

#include "../include/dqlite.h"

#include "../src/lib/byte.h"
#include "../src/stmt.h"

#include "case.h"
#include "log.h"
#include "./lib/runner.h"

TEST_MODULE(stmt);

/******************************************************************************
 *
 * Helpers
 *
 ******************************************************************************/

struct fixture
{
	sqlite3_vfs *vfs;
	struct stmt *stmt;
	struct message *message;
	dqlite_logger * logger;
};

/* Helper to execute a statement. */
static void __exec(struct fixture *f, const char *sql)
{
	char *errmsg;
	int rc;

	rc = sqlite3_exec(f->stmt->db, sql, NULL, NULL, &errmsg);
	munit_assert_int(rc, ==, SQLITE_OK);
}

/* Helper to prepare a statement. */
static void __prepare(struct fixture *f, const char *sql)
{
	const char *tail;
	int rc;

	munit_assert_ptr_not_equal(f->stmt->db, NULL);

	rc = sqlite3_prepare(f->stmt->db, sql, -1, &f->stmt->stmt, &tail);
	munit_assert_int(rc, ==, SQLITE_OK);
}

/******************************************************************************
 *
 * Setup and tear down
 *
 ******************************************************************************/

static void *setup(const MunitParameter params[], void *user_data)
{
	struct fixture *f = munit_malloc(sizeof *f);
	int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	int rc;

	test_case_setup(params, user_data);

	f->logger = test_logger();

	/* Register a volatile VFS. */
	f->vfs = dqlite_vfs_create("test", f->logger);
	munit_assert_ptr_not_null(f->vfs);
	sqlite3_vfs_register(f->vfs, 0);

	/* Create a stmt object associated with a database. */
	f->stmt = munit_malloc(sizeof *f->stmt);
	stmt__init(f->stmt);

	rc = sqlite3_open_v2("test.db:", &f->stmt->db, flags, "test");
	munit_assert_int(rc, ==, SQLITE_OK);

	__exec(f, "PRAGMA synchronous=OFF");

	/* Create a message object */
	f->message = munit_malloc(sizeof *f->message);
	message__init(f->message);

	return f;
}

static void tear_down(void *data)
{
	struct fixture *f = data;

	message__close(f->message);

	sqlite3_close_v2(f->stmt->db);
	stmt__close(f->stmt);

	sqlite3_vfs_unregister(f->vfs);
	dqlite_vfs_destroy(f->vfs);

	test_case_tear_down(data);

	free(f->stmt);
	free(f->message);
	free(f->logger);
	free(f);
}

/******************************************************************************
 *
 * stmt__bind
 *
 ******************************************************************************/

TEST_SUITE(bind);
TEST_SETUP(bind, setup);
TEST_TEAR_DOWN(bind, tear_down);

/* If a message carries no bindings, stmt__bind is a no-op. */
TEST_CASE(bind, none, NULL)
{
	struct fixture *f = data;
	int rc;

	(void)params;

	__prepare(f, "SELECT 1");

	rc = stmt__bind(f->stmt, f->message);
	munit_assert_int(rc, ==, SQLITE_OK);

	return MUNIT_OK;
}

/* If a message ends before all expected param types are read, an error is
 * returned. */
TEST_CASE(bind, missing_types, NULL)
{
	struct fixture *f = data;
	int rc;

	(void)params;

	__prepare(f, "SELECT ?");

	/* Eight parameters, but only 7 bytes left in the message after the
	 * parameters count. */
	f->message->words = 1;
	f->message->body1[0] = 8;

	rc = stmt__bind(f->stmt, f->message);
	munit_assert_int(rc, ==, SQLITE_ERROR);

	munit_assert_string_equal(f->stmt->error, "incomplete param types");

	return MUNIT_OK;
}

/* If a message ends right after the parameter types, providing no parameter
 * values, an error is returned. */
TEST_CASE(bind, no_params, NULL)
{
	struct fixture *f = data;
	int rc;

	(void)params;

	__prepare(f, "SELECT ?");

	/* One parameter of integer type, but no more words left in the
	 * message. */
	f->message->words = 1;
	f->message->body1[0] = 1;
	f->message->body1[1] = SQLITE_INTEGER;

	rc = stmt__bind(f->stmt, f->message);
	munit_assert_int(rc, ==, SQLITE_ERROR);

	munit_assert_string_equal(f->stmt->error, "incomplete param values");

	return MUNIT_OK;
}

/* If a message ends before all expected parameters are read, an error is
 * returned.
 */
TEST_CASE(bind, missing_params, NULL)
{
	struct fixture *f = data;
	int rc;

	(void)params;

	__prepare(f, "SELECT ?");

	/* Two parameters of integer type, but only on word left in the
	 * message. */
	f->message->words = 2;
	f->message->body1[0] = 2;
	f->message->body1[1] = SQLITE_INTEGER;
	f->message->body1[2] = SQLITE_INTEGER;

	rc = stmt__bind(f->stmt, f->message);
	munit_assert_int(rc, ==, SQLITE_ERROR);

	munit_assert_string_equal(f->stmt->error, "incomplete param values");

	return MUNIT_OK;
}

/* If a message sports an unknown parameter type, an error is returned. */
TEST_CASE(bind, bad_type, NULL)
{
	struct fixture *f = data;
	int rc;

	(void)params;

	__prepare(f, "SELECT ?");

	/* One parameter of unknown type. */
	f->message->words = 2;
	f->message->body1[0] = 1;
	f->message->body1[1] = 127;

	rc = stmt__bind(f->stmt, f->message);
	munit_assert_int(rc, ==, SQLITE_ERROR);

	munit_assert_string_equal(f->stmt->error,
				  "invalid param 1: unknown type 127");

	return MUNIT_OK;
}

/* If a parameter fails to be bound, an error is returned. */
TEST_CASE(bind, bad_param, NULL)
{
	struct fixture *f = data;
	int rc;

	(void)params;

	/* Prepare a statement with no parameters. */
	__prepare(f, "SELECT 1");

	/* A single integer parameter. */
	f->message->words = 2;
	f->message->body1[0] = 1;
	f->message->body1[1] = SQLITE_INTEGER;

	rc = stmt__bind(f->stmt, f->message);
	munit_assert_int(rc, ==, SQLITE_RANGE);

	munit_assert_string_equal(f->stmt->error, "column index out of range");

	return MUNIT_OK;
}

/* Bind a parameter of type integer. */
TEST_CASE(bind, integer, NULL)
{
	struct fixture *f = data;
	int rc;
	uint64_t buf = byte__flip64((uint64_t)(-666));

	(void)params;

	__prepare(f, "SELECT ?");

	/* One parameter of type integer. */
	f->message->words = 2;
	f->message->body1[0] = 1;
	f->message->body1[1] = SQLITE_INTEGER;

	memcpy(f->message->body1 + 8, &buf, sizeof buf);

	rc = stmt__bind(f->stmt, f->message);
	munit_assert_int(rc, ==, SQLITE_OK);

	/* The float parameter was correctly bound. */
	rc = sqlite3_step(f->stmt->stmt);
	munit_assert_int(rc, ==, SQLITE_ROW);

	munit_assert_int(sqlite3_column_type(f->stmt->stmt, 0), ==,
			 SQLITE_INTEGER);
	munit_assert_int(sqlite3_column_int64(f->stmt->stmt, 0), ==, -666);

	return MUNIT_OK;
}

/* Bind a parameter of type float. */
TEST_CASE(bind, float, NULL)
{
	struct fixture *f = data;
	int rc;
	double float_ = 3.1415;
	uint64_t buf;

	(void)params;

	__prepare(f, "SELECT ?");

	/* One parameter of type double. */
	f->message->words = 2;
	f->message->body1[0] = 1;
	f->message->body1[1] = SQLITE_FLOAT;

	memcpy(&buf, &float_, sizeof float_);
	buf = byte__flip64(buf);
	memcpy(f->message->body1 + 8, &buf, sizeof buf);

	rc = stmt__bind(f->stmt, f->message);
	munit_assert_int(rc, ==, SQLITE_OK);

	/* The float parameter was correctly bound. */
	rc = sqlite3_step(f->stmt->stmt);
	munit_assert_int(rc, ==, SQLITE_ROW);

	munit_assert_int(sqlite3_column_type(f->stmt->stmt, 0), ==,
			 SQLITE_FLOAT);
	munit_assert_double(sqlite3_column_double(f->stmt->stmt, 0), ==,
			    3.1415);

	return MUNIT_OK;
}

/* Bind a parameter of type text. */
TEST_CASE(bind, text, NULL)
{
	struct fixture *f = data;
	int rc;

	(void)params;

	__prepare(f, "SELECT ?");

	/* One parameter of type string. */
	f->message->words = 2;
	f->message->body1[0] = 1;
	f->message->body1[1] = SQLITE_TEXT;

	strcpy(f->message->body1 + 8, "hello");

	rc = stmt__bind(f->stmt, f->message);
	munit_assert_int(rc, ==, SQLITE_OK);

	/* The float parameter was correctly bound. */
	rc = sqlite3_step(f->stmt->stmt);
	munit_assert_int(rc, ==, SQLITE_ROW);

	munit_assert_int(sqlite3_column_type(f->stmt->stmt, 0), ==,
			 SQLITE_TEXT);
	munit_assert_string_equal(
	    (const char *)sqlite3_column_text(f->stmt->stmt, 0), "hello");

	return MUNIT_OK;
}

/* Bind a parameter of type iso8601. */
TEST_CASE(bind, iso8601, NULL)
{
	struct fixture *f = data;
	int rc;

	(void)params;

	__prepare(f, "SELECT ?");

	/* One parameter of type string. */
	f->message->words = 5;
	f->message->body1[0] = 1;
	f->message->body1[1] = DQLITE_ISO8601;

	strcpy(f->message->body1 + 8, "2018-07-20 09:49:05+00:00");

	rc = stmt__bind(f->stmt, f->message);
	munit_assert_int(rc, ==, SQLITE_OK);

	/* The float parameter was correctly bound. */
	rc = sqlite3_step(f->stmt->stmt);
	munit_assert_int(rc, ==, SQLITE_ROW);

	munit_assert_int(sqlite3_column_type(f->stmt->stmt, 0), ==,
			 SQLITE_TEXT);
	munit_assert_string_equal(
	    (const char *)sqlite3_column_text(f->stmt->stmt, 0),
	    "2018-07-20 09:49:05+00:00");

	return MUNIT_OK;
}

/******************************************************************************
 *
 * stmt__query
 *
 ******************************************************************************/

TEST_SUITE(query);
TEST_SETUP(query, setup);
TEST_TEAR_DOWN(query, tear_down);

/* If a statement doesn't yield any column, an error is returned. */
TEST_CASE(query, no_columns, NULL)
{
	struct fixture *f = data;
	int rc;

	(void)params;

	__exec(f, "CREATE TABLE test (n INT)");

	/* This statement yields no columns. */
	__prepare(f, "DELETE FROM test");

	rc = stmt__query(f->stmt, f->message);
	munit_assert_int(rc, ==, SQLITE_ERROR);

	munit_assert_string_equal(f->stmt->error,
				  "stmt doesn't yield any column");

	return MUNIT_OK;
}

/* Encode a query yielding no rows. */
TEST_CASE(query, none, NULL)
{
	struct fixture *f = data;
	int rc;
	uint64_t *buf;
	const char *text;

	(void)params;

	__prepare(f, "SELECT name FROM sqlite_master");

	rc = stmt__query(f->stmt, f->message);
	munit_assert_int(rc, ==, SQLITE_DONE);

	/* The first word written is the column count. */
	buf = (uint64_t *)f->message->body1;
	munit_assert_int(byte__flip64(*buf), ==, 1);

	/* Then the column name. */
	text = (const char *)(f->message->body1 + 8);
	munit_assert_string_equal(text, "name");

	/* That's it. */
	munit_assert_int(f->message->offset1, ==, 16);

	return MUNIT_OK;
}

/* Encode a query yielding a single row with an integer column. */
TEST_CASE(query, integer, NULL)
{
	struct fixture *f = data;
	int rc;
	uint64_t *buf;
	const char *text;

	(void)params;

	/* Create a test table and insert a row into it. */
	__exec(f, "CREATE TABLE test (n INT)");
	__exec(f, "INSERT INTO test VALUES(-123)");

	__prepare(f, "SELECT n FROM test");

	rc = stmt__query(f->stmt, f->message);
	munit_assert_int(rc, ==, SQLITE_DONE);

	/* The first word written is the column count. */
	buf = (uint64_t *)f->message->body1;
	munit_assert_int(byte__flip64(*buf), ==, 1);

	/* Then the column name. */
	text = (const char *)(f->message->body1 + 8);
	munit_assert_string_equal(text, "n");

	/* Then the row, with its header and value. */
	munit_assert_int(f->message->body1[16], ==, SQLITE_INTEGER);
	buf = (uint64_t *)(f->message->body1 + 24);
	munit_assert_int((int64_t)(byte__flip64(*buf)), ==, -123);

	return MUNIT_OK;
}

/* Encode a query yielding a single row with a float column. */
TEST_CASE(query, float, NULL)
{
	struct fixture *f = data;
	int rc;
	uint64_t *buf;
	const char *text;

	(void)params;

	/* Create a test table and insert a row into it. */
	__exec(f, "CREATE TABLE test (f FLOAT)");
	__exec(f, "INSERT INTO test VALUES(3.1415)");

	__prepare(f, "SELECT f FROM test");

	rc = stmt__query(f->stmt, f->message);
	munit_assert_int(rc, ==, SQLITE_DONE);

	/* The first word written is the column count. */
	buf = (uint64_t *)f->message->body1;
	munit_assert_int(byte__flip64(*buf), ==, 1);

	/* Then the column name. */
	text = (const char *)(f->message->body1 + 8);
	munit_assert_string_equal(text, "f");

	/* Then the row, with its header and value. */
	munit_assert_int(f->message->body1[16], ==, SQLITE_FLOAT);
	buf = (uint64_t *)(f->message->body1 + 24);
	*buf = byte__flip64(*buf);
	munit_assert_double(*(double *)(buf), ==, 3.1415);

	return MUNIT_OK;
}

/* Encode a query yielding a single row with a null column. */
TEST_CASE(query, null, NULL)
{
	struct fixture *f = data;
	int rc;
	uint64_t *buf;
	const char *text;

	(void)params;

	/* Create a test table and insert a row into it. */
	__exec(f, "CREATE TABLE test (t TEXT)");
	__exec(f, "INSERT INTO test VALUES(NULL)");

	__prepare(f, "SELECT t FROM test");

	rc = stmt__query(f->stmt, f->message);
	munit_assert_int(rc, ==, SQLITE_DONE);

	/* The first word written is the column count. */
	buf = (uint64_t *)f->message->body1;
	munit_assert_int(byte__flip64(*buf), ==, 1);

	/* Then the column name. */
	text = (const char *)(f->message->body1 + 8);
	munit_assert_string_equal(text, "t");

	/* Then the row, with its header and value. */
	munit_assert_int(f->message->body1[16], ==, SQLITE_NULL);
	buf = (uint64_t *)(f->message->body1 + 24);
	munit_assert_int((int64_t)(byte__flip64(*buf)), ==, 0);

	return MUNIT_OK;
}

/* Encode a query yielding a single row with a text column. */
TEST_CASE(query, text, NULL)
{
	struct fixture *f = data;
	int rc;
	uint64_t *buf;
	const char *text;

	(void)params;

	/* Create a test table and insert a row into it. */
	__exec(f, "CREATE TABLE test (t TEXT)");
	__exec(f, "INSERT INTO test VALUES('hello')");

	__prepare(f, "SELECT t FROM test");

	rc = stmt__query(f->stmt, f->message);
	munit_assert_int(rc, ==, SQLITE_DONE);

	/* The first word written is the column count. */
	buf = (uint64_t *)f->message->body1;
	munit_assert_int(byte__flip64(*buf), ==, 1);

	/* Then the column name. */
	text = (const char *)(f->message->body1 + 8);
	munit_assert_string_equal(text, "t");

	/* Then the row, with its header and value. */
	munit_assert_int(f->message->body1[16], ==, SQLITE_TEXT);
	text = (const char *)(f->message->body1 + 24);
	munit_assert_string_equal(text, "hello");

	return MUNIT_OK;
}

/* Encode a query yielding a single row with a Unix time column. */
TEST_CASE(query, unixtime, NULL)
{
	struct fixture *f = data;
	int rc;
	uint64_t *buf;
	const char *text;
	time_t now;

	(void)params;

	/* Create a test table and insert a row into it. */
	__exec(f, "CREATE TABLE test (t DATETIME)");
	__exec(f, "INSERT INTO test VALUES(strftime('%s','now'))");

	__prepare(f, "SELECT t FROM test");

	rc = stmt__query(f->stmt, f->message);
	munit_assert_int(rc, ==, SQLITE_DONE);

	/* The first word written is the column count. */
	buf = (uint64_t *)f->message->body1;
	munit_assert_int(byte__flip64(*buf), ==, 1);

	/* Then the column name. */
	text = (const char *)(f->message->body1 + 8);
	munit_assert_string_equal(text, "t");

	/* Get the current Unix time */
	now = time(NULL);

	/* Then the row, with its header and value. */
	munit_assert_int(f->message->body1[16], ==, DQLITE_UNIXTIME);
	buf = (uint64_t *)(f->message->body1 + 24);
	munit_assert_double_equal((double)(byte__flip64(*buf)), (double)(now),
				  0);

	return MUNIT_OK;
}

/* Encode a query yielding a single row with a ISO8601 time column. */
TEST_CASE(query, iso8601, NULL)
{
	struct fixture *f = data;
	int rc;
	uint64_t *buf;
	const char *text;

	(void)params;

	/* Create a test table and insert a row into it. */
	__exec(f, "CREATE TABLE test (t DATETIME)");
	__exec(f, "INSERT INTO test VALUES(datetime(1532078292, 'unixepoch'))");

	__prepare(f, "SELECT t FROM test");

	rc = stmt__query(f->stmt, f->message);
	munit_assert_int(rc, ==, SQLITE_DONE);

	/* The first word written is the column count. */
	buf = (uint64_t *)f->message->body1;
	munit_assert_int(byte__flip64(*buf), ==, 1);

	/* Then the column name. */
	text = (const char *)(f->message->body1 + 8);
	munit_assert_string_equal(text, "t");

	/* Then the row, with its header and value. */
	munit_assert_int(f->message->body1[16], ==, DQLITE_ISO8601);
	text = (const char *)(f->message->body1 + 24);
	munit_assert_string_equal(text, "2018-07-20 09:18:12");

	return MUNIT_OK;
}

/* Encode a query yielding a single row with a null time column. */
TEST_CASE(query, iso8601_null, NULL)
{
	struct fixture *f = data;
	int rc;
	uint64_t *buf;
	const char *text;

	(void)params;

	/* Create a test table and insert a row into it. */
	__exec(f, "CREATE TABLE test (t DATETIME)");
	__exec(f, "INSERT INTO test VALUES(NULL)");

	__prepare(f, "SELECT t FROM test");

	rc = stmt__query(f->stmt, f->message);
	munit_assert_int(rc, ==, SQLITE_DONE);

	/* The first word written is the column count. */
	buf = (uint64_t *)f->message->body1;
	munit_assert_int(byte__flip64(*buf), ==, 1);

	/* Then the column name. */
	text = (const char *)(f->message->body1 + 8);
	munit_assert_string_equal(text, "t");

	/* Then the row, with its header and value. */
	munit_assert_int(f->message->body1[16], ==, DQLITE_ISO8601);
	text = (const char *)(f->message->body1 + 24);
	munit_assert_string_equal(text, "");

	return MUNIT_OK;
}

/* Encode a query yielding a single row with an empty string time column. */
TEST_CASE(query, iso8601_empty, NULL)
{
	struct fixture *f = data;
	int rc;
	uint64_t *buf;
	const char *text;

	(void)params;

	/* Create a test table and insert a row into it. */
	__exec(f, "CREATE TABLE test (t DATETIME)");
	__exec(f, "INSERT INTO test VALUES('')");

	__prepare(f, "SELECT t FROM test");

	rc = stmt__query(f->stmt, f->message);
	munit_assert_int(rc, ==, SQLITE_DONE);

	/* The first word written is the column count. */
	buf = (uint64_t *)f->message->body1;
	munit_assert_int(byte__flip64(*buf), ==, 1);

	/* Then the column name. */
	text = (const char *)(f->message->body1 + 8);
	munit_assert_string_equal(text, "t");

	/* Then the row, with its header and value. */
	munit_assert_int(f->message->body1[16], ==, DQLITE_ISO8601);
	text = (const char *)(f->message->body1 + 24);
	munit_assert_string_equal(text, "");

	return MUNIT_OK;
}

/* Encode a query yielding a single row with a boolean time column. */
TEST_CASE(query, boolean, NULL)
{
	struct fixture *f = data;
	int rc;
	uint64_t *buf;
	const char *text;

	(void)params;

	/* Create a test table and insert a row into it. */
	__exec(f, "CREATE TABLE test (b BOOLEAN)");
	__exec(f, "INSERT INTO test VALUES(1)");

	__prepare(f, "SELECT b FROM test");

	rc = stmt__query(f->stmt, f->message);
	munit_assert_int(rc, ==, SQLITE_DONE);

	/* The first word written is the column count. */
	buf = (uint64_t *)f->message->body1;
	munit_assert_int(byte__flip64(*buf), ==, 1);

	/* Then the column name. */
	text = (const char *)(f->message->body1 + 8);
	munit_assert_string_equal(text, "b");

	/* Then the row, with its header and value. */
	munit_assert_int(f->message->body1[16], ==, DQLITE_BOOLEAN);
	buf = (uint64_t *)(f->message->body1 + 24);
	munit_assert_int(byte__flip64(*buf), ==, 1);

	return MUNIT_OK;
}

/* Encode a query yielding two rows with one column. */
TEST_CASE(query, two_simple, NULL)
{
	struct fixture *f = data;
	int rc;
	uint64_t *buf;
	const char *text;

	(void)params;

	/* Create a test table and insert a row into it. */
	__exec(f, "CREATE TABLE test (n INT)");
	__exec(f, "INSERT INTO test VALUES(1)");
	__exec(f, "INSERT INTO test VALUES(2)");

	__prepare(f, "SELECT n FROM test");

	rc = stmt__query(f->stmt, f->message);
	munit_assert_int(rc, ==, SQLITE_DONE);

	/* The first word written is the column count. */
	buf = (uint64_t *)f->message->body1;
	munit_assert_int(byte__flip64(*buf), ==, 1);

	/* Then the column name. */
	text = (const char *)(f->message->body1 + 8);
	munit_assert_string_equal(text, "n");

	/* Then the first row, with its header and value. */
	munit_assert_int(f->message->body1[16], ==, SQLITE_INTEGER);
	buf = (uint64_t *)(f->message->body1 + 24);
	munit_assert_int(byte__flip64(*buf), ==, 1);

	return MUNIT_OK;
}

/* Encode a query yielding two rows with three columns. */
TEST_CASE(query, two_complex, NULL)
{
	struct fixture *f = data;
	int rc;
	uint64_t *buf;
	const char *text;

	(void)params;

	/* Create a test table and insert a row into it. */
	__exec(f, "CREATE TABLE test (n INT, t TEXT, f FLOAT)");
	__exec(f, "INSERT INTO test VALUES(1, 'hi', 3.1415)");
	__exec(f, "INSERT INTO test VALUES(2,'hello world', NULL)");

	__prepare(f, "SELECT n, t, f FROM test");

	rc = stmt__query(f->stmt, f->message);
	munit_assert_int(rc, ==, SQLITE_DONE);

	/* The first word written is the column count. */
	buf = (uint64_t *)f->message->body1;
	munit_assert_int(byte__flip64(*buf), ==, 3);

	/* Then the column names. */
	text = (const char *)(f->message->body1 + 8);
	munit_assert_string_equal(text, "n");

	text = (const char *)(f->message->body1 + 16);
	munit_assert_string_equal(text, "t");

	text = (const char *)(f->message->body1 + 24);
	munit_assert_string_equal(text, "f");

	/* Then the first row, with its header and columns. */
	munit_assert_int(f->message->body1[32] & 0x0f, ==, SQLITE_INTEGER);
	buf = (uint64_t *)(f->message->body1 + 40);
	munit_assert_int(byte__flip64(*buf), ==, 1);

	munit_assert_int((f->message->body1[32] & 0xf0) >> 4, ==, SQLITE_TEXT);
	text = (const char *)(f->message->body1 + 48);
	munit_assert_string_equal(text, "hi");

	munit_assert_int(f->message->body1[33], ==, SQLITE_FLOAT);
	buf = (uint64_t *)(f->message->body1 + 56);
	*buf = byte__flip64(*buf);
	munit_assert_double(*(double *)(buf), ==, 3.1415);

	/* Then the second row, with its header and columns. */
	munit_assert_int(f->message->body1[64] & 0x0f, ==, SQLITE_INTEGER);
	buf = (uint64_t *)(f->message->body1 + 72);
	munit_assert_int(byte__flip64(*buf), ==, 2);

	munit_assert_int((f->message->body1[64] & 0xf0) >> 4, ==, SQLITE_TEXT);
	text = (const char *)(f->message->body1 + 80);
	munit_assert_string_equal(text, "hello world");

	munit_assert_int(f->message->body1[65], ==, SQLITE_NULL);
	buf = (uint64_t *)(f->message->body1 + 96);
	munit_assert_int(*buf, ==, 0);

	return MUNIT_OK;
}

/* Encode a result set yielding a column with no underlying name
 * (e.g. COUNT). */
TEST_CASE(query, count, NULL)
{
	struct fixture *f = data;
	int rc;
	uint64_t *buf;
	const char *text;

	(void)params;

	__prepare(f, "SELECT COUNT(name) FROM sqlite_master");

	rc = stmt__query(f->stmt, f->message);
	munit_assert_int(rc, ==, SQLITE_DONE);

	/* The first word written is the column count. */
	buf = (uint64_t *)f->message->body1;
	munit_assert_int(byte__flip64(*buf), ==, 1);

	/* Then the column names. */
	text = (const char *)(f->message->body1 + 8);
	munit_assert_string_equal(text, "COUNT(name)");

	/* Then the row, with its header and columns. */
	munit_assert_int(f->message->body1[24] & 0x0f, ==, SQLITE_INTEGER);
	buf = (uint64_t *)(f->message->body1 + 32);
	munit_assert_int(byte__flip64(*buf), ==, 0);

	return MUNIT_OK;
}

/* Encode a result set exceeding the statically allocaed message body. */
TEST_CASE(query, large, NULL)
{
	struct fixture *f = data;
	int rc;
	uint64_t *buf;
	const char *text;
	int i;

	(void)params;

	/* Create a test table and insert lots of rows into it. */
	__exec(f, "CREATE TABLE test (n INT)");
	for (i = 0; i < 256; i++) {
		__exec(f, "INSERT INTO test VALUES(123456789)");
	}

	/* Fetch everything. */
	__prepare(f, "SELECT n FROM test");

	/* The return code is SQLITE_ROW, to indicate that not all rows were
	 * fetched. */
	rc = stmt__query(f->stmt, f->message);
	munit_assert_int(rc, ==, SQLITE_ROW);

	/* The first word written is the column count. */
	buf = (uint64_t *)f->message->body1;
	munit_assert_int(byte__flip64(*buf), ==, 1);

	/* Then the column names. */
	text = (const char *)(f->message->body1 + 8);
	munit_assert_string_equal(text, "n");

	/* The static body is full. */
	munit_assert_int(f->message->offset1, ==, 4096);

	/* The dynamic body was allocated. */
	munit_assert_ptr_not_null(f->message->body2.base);

	return MUNIT_OK;
}
