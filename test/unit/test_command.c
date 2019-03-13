#include <sqlite3.h>

#include "../../src/command.h"

#include "../lib/runner.h"

TEST_MODULE(command);

/******************************************************************************
 *
 * Open.
 *
 ******************************************************************************/

TEST_SUITE(open);

TEST_CASE(open, encode, NULL)
{
	struct command__open c;
	struct raft_buffer buf;
	int rc;
	(void)data;
	(void)params;
	c.filename = "test.db";
	rc = command__encode_open(&c, &buf);
	munit_assert_int(rc, ==, 0);
	munit_assert_int(buf.len, ==, 16);
	sqlite3_free(buf.base);
	return MUNIT_OK;
}

TEST_CASE(open, decode, NULL)
{
	struct command__open c1;
	void *c2;
	int type;
	struct raft_buffer buf;
	int rc;
	(void)data;
	(void)params;
	c1.filename = "db";
	rc = command__encode_open(&c1, &buf);
	munit_assert_int(rc, ==, 0);
	rc = command__decode(&buf, &type, &c2);
	munit_assert_int(rc, ==, 0);
	munit_assert_int(type, ==, COMMAND__OPEN);
	munit_assert_string_equal(((struct command__open *)c2)->filename, "db");
	sqlite3_free(c2);
	sqlite3_free(buf.base);
	return MUNIT_OK;
}
