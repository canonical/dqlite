#include "../../src/replication.h"

#include "../lib/cluster.h"
#include "../lib/heap.h"
#include "../lib/runner.h"
#include "../lib/sqlite.h"

TEST_MODULE(replication);

#define FIXTURE FIXTURE_CLUSTER;

#define SETUP         \
	SETUP_HEAP;   \
	SETUP_SQLITE; \
	SETUP_CLUSTER;

#define TEAR_DOWN          \
	TEAR_DOWN_CLUSTER; \
	TEAR_DOWN_SQLITE;  \
	TEAR_DOWN_HEAP;

/******************************************************************************
 *
 * sqlite3_wal_replication->xBegin
 *
 ******************************************************************************/

struct begin_fixture
{
	FIXTURE;
};

TEST_SUITE(begin);
TEST_SETUP(begin)
{
	struct begin_fixture *f = munit_malloc(sizeof *f);
	SETUP;
	return f;
}
TEST_TEAR_DOWN(begin)
{
	struct begin_fixture *f = data;
	TEAR_DOWN;
	free(f);
}

TEST_CASE(begin, open, NULL)
{
	struct begin_fixture *f = data;
	int rc;
	struct exec req;
	struct leader *leader0 = CLUSTER_LEADER(0);
	struct leader *leader1 = CLUSTER_LEADER(1);
	(void)params;
	CLUSTER_ELECT(0);
	STMT_PREPARE(leader0->conn, f->stmt, "CREATE TABLE test (a INT)");
	rc = leader__exec(leader0, &req, f->stmt, NULL);
	munit_assert_int(rc, ==, 0);
	CLUSTER_APPLIED(3);
	STMT_FINALIZE(f->stmt);
	STMT_EXEC(leader1->conn, "SELECT * FROM test");
	STMT_PREPARE(leader0->conn, f->stmt, "INSERT INTO test(a) VALUES(1)");
	rc = leader__exec(leader0, &req, f->stmt, NULL);
	munit_assert_int(rc, ==, 0);
	CLUSTER_APPLIED(4);
	STMT_FINALIZE(f->stmt);
	return MUNIT_OK;
}
