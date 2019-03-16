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
	(void)params;
	rc = leader__exec(f->leader, &req, f->stmt, NULL);
	munit_assert_int(rc, ==, 0);
	CLUSTER_STEP;
	CLUSTER_STEP;
	CLUSTER_STEP;
	CLUSTER_STEP;
	CLUSTER_STEP;
	CLUSTER_STEP;
	CLUSTER_STEP;
	CLUSTER_STEP;
	CLUSTER_STEP;
	char *msg;
	rc = sqlite3_exec(f->servers[(f->leader_index + 1) % 2].leader.conn,
			  "SELECT * FROM test", NULL, NULL, &msg);
	munit_assert_int(rc, ==, SQLITE_OK);
	return MUNIT_OK;
}
