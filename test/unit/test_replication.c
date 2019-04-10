#include "../lib/cluster.h"
#include "../lib/runner.h"

#include "../../src/leader.h"

TEST_MODULE(replication);

/******************************************************************************
 *
 * Fixture
 *
 ******************************************************************************/

#define FIXTURE                           \
	FIXTURE_CLUSTER;                  \
	struct leader leaders[N_SERVERS]; \
	sqlite3_stmt *stmt;

#define SETUP                             \
	unsigned i;                       \
	SETUP_CLUSTER                     \
	for (i = 0; i < N_SERVERS; i++) { \
		SETUP_LEADER(i);          \
	}

#define SETUP_LEADER(I)                                   \
	struct leader *leader = &f->leaders[I];           \
	struct registry *registry = CLUSTER_REGISTRY(I);  \
	struct db *db;                                    \
	int rc2;                                          \
	rc2 = registry__db_get(registry, "test.db", &db); \
	munit_assert_int(rc2, ==, 0);                     \
	leader__init(leader, db, CLUSTER_RAFT(I));

#define TEAR_DOWN                         \
	unsigned i;                       \
	for (i = 0; i < N_SERVERS; i++) { \
		TEAR_DOWN_LEADER(i);      \
	}                                 \
	TEAR_DOWN_CLUSTER

#define TEAR_DOWN_LEADER(I)                     \
	struct leader *leader = &f->leaders[I]; \
	leader__close(leader);

/******************************************************************************
 *
 * Helper macros.
 *
 ******************************************************************************/

/* Return the i'th leader object. */
#define LEADER(I) &f->leaders[I]

/* Return the SQLite connection of the i'th leader object */
#define CONN(I) (LEADER(I))->conn

/* Prepare the fixture's statement using the connection of the I'th leader */
#define PREPARE(I, SQL)                                                     \
	{                                                                   \
		int rc2;                                                    \
		rc2 = sqlite3_prepare_v2(CONN(I), SQL, -1, &f->stmt, NULL); \
		munit_assert_int(rc2, ==, 0);                               \
	}

/* Reset the fixture's statement, expecting the given return code. */
#define RESET(RC)                              \
	{                                      \
		int rc2;                       \
		rc2 = sqlite3_reset(f->stmt);  \
		munit_assert_int(rc2, ==, RC); \
	}

/* Finalize the fixture's statement */
#define FINALIZE                                 \
	{                                        \
		int rc2;                         \
		rc2 = sqlite3_finalize(f->stmt); \
		munit_assert_int(rc2, ==, 0);    \
	}

/* Submit an exec request using the I'th leader. */
#define EXEC(I)                                                 \
	{                                                       \
		int rc2;                                        \
		rc2 = leader__exec(LEADER(I), &f->req, f->stmt, \
				   fixture_exec_cb);            \
		munit_assert_int(rc2, ==, 0);                   \
	}

/******************************************************************************
 *
 * leader__init
 *
 ******************************************************************************/

struct init_fixture
{
	FIXTURE;
};

TEST_SUITE(init);
TEST_SETUP(init)
{
	struct init_fixture *f = munit_malloc(sizeof *f);
	SETUP;
	return f;
}
TEST_TEAR_DOWN(init)
{
	struct init_fixture *f = data;
	TEAR_DOWN;
	free(f);
}

/* The connection is open and can be used. */
TEST_CASE(init, conn, NULL)
{
	struct init_fixture *f = data;
	sqlite3_stmt *stmt;
	int rc;
	(void)params;
	rc = sqlite3_prepare_v2(CONN(0), "SELECT 1", -1, &stmt, NULL);
	munit_assert_int(rc, ==, 0);
	sqlite3_finalize(stmt);
	return MUNIT_OK;
}

/******************************************************************************
 *
 * leader__exec
 *
 ******************************************************************************/

struct exec_fixture
{
	FIXTURE;
	struct exec req;
	bool invoked;
	int status;
};

static void fixture_exec_cb(struct exec *req, int status)
{
	struct exec_fixture *f = req->data;
	f->invoked = true;
	f->status = status;
}

TEST_SUITE(exec);
TEST_SETUP(exec)
{
	struct exec_fixture *f = munit_malloc(sizeof *f);
	SETUP;
	f->req.data = f;
	return f;
}
TEST_TEAR_DOWN(exec)
{
	struct exec_fixture *f = data;
	TEAR_DOWN;
	free(f);
}

TEST_CASE(exec, success, NULL)
{
	struct exec_fixture *f = data;
	(void)params;
	CLUSTER_ELECT(0);
	PREPARE(0, "CREATE TABLE test (a  INT)");
	EXEC(0);
	CLUSTER_APPLIED(3);
	munit_assert_true(f->invoked);
	munit_assert_int(f->status, ==, SQLITE_DONE);
	FINALIZE;
	return MUNIT_OK;
}

TEST_GROUP(exec, error);

/* The local server is not the leader. */
TEST_CASE(exec, error, begin_not_leader, NULL)
{
	struct exec_fixture *f = data;
	(void)params;
	CLUSTER_ELECT(1);
	PREPARE(0, "CREATE TABLE test (a  INT)");
	EXEC(0);
	munit_assert_true(f->invoked);
	munit_assert_int(f->status, ==, SQLITE_IOERR_NOT_LEADER);
	RESET(SQLITE_IOERR_NOT_LEADER);
	FINALIZE;
	return MUNIT_OK;
}
