#include "../lib/heap.h"
#include "../lib/leader.h"
#include "../lib/logger.h"
#include "../lib/options.h"
#include "../lib/raft.h"
#include "../lib/registry.h"
#include "../lib/replication.h"
#include "../lib/runner.h"
#include "../lib/sqlite.h"
#include "../lib/stmt.h"
#include "../lib/vfs.h"

TEST_MODULE(leader);

#define FIXTURE              \
	FIXTURE_LOGGER;      \
	FIXTURE_OPTIONS;     \
	FIXTURE_REGISTRY;    \
	FIXTURE_RAFT;        \
	FIXTURE_VFS;         \
	FIXTURE_REPLICATION; \
	FIXTURE_LEADER;

#define SETUP              \
	SETUP_HEAP;        \
	SETUP_SQLITE;      \
	SETUP_LOGGER;      \
	SETUP_OPTIONS;     \
	SETUP_REGISTRY;    \
	SETUP_RAFT;        \
	SETUP_VFS;         \
	SETUP_REPLICATION; \
	SETUP_LEADER;

#define TEAR_DOWN              \
	TEAR_DOWN_LEADER;      \
	TEAR_DOWN_REPLICATION; \
	TEAR_DOWN_REGISTRY;    \
	TEAR_DOWN_VFS;         \
	TEAR_DOWN_RAFT;        \
	TEAR_DOWN_OPTIONS;     \
	TEAR_DOWN_LOGGER;      \
	TEAR_DOWN_SQLITE;      \
	TEAR_DOWN_HEAP;

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
	rc = sqlite3_prepare_v2(f->leader.conn, "SELECT 1", -1, &stmt, NULL);
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
	FIXTURE_STMT;
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
	f->invoked = false;
	f->status = -1;
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
	int rc;
	RAFT_BECOME_LEADER;
	STMT_PREPARE(f->leader.conn, f->stmt, "CREATE TABLE test (a INT)");
	rc = leader__exec(&f->leader, &f->req, f->stmt, fixture_exec_cb);
	munit_assert_int(rc, ==, 0);
	RAFT_COMMIT;
	RAFT_COMMIT;
	STMT_FINALIZE(f->stmt);
	munit_assert_true(f->invoked);
	return MUNIT_OK;
}

TEST_GROUP(exec, error);

/* The local server is not the leader. */
TEST_CASE(exec, error, begin_not_leader, NULL)
{
	struct exec_fixture *f = data;
	int rc;
	(void)params;
	STMT_PREPARE(f->leader.conn, f->stmt, "CREATE TABLE test (a INT)");
	rc = leader__exec(&f->leader, &f->req, f->stmt, NULL);
	munit_assert_int(rc, ==, 0);
	munit_assert_true(f->req.done);
	munit_assert_int(f->req.status, ==, SQLITE_IOERR_NOT_LEADER);
	STMT_FINALIZE(f->stmt);
	return MUNIT_OK;
}
