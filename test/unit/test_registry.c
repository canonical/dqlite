#include "../lib/db.h"
#include "../lib/heap.h"
#include "../lib/logger.h"
#include "../lib/registry.h"
#include "../lib/runner.h"
#include "../lib/sqlite.h"
#include "../lib/vfs.h"

TEST_MODULE(registry);

#define FIXTURE         \
	FIXTURE_LOGGER; \
	FIXTURE_VFS;    \
	FIXTURE_DB(db); \
	FIXTURE_REGISTRY;

#define SETUP           \
	SETUP_LOGGER;   \
	SETUP_HEAP;     \
	SETUP_SQLITE;   \
	SETUP_VFS;      \
	SETUP_REGISTRY; \
	SETUP_DB(db);

#define TEAR_DOWN           \
	TEAR_DOWN_REGISTRY; \
	TEAR_DOWN_DB(db);   \
	TEAR_DOWN_VFS;      \
	TEAR_DOWN_SQLITE;   \
	TEAR_DOWN_HEAP;     \
	TEAR_DOWN_LOGGER;

/******************************************************************************
 *
 * Follower-related APIs.
 *
 ******************************************************************************/

struct follower_fixture
{
	FIXTURE;
};

TEST_SUITE(follower);
TEST_SETUP(follower)
{
	struct follower_fixture *f = munit_malloc(sizeof *f);
	SETUP;
	return f;
}
TEST_TEAR_DOWN(follower)
{
	struct follower_fixture *f = data;
	TEAR_DOWN;
	free(f);
}

/* Add a new follower connection to the registry. */
TEST_CASE(follower, add, NULL)
{
	struct follower_fixture *f = data;
	(void)params;
	int rc;
	rc = registry__conn_follower_add(&f->registry, f->db);
	munit_assert_int(rc, ==, 0);
	return MUNIT_OK;
}

/* Get a previously registered follower connection. */
TEST_CASE(follower, get, NULL)
{
	struct follower_fixture *f = data;
	sqlite3 *db;
	(void)params;
	int rc;
	db = registry__conn_follower_get(&f->registry, "test.db");
	munit_assert_ptr_null(db);
	rc = registry__conn_follower_add(&f->registry, f->db);
	munit_assert_int(rc, ==, 0);
	db = registry__conn_follower_get(&f->registry, "test.db");
	munit_assert_ptr_equal(db, f->db);
	return MUNIT_OK;
}
