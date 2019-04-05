#include "../lib/config.h"
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
	FIXTURE_CONFIG; \
	FIXTURE_REGISTRY;

#define SETUP         \
	SETUP_HEAP;   \
	SETUP_SQLITE; \
	SETUP_LOGGER; \
	SETUP_VFS;    \
	SETUP_CONFIG; \
	SETUP_REGISTRY;

#define TEAR_DOWN           \
	TEAR_DOWN_REGISTRY; \
	TEAR_DOWN_CONFIG;   \
	TEAR_DOWN_VFS;      \
	TEAR_DOWN_LOGGER;   \
	TEAR_DOWN_SQLITE;   \
	TEAR_DOWN_HEAP;

/******************************************************************************
 *
 * db-related APIs.
 *
 ******************************************************************************/

struct db_fixture
{
	FIXTURE;
};

TEST_SUITE(db);
TEST_SETUP(db)
{
	struct db_fixture *f = munit_malloc(sizeof *f);
	SETUP;
	return f;
}
TEST_TEAR_DOWN(db)
{
	struct db_fixture *f = data;
	TEAR_DOWN;
	free(f);
}

/* Get a db that didn't exist before. */
TEST_CASE(db, get_new, NULL)
{
	struct db_fixture *f = data;
	struct db *db;
	(void)params;
	int rc;
	rc = registry__db_get(&f->registry, "test.db", &db);
	munit_assert_int(rc, ==, 0);
	munit_assert_string_equal(db->filename, "test.db");
	return MUNIT_OK;
}

/* Get a previously registered db. */
TEST_CASE(db, get_existing, NULL)
{
	struct db_fixture *f = data;
	struct db *db1;
	struct db *db2;
	(void)params;
	int rc;
	rc = registry__db_get(&f->registry, "test.db", &db1);
	munit_assert_int(rc, ==, 0);
	rc = registry__db_get(&f->registry, "test.db", &db2);
	munit_assert_int(rc, ==, 0);
	munit_assert_ptr_equal(db1, db2);
	return MUNIT_OK;
}
