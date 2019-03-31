#include "../lib/cluster.h"
#include "../lib/runner.h"

#include "../../src/leader.h"

TEST_MODULE(leader);

#define FIXTURE          \
	FIXTURE_CLUSTER; \
	struct leader leaders[N_SERVERS];

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
	leader__init(leader, db);

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

#define LEADER(I) &f->leaders[I]
#define CONN(I) (LEADER(I))->conn

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
