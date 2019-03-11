#include "../../src/replication.h"

#include "../lib/heap.h"
#include "../lib/logger.h"
#include "../lib/raft.h"
#include "../lib/runner.h"
#include "../lib/sqlite.h"

TEST_MODULE(replication);

#define FIXTURE                      \
	RAFT_FIXTURE;                \
	struct dqlite_logger logger; \
	sqlite3_wal_replication replication;

#define SETUP                                                          \
	RAFT_SETUP;                                                    \
	int rv;                                                        \
	test_heap_setup(params, user_data);                            \
	test_sqlite_setup(params);                                     \
	test_logger_setup(params, &f->logger);                         \
	rv = replication__init(&f->replication, &f->logger, &f->raft); \
	munit_assert_int(rv, ==, 0);

#define TEAR_DOWN                            \
	replication__close(&f->replication); \
	test_logger_tear_down(&f->logger);   \
	test_sqlite_tear_down();             \
	test_heap_tear_down(data);           \
	RAFT_TEAR_DOWN;

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

TEST_CASE(begin, foo, NULL)
{
	return MUNIT_OK;
}
