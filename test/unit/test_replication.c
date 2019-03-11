#include "../../src/replication.h"

#include "../lib/db.h"
#include "../lib/heap.h"
#include "../lib/logger.h"
#include "../lib/raft.h"
#include "../lib/runner.h"
#include "../lib/sqlite.h"
#include "../lib/vfs.h"

TEST_MODULE(replication);

#define FIXTURE         \
	RAFT_FIXTURE;   \
	LOGGER_FIXTURE; \
	VFS_FIXTURE;    \
	DB_FIXTURE(db); \
	sqlite3_wal_replication replication;

#define SETUP                                                          \
	int rv;                                                        \
	RAFT_SETUP;                                                    \
	LOGGER_SETUP;                                                  \
	HEAP_SETUP;                                                    \
	SQLITE_SETUP;                                                  \
	VFS_SETUP;                                                     \
	DB_SETUP(db);                                                  \
	rv = replication__init(&f->replication, &f->logger, &f->raft); \
	munit_assert_int(rv, ==, 0);

#define TEAR_DOWN                            \
	replication__close(&f->replication); \
	DB_TEAR_DOWN(db);                    \
	VFS_TEAR_DOWN;                       \
	SQLITE_TEAR_DOWN;                    \
	HEAP_TEAR_DOWN;                      \
	LOGGER_TEAR_DOWN;                    \
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
