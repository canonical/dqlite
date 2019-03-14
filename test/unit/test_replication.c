#include "../../src/replication.h"

#include "../lib/heap.h"
#include "../lib/logger.h"
#include "../lib/options.h"
#include "../lib/raft.h"
#include "../lib/registry.h"
#include "../lib/replication.h"
#include "../lib/runner.h"
#include "../lib/sqlite.h"
#include "../lib/vfs.h"

TEST_MODULE(replication);

#define FIXTURE           \
	FIXTURE_RAFT;     \
	FIXTURE_LOGGER;   \
	FIXTURE_VFS;      \
	FIXTURE_OPTIONS;  \
	FIXTURE_REGISTRY; \
	FIXTURE_REPLICATION;

#define SETUP           \
	SETUP_RAFT;     \
	SETUP_LOGGER;   \
	SETUP_HEAP;     \
	SETUP_SQLITE;   \
	SETUP_VFS;      \
	SETUP_OPTIONS;  \
	SETUP_REGISTRY; \
	SETUP_REPLICATION;

#define TEAR_DOWN              \
	TEAR_DOWN_REPLICATION; \
	TEAR_DOWN_REGISTRY;    \
	TEAR_DOWN_OPTIONS;     \
	TEAR_DOWN_VFS;         \
	TEAR_DOWN_SQLITE;      \
	TEAR_DOWN_HEAP;        \
	TEAR_DOWN_LOGGER;      \
	TEAR_DOWN_RAFT;

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
