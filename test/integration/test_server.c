#include "../lib/heap.h"
#include "../lib/runner.h"
#include "../lib/server.h"
#include "../lib/sqlite.h"

/******************************************************************************
 *
 * Fixture
 *
 ******************************************************************************/

#define FIXTURE struct test_server server
#define SETUP                                     \
	test_heap_setup(params, user_data);       \
	test_sqlite_setup(params);                \
	test_server_setup(&f->server, 1, params); \
	test_server_start(&f->server)
#define TEAR_DOWN                          \
	test_server_tear_down(&f->server); \
	test_sqlite_tear_down();           \
	test_heap_tear_down(data)

/******************************************************************************
 *
 * dqlite_node_start
 *
 ******************************************************************************/

SUITE(dqlite_node_start);

struct fixture
{
	FIXTURE;
};

static void *setUp(const MunitParameter params[], void *user_data)
{
	struct fixture *f = munit_malloc(sizeof *f);
	SETUP;
	return f;
}

static void tearDown(void *data)
{
	struct fixture *f = data;
	TEAR_DOWN;
	free(f);
}

TEST(dqlite_node_start, success, setUp, tearDown, 0, NULL)
{
	struct run_fixture *f = data;
	(void)params;
	(void)f;
	return MUNIT_OK;
}
