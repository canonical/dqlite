#include "../lib/client.h"
#include "../lib/heap.h"
#include "../lib/runner.h"
#include "../lib/server.h"
#include "../lib/sqlite.h"

/******************************************************************************
 *
 * Handle client requests
 *
 ******************************************************************************/

SUITE(client);

static char* bools[] = {
    "0", "1", NULL
};

static MunitParameterEnum client_params[] = {
    { "disk_mode", bools },
    { NULL, NULL },
};

struct fixture
{
	struct test_server server;
	struct client *client;
};

static void *setUp(const MunitParameter params[], void *user_data)
{
	struct fixture *f = munit_malloc(sizeof *f);
	(void)user_data;
	test_heap_setup(params, user_data);
	test_sqlite_setup(params);
	test_server_setup(&f->server, 1, params);
	test_server_start(&f->server, params);
	f->client = test_server_client(&f->server);
	HANDSHAKE;
	OPEN;
	return f;
}

static void tearDown(void *data)
{
	struct fixture *f = data;
	test_server_tear_down(&f->server);
	test_sqlite_tear_down();
	test_heap_tear_down(data);

	free(f);
}

TEST(client, exec, setUp, tearDown, 0, client_params)
{
	struct fixture *f = data;
	unsigned stmt_id;
	unsigned last_insert_id;
	unsigned rows_affected;
	(void)params;
	PREPARE("CREATE TABLE test (n INT)", &stmt_id);
	EXEC(stmt_id, &last_insert_id, &rows_affected);
	return MUNIT_OK;
}

TEST(client, query, setUp, tearDown, 0, client_params)
{
	struct fixture *f = data;
	unsigned stmt_id;
	unsigned last_insert_id;
	unsigned rows_affected;
	unsigned i;
	struct rows rows;
	(void)params;
	PREPARE("CREATE TABLE test (n INT)", &stmt_id);
	EXEC(stmt_id, &last_insert_id, &rows_affected);

	PREPARE("BEGIN", &stmt_id);
	EXEC(stmt_id, &last_insert_id, &rows_affected);

	PREPARE("INSERT INTO test (n) VALUES(123)", &stmt_id);
	for (i = 0; i < 256; i++) {
		EXEC(stmt_id, &last_insert_id, &rows_affected);
	}

	PREPARE("COMMIT", &stmt_id);
	EXEC(stmt_id, &last_insert_id, &rows_affected);

	PREPARE("SELECT n FROM test", &stmt_id);
	QUERY(stmt_id, &rows);

	clientCloseRows(&rows);

	return MUNIT_OK;
}
