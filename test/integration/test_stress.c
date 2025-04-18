#include "../lib/client.h"
#include "../lib/heap.h"
#include "../lib/runner.h"
#include "../lib/server.h"
#include "../lib/sqlite.h"

#include <stdatomic.h>

#define N_CLIENTS 20

/******************************************************************************
 *
 * Handle client requests
 *
 ******************************************************************************/

SUITE(stress);

static char *bools[] = { "0", "1", NULL };

static MunitParameterEnum client_params[] = {
	{ "disk_mode", bools },
	{ NULL, NULL },
};

struct fixture {
	struct test_server server;
	struct client_proto *client;
	volatile int64_t query_count;
	const char* read_query;
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
	f->query_count = 10000;
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

static void* client_read_only(void *data) {
	struct fixture *f = data;
	struct client_proto client;
	struct rows rows;
	uint32_t stmt_id;

	test_server_client_connect(&f->server, &client);
	HANDSHAKE_C(&client);
	OPEN_C(&client);
	PREPARE_C(&client, f->read_query, &stmt_id);

	do {
		int64_t prev = atomic_fetch_sub(&f->query_count, 10);
		if (prev > 0) {
			for (int i = 0; i < 10; i++) {
				QUERY_DONE_C(&client, stmt_id, &rows, {});	
			}
		}
	} while (atomic_load(&f->query_count) > 0);

	test_server_client_close(&f->server, &client);
	return NULL;
}

TEST(stress, read_only, setUp, tearDown, 0, client_params)
{
	struct fixture *f = data;
	uint32_t stmt_id;
	uint64_t last_insert_id;
	uint64_t rows_affected;
	(void)params;
	
	PREPARE("CREATE TABLE test (n INT)", &stmt_id);
	EXEC(stmt_id, &last_insert_id, &rows_affected);

	PREPARE(
		"WITH RECURSIVE seq(n) AS ("
		"    SELECT 1 UNION ALL     "
		"    SELECT n+1 FROM seq    "
		"    WHERE  n < 10000       "
		")                          "
		"INSERT INTO test(n)        "
		"SELECT n FROM seq          ", 
		&stmt_id
	);
	EXEC(stmt_id, &last_insert_id, &rows_affected);

	f->read_query = "SELECT SUM(n) FROM test ORDER BY random() LIMIT 100";

	pthread_t threads[N_CLIENTS];

	for (int i = 0; i < N_CLIENTS; i++) {
		pthread_create(&threads[i], NULL, client_read_only, data);
	}

	for (int i = 0; i < N_CLIENTS; i++) {
		pthread_join(threads[i], NULL);
	}

	return MUNIT_OK;
}
