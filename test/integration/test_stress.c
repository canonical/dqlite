#include "../lib/client.h"
#include "../lib/heap.h"
#include "../lib/runner.h"
#include "../lib/server.h"
#include "../lib/sqlite.h"

#include <stdatomic.h>

SUITE(stress);

/* TODO: multiple databases (1, 2, 4) */
/* TODO: multiple servers (1, 3, 5) */
static char *bools[] = { "0", "1", NULL };
static char *writers[] = { "1", "2", "4", NULL };
static char *readers[] = { "1", "4", "16", NULL };

static MunitParameterEnum stress_params[] = {
	{ "disk_mode", bools },
	{ "writers", writers },
	{ "readers", readers },
	{ NULL, NULL },
};

struct fixture {
	struct test_server server;
	struct client_proto *client;
	volatile int64_t read_count;
	volatile int64_t write_count;
	int readers, writers;
};

static void* client_read(void *data) {
	const char *sql = "SELECT MAX(n) FROM test ORDER BY random() LIMIT 100";

	struct fixture *f = data;
	struct client_proto client;
	struct rows rows;
	uint32_t stmt_id;

	test_server_client_connect(&f->server, &client);
	HANDSHAKE_C(&client);
	OPEN_C(&client, "test");
	PREPARE_C(&client, sql, &stmt_id);

	do {
		int64_t prev = atomic_fetch_sub(&f->read_count, 10);
		if (prev > 0) {
			for (int i = 0; i < 10; i++) {
				QUERY_DONE_C(&client, stmt_id, &rows, {});	
			}
		}
	} while (atomic_load(&f->read_count) > 0);

	test_server_client_close(&f->server, &client);
	return NULL;
}

static void* client_write(void *data) {
	const char *sql = "INSERT INTO test(n) VALUES (random())";

	struct fixture *f = data;
	struct client_proto client;
	uint64_t last_insert_id;
	uint64_t rows_affected;
	uint32_t stmt_id;

	test_server_client_connect(&f->server, &client);
	HANDSHAKE_C(&client);
	OPEN_C(&client, "test");
	PREPARE_C(&client, sql, &stmt_id);

	do {
		int64_t prev = atomic_fetch_sub(&f->write_count, 10);
		if (prev > 0) {
			for (int i = 0; i < 10; i++) {
				EXEC_C(&client, stmt_id, &last_insert_id, &rows_affected);
				munit_assert_int(last_insert_id, >, 1);
				munit_assert_int(rows_affected, ==, 1);
			}
		}
	} while (atomic_load(&f->write_count) > 0);

	test_server_client_close(&f->server, &client);
	return NULL;
}

static void *setUp(const MunitParameter params[], void *user_data)
{
	struct fixture *f = munit_malloc(sizeof *f);
	(void)user_data;
	test_heap_setup(params, user_data);
	test_sqlite_setup(params);
	test_server_setup(&f->server, 1, params);
	test_server_prepare(&f->server, params);
	dqlite_node_set_busy_timeout(f->server.dqlite, 200);
	test_server_run(&f->server);
	f->client = test_server_client(&f->server);
	HANDSHAKE;
	OPEN;
	f->readers = atoi(munit_parameters_get(params, "readers"));
	f->writers = atoi(munit_parameters_get(params, "writers"));
	f->read_count = 1000 * f->readers;
	f->write_count = 1000 * f->writers;

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

TEST(stress, read_write, setUp, tearDown, 0, stress_params)
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

	pthread_t *workers = malloc((f->readers + f->writers) * sizeof(pthread_t));
	pthread_t *write_workers = workers;
	pthread_t *read_workers = write_workers + f->writers;

	for (int i = 0; i < f->readers; i++) {
		pthread_create(&read_workers[i], NULL, client_read, data);
	}

	for (int i = 0; i < f->writers; i++) {
		pthread_create(&write_workers[i], NULL, client_write, data);
	}

	for (int i = 0; i < f->readers + f->writers; i++) {
		pthread_join(workers[i], NULL);
	}

	free(workers);
	return MUNIT_OK;
}
