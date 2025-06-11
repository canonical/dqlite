#include "../lib/client.h"
#include "../lib/heap.h"
#include "../lib/runner.h"
#include "../lib/server.h"
#include "../lib/sqlite.h"

SUITE(stress);

#define READ_COUNT 1000
#define WRITE_COUNT 1000

static char *disk_mode[] = { "0", "1", NULL };
static char *databases[] = { "1", "2", "4", NULL };
static char *writers[] = { "0", "1", "2", "4", NULL };
static char *readers[] = { "0", "1", "4", "16", NULL };

static MunitParameterEnum stress_params[] = {
	{ "disk_mode", disk_mode },
	{ "writers", writers },
	{ "readers", readers },
	{ "databases", databases },
	{ NULL, NULL },
};

struct fixture {
	struct test_server server;
	struct client_proto *client;
	int databases;
	int readers, writers;
};

struct worker {
	pthread_t thread;
	struct fixture *f;
	char database[16];
};

static void *client_read(void *data)
{
	const char *sql =
	    "SELECT MAX(n)         "
	    "FROM (                "
	    "    SELECT n          "
	    "    FROM test         "
	    "    ORDER BY random() "
	    "    LIMIT 100         "
	    ")                     ";

	struct worker *self = data;
	struct client_proto client;
	struct rows rows;
	uint32_t stmt_id;

	test_server_client_connect(&self->f->server, &client);
	HANDSHAKE_C(&client);
	OPEN_C(&client, self->database);
	PREPARE_C(&client, sql, &stmt_id);

	for (int i = 0; i < READ_COUNT; i++) {
		QUERY_DONE_C(&client, stmt_id, &rows, {});
	}

	clientClose(&client);
	return NULL;
}

static void *client_write(void *data)
{
	const char *sql = "INSERT INTO test(n) VALUES (random())";

	struct worker *self = data;
	struct client_proto client;
	uint64_t last_insert_id;
	uint64_t rows_affected;
	uint32_t stmt_id;

	test_server_client_connect(&self->f->server, &client);
	HANDSHAKE_C(&client);
	OPEN_C(&client, self->database);
	PREPARE_C(&client, sql, &stmt_id);

	for (int i = 0; i < WRITE_COUNT; i++) {
		int rv = clientSendExec(&client, stmt_id, NULL, 0, NULL);
		munit_assert_int(rv, ==, DQLITE_OK);

		rv = clientRecvResult(&client, &last_insert_id, &rows_affected,
				      NULL);
		if (rv == DQLITE_CLIENT_PROTO_RECEIVED_FAILURE &&
		    client.errcode == SQLITE_BUSY) {
			/* Just retry */
			i--;
		} else {
			munit_assert_int(rv, ==, DQLITE_OK);
			munit_assert_int(last_insert_id, >, 1);
			munit_assert_int(rows_affected, ==, 1);
		}
	}

	clientClose(&client);
	return NULL;
}

static void *setUp(const MunitParameter params[], void *user_data)
{
	struct fixture *f = munit_malloc(sizeof *f);
	(void)user_data;
	f->databases = atoi(munit_parameters_get(params, "databases"));
	f->readers = atoi(munit_parameters_get(params, "readers"));
	f->writers = atoi(munit_parameters_get(params, "writers"));
	test_heap_setup(params, user_data);
	test_sqlite_setup(params);
	test_server_setup(&f->server, 1, params);
	test_server_prepare(&f->server, params);
	dqlite_node_set_busy_timeout(f->server.dqlite, 200 * f->writers);
	test_server_run(&f->server);
	f->client = test_server_client(&f->server);

	for (int i = 0; i < f->databases; i++) {
		char name[16];
		uint32_t stmt_id;
		uint64_t last_insert_id;
		uint64_t rows_affected;

		snprintf(name, 16, "test%d", i);
		test_server_client_reconnect(&f->server, f->client);
		HANDSHAKE;
		OPEN_C(f->client, name);

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
		    &stmt_id);
		EXEC(stmt_id, &last_insert_id, &rows_affected);
	}

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
	(void)params;

	if (f->readers == 0 && f->writers == 0) {
		return MUNIT_SKIP;
	}

	int num_workers = (f->readers + f->writers) * f->databases;
	struct worker *workers =
	    munit_malloc(num_workers * sizeof(struct worker));
	struct worker *write_workers = workers;
	struct worker *read_workers =
	    write_workers + (f->writers * f->databases);

	for (int i = 0; i < f->readers; i++) {
		for (int j = 0; j < f->databases; j++) {
			struct worker *worker =
			    &read_workers[i * f->databases + j];
			worker->f = f;
			snprintf(worker->database, 16, "test%d", j);
			pthread_create(&worker->thread, NULL, client_read,
				       worker);
		}
	}

	for (int i = 0; i < f->writers; i++) {
		for (int j = 0; j < f->databases; j++) {
			struct worker *worker =
			    &write_workers[i * f->databases + j];
			worker->f = f;
			snprintf(worker->database, 16, "test%d", j);
			pthread_create(&worker->thread, NULL, client_write,
				       worker);
		}
	}

	for (int i = 0; i < num_workers; i++) {
		pthread_join(workers[i].thread, NULL);
	}

	free(workers);
	return MUNIT_OK;
}
