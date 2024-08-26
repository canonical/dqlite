#include "../../src/client/protocol.h"
#include "../../src/server.h"
#include "../lib/client.h"
#include "../lib/endpoint.h"
#include "../lib/fs.h"
#include "../lib/heap.h"
#include "../lib/runner.h"
#include "../lib/server.h"
#include "../lib/sqlite.h"

/******************************************************************************
 *
 * Fixture
 *
 ******************************************************************************/

#define N_SERVERS 3
#define FIXTURE                                \
	struct test_server servers[N_SERVERS]; \
	struct client_proto *client

#define SETUP                                                 \
	unsigned i_;                                          \
	test_heap_setup(params, user_data);                   \
	test_sqlite_setup(params);                            \
	for (i_ = 0; i_ < N_SERVERS; i_++) {                  \
		struct test_server *server = &f->servers[i_]; \
		test_server_setup(server, i_ + 1, params);    \
	}                                                     \
	test_server_network(f->servers, N_SERVERS);           \
	for (i_ = 0; i_ < N_SERVERS; i_++) {                  \
		struct test_server *server = &f->servers[i_]; \
		test_server_start(server, params);            \
	}                                                     \
	SELECT(1)

#define TEAR_DOWN                                       \
	unsigned i_;                                    \
	for (i_ = 0; i_ < N_SERVERS; i_++) {            \
		test_server_tear_down(&f->servers[i_]); \
	}                                               \
	test_sqlite_tear_down();                        \
	test_heap_tear_down(data)

/* Use the client connected to the server with the given ID. */
#define SELECT(ID) f->client = test_server_client(&f->servers[ID - 1])

/******************************************************************************
 *
 * cluster
 *
 ******************************************************************************/

SUITE(cluster)

struct fixture {
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

static char *bools[] = { "0", "1", NULL };

static char *num_records[] = {
	"0", "1", "256",
	/* WAL will just have been checkpointed after 993 writes. */
	"993",
	/* Non-empty WAL, checkpointed twice, 2 snapshots taken */
	"2200", NULL
};

static MunitParameterEnum cluster_params[] = {
	{ "num_records", num_records },
	{ "disk_mode", bools },
	{ NULL, NULL },
};

/* Restart a node and check if all data is there */
TEST(cluster, restart, setUp, tearDown, 0, cluster_params)
{
	struct fixture *f = data;
	uint32_t stmt_id;
	uint64_t last_insert_id;
	uint64_t rows_affected;
	struct rows rows;
	long n_records =
	    strtol(munit_parameters_get(params, "num_records"), NULL, 0);

	HANDSHAKE;
	OPEN;
	PREPARE("CREATE TABLE test (n INT)", &stmt_id);
	EXEC(stmt_id, &last_insert_id, &rows_affected);

	PREPARE("INSERT INTO TEST(n) VALUES(?)", &stmt_id);
	for (int i = 0; i < n_records; ++i) {
		EXEC_PARAMS(stmt_id, &last_insert_id, &rows_affected,
			    {.type = SQLITE_INTEGER, .integer = i});
	}

	struct test_server *server = &f->servers[0];
	test_server_stop(server);
	test_server_start(server, params);

	/* The table is visible after restart. */
	HANDSHAKE;
	OPEN;
	PREPARE("SELECT COUNT(*) from test", &stmt_id);

	QUERY_DONE(stmt_id, &rows, {});
	return MUNIT_OK;
}

/* Add data to a node, add a new node and make sure data is there. */
TEST(cluster, dataOnNewNode, setUp, tearDown, 0, cluster_params)
{
	struct fixture *f = data;
	uint32_t stmt_id;
	uint64_t last_insert_id;
	uint64_t rows_affected;
	struct rows rows;
	long n_records =
	    strtol(munit_parameters_get(params, "num_records"), NULL, 0);
	unsigned id = 2;
	const char *address = "@2";
	int rv;

	HANDSHAKE;
	OPEN;
	PREPARE("CREATE TABLE test (n INT)", &stmt_id);
	EXEC(stmt_id, &last_insert_id, &rows_affected);

	PREPARE("INSERT INTO test(n) VALUES(?)", &stmt_id);
	for (int i = 0; i < n_records; ++i) {
		EXEC_PARAMS(stmt_id, &last_insert_id, &rows_affected,
			    {.type = SQLITE_INTEGER, .integer = i});
	}

	/* Add a second voting server, this one will receive all data from the
	 * original leader. */
	ADD(id, address);
	ASSIGN(id, DQLITE_VOTER);

	/* Remove original server so second server becomes leader after election
	 * timeout */
	REMOVE(1);
	sleep(1);

	struct test_server *first = &f->servers[0];
	test_server_stop(first);
	test_server_prepare(first, params);
	/* One entry per INSERT, plus one for the initial configuration, plus
	 * one for the CREATE TABLE, plus one legacy checkpoint command entry
	 * after 993 records or two after 2200 records. */
	uint64_t expected_entries = n_records + (n_records >= 2200 ? 4 :
						 n_records >= 993 ? 3 :
						 2);
	/* We also expect a variable number of barrier entries. Just specify an
	 * upper bound since we don't know the exact count. */
	uint64_t max_barriers = 10;
	uint64_t last_entry_index;
	uint64_t last_entry_term;
	rv = dqlite_node_describe_last_entry(first->dqlite,
					     &last_entry_index,
					     &last_entry_term);
	munit_assert_int(rv, ==, 0);
	munit_assert_uint64(expected_entries, <=, last_entry_index);
	munit_assert_uint64(last_entry_index, <, expected_entries + max_barriers);
	munit_assert_uint64(last_entry_term, ==, 1);
	test_server_run(first);

	/* The full table is visible from the new node */
	SELECT(2);
	HANDSHAKE;
	OPEN;
	PREPARE("SELECT COUNT(*) from test", &stmt_id);
	QUERY(stmt_id, &rows);
	munit_assert_long(rows.next->values->integer, ==, n_records);
	clientCloseRows(&rows);

	/* One more entry on the new node. */
	PREPARE("INSERT INTO test(n) VALUES(?)", &stmt_id);
	EXEC_PARAMS(stmt_id, &last_insert_id, &rows_affected,
		    {.type = SQLITE_INTEGER, .integer = 5000});

	struct test_server *second = &f->servers[1];
	test_server_stop(second);
	test_server_prepare(second, params);
	rv = dqlite_node_describe_last_entry(second->dqlite,
					     &last_entry_index,
					     &last_entry_term);
	munit_assert_int(rv, ==, 0);
	munit_assert_uint64(expected_entries + 1, <=, last_entry_index);
	munit_assert_uint64(last_entry_index, <, expected_entries + max_barriers + 1);
	munit_assert_uint64(last_entry_term, ==, 1);
	test_server_run(second);
	return MUNIT_OK;
}

/* Insert a huge row, causing SQLite to allocate overflow pages. Then
 * insert the same row again. (Reproducer for
 * https://github.com/canonical/raft/issues/432.) */
TEST(cluster, hugeRow, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	uint32_t stmt_id;
	uint64_t last_insert_id;
	uint64_t rows_affected;
	char *sql;
	ssize_t n;
	size_t huge = 20000000;
	(void)params;

	HANDSHAKE;
	OPEN;
	PREPARE(
	    "CREATE TABLE IF NOT EXISTS model(key TEXT, value TEXT, "
	    "UNIQUE(key))",
	    &stmt_id);
	EXEC(stmt_id, &last_insert_id, &rows_affected);
	sql = munit_malloc(huge);
	n = snprintf(
	    sql, huge,
	    "INSERT OR REPLACE INTO model (key, value) VALUES('my-key-1', '");
	memset(sql + n, 'A', huge - n);
	memcpy(sql + huge - 3, "')", 3);
	PREPARE(sql, &stmt_id);
	free(sql);
	EXEC(stmt_id, &last_insert_id, &rows_affected);
	/* Again */
	EXEC(stmt_id, &last_insert_id, &rows_affected);
	return MUNIT_OK;
}

TEST(cluster, modifyingQuery, setUp, tearDown, 0, cluster_params)
{
	struct fixture *f = data;
	uint32_t stmt_id;
	uint64_t last_insert_id;
	uint64_t rows_affected;
	struct rows rows;
	long n_records =
	    strtol(munit_parameters_get(params, "num_records"), NULL, 0);
	char sql[128];
	unsigned id = 2;
	const char *address = "@2";

	HANDSHAKE;
	OPEN;
	PREPARE("CREATE TABLE test (n INT)", &stmt_id);
	EXEC(stmt_id, &last_insert_id, &rows_affected);

	for (int i = 0; i < n_records; ++i) {
		sprintf(sql, "INSERT INTO test(n) VALUES(%d)", i + 1);
		PREPARE(sql, &stmt_id);
		QUERY(stmt_id, &rows);
		munit_assert_uint64(rows.column_count, ==, 0);
		munit_assert_ptr(rows.next, ==, NULL);
		clientCloseRows(&rows);
	}

	ADD(id, address);
	ASSIGN(id, DQLITE_VOTER);

	REMOVE(1);
	sleep(1);

	SELECT(2);
	HANDSHAKE;
	OPEN;
	PREPARE("SELECT COUNT(*) from test", &stmt_id);
	QUERY(stmt_id, &rows);
	munit_assert_long(rows.next->values->integer, ==, n_records);
	clientCloseRows(&rows);
	return MUNIT_OK;
}

TEST(cluster, modifyingQuerySql, setUp, tearDown, 0, cluster_params)
{
	struct fixture *f = data;
	uint32_t stmt_id;
	uint64_t last_insert_id;
	uint64_t rows_affected;
	struct rows rows;
	long n_records =
	    strtol(munit_parameters_get(params, "num_records"), NULL, 0);
	char sql[128];
	unsigned id = 2;
	const char *address = "@2";

	HANDSHAKE;
	OPEN;
	PREPARE("CREATE TABLE test (n INT)", &stmt_id);
	EXEC(stmt_id, &last_insert_id, &rows_affected);

	for (int i = 0; i < n_records; ++i) {
		sprintf(sql, "INSERT INTO test(n) VALUES(%d)", i + 1);
		QUERY_SQL(sql, &rows);
		munit_assert_uint64(rows.column_count, ==, 0);
		munit_assert_ptr(rows.next, ==, NULL);
		clientCloseRows(&rows);
	}

	ADD(id, address);
	ASSIGN(id, DQLITE_VOTER);

	REMOVE(1);
	sleep(1);

	SELECT(2);
	HANDSHAKE;
	OPEN;
	PREPARE("SELECT COUNT(*) from test", &stmt_id);
	QUERY(stmt_id, &rows);
	munit_assert_long(rows.next->values->integer, ==, n_records);
	clientCloseRows(&rows);
	return MUNIT_OK;
}

/* Edge cases for dqlite_node_describe_last_entry. */
TEST(cluster, last_entry_edge_cases, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	uint64_t index;
	uint64_t term;
	int rv;

	sleep(1);

	struct test_server *first = &f->servers[0];
	test_server_stop(first);
	test_server_prepare(first, params);
	rv = dqlite_node_describe_last_entry(first->dqlite, &index, &term);
	munit_assert_int(rv, ==, 0);
	/* The log contains only the bootstrap configuration. */
	munit_assert_uint64(index, ==, 1);
	/* The bootstrap configuration is always tagged with term 1. */
	munit_assert_uint64(term, ==, 1);
	test_server_run(first);

	struct test_server *second = &f->servers[1];
	test_server_stop(second);
	test_server_prepare(second, params);
	rv = dqlite_node_describe_last_entry(second->dqlite, &index, &term);
	munit_assert_int(rv, ==, 0);
	/* We didn't bootstrap and haven't joined the leader, so our log is
	 * empty. */
	munit_assert_uint64(index, ==, 0);
	munit_assert_uint64(term, ==, 0);
	test_server_run(second);

	return MUNIT_OK;
}
