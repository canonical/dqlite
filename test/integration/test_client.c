#include "../lib/heap.h"
#include "../lib/runner.h"
#include "../lib/server.h"
#include "../lib/sqlite.h"

/******************************************************************************
 *
 * Helper macros.
 *
 ******************************************************************************/

/* Send the initial client handshake. */
#define HANDSHAKE                                     \
	{                                             \
		int rv_;                              \
		rv_ = clientSendHandshake(f->client); \
		munit_assert_int(rv_, ==, 0);         \
	}

/* Open a test database. */
#define OPEN                                             \
	{                                                \
		int rv_;                                 \
		rv_ = clientSendOpen(f->client, "test"); \
		munit_assert_int(rv_, ==, 0);            \
		rv_ = clientRecvDb(f->client);           \
		munit_assert_int(rv_, ==, 0);            \
	}

/* Prepare a statement. */
#define PREPARE(SQL, STMT_ID)                             \
	{                                                 \
		int rv_;                                  \
		rv_ = clientSendPrepare(f->client, SQL);  \
		munit_assert_int(rv_, ==, 0);             \
		rv_ = clientRecvStmt(f->client, STMT_ID); \
		munit_assert_int(rv_, ==, 0);             \
	}

/* Execute a statement. */
#define EXEC(STMT_ID, LAST_INSERT_ID, ROWS_AFFECTED)              \
	{                                                         \
		int rv_;                                          \
		rv_ = clientSendExec(f->client, STMT_ID);         \
		munit_assert_int(rv_, ==, 0);                     \
		rv_ = clientRecvResult(f->client, LAST_INSERT_ID, \
				       ROWS_AFFECTED);            \
		munit_assert_int(rv_, ==, 0);                     \
	}

/* Perform a query. */
#define QUERY(STMT_ID, ROWS)                               \
	{                                                  \
		int rv_;                                   \
		rv_ = clientSendQuery(f->client, STMT_ID); \
		munit_assert_int(rv_, ==, 0);              \
		rv_ = clientRecvRows(f->client, ROWS);     \
		munit_assert_int(rv_, ==, 0);              \
	}

/******************************************************************************
 *
 * Handle client requests
 *
 ******************************************************************************/

SUITE(client);

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
	test_server_start(&f->server);
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

TEST(client, exec, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	unsigned stmt_id;
	unsigned lastInsertId;
	unsigned rows_affected;
	(void)params;
	PREPARE("CREATE TABLE test (n INT)", &stmt_id);
	EXEC(stmt_id, &lastInsertId, &rows_affected);
	return MUNIT_OK;
}

TEST(client, query, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	unsigned stmt_id;
	unsigned lastInsertId;
	unsigned rows_affected;
	unsigned i;
	struct rows rows;
	(void)params;
	PREPARE("CREATE TABLE test (n INT)", &stmt_id);
	EXEC(stmt_id, &lastInsertId, &rows_affected);

	PREPARE("BEGIN", &stmt_id);
	EXEC(stmt_id, &lastInsertId, &rows_affected);

	PREPARE("INSERT INTO test (n) VALUES(123)", &stmt_id);
	for (i = 0; i < 256; i++) {
		EXEC(stmt_id, &lastInsertId, &rows_affected);
	}

	PREPARE("COMMIT", &stmt_id);
	EXEC(stmt_id, &lastInsertId, &rows_affected);

	PREPARE("SELECT n FROM test", &stmt_id);
	QUERY(stmt_id, &rows);

	clientCloseRows(&rows);

	return MUNIT_OK;
}
