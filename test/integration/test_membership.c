#include "../../src/client.h"
#include "../../src/server.h"
#include "../lib/endpoint.h"
#include "../lib/fs.h"
#include "../lib/heap.h"
#include "../lib/runner.h"
#include "../lib/server.h"
#include "../lib/sqlite.h"

TEST_MODULE(membership);

/******************************************************************************
 *
 * Fixture
 *
 ******************************************************************************/

#define N_SERVERS 3
#define FIXTURE                                \
	struct test_server servers[N_SERVERS]; \
	struct client *client

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
		test_server_start(server);                    \
	}                                                     \
	SELECT(1)

#define TEAR_DOWN                                       \
	unsigned i_;                                    \
	for (i_ = 0; i_ < N_SERVERS; i_++) {            \
		test_server_tear_down(&f->servers[i_]); \
	}                                               \
	test_sqlite_tear_down();                        \
	test_heap_tear_down(data)

/******************************************************************************
 *
 * Common parameters.
 *
 ******************************************************************************/

/* Run the test using only TCP. */
char *socket_family[] = {"tcp", NULL};
static MunitParameterEnum params[] = {
    {TEST_ENDPOINT_FAMILY, socket_family},
    {NULL, NULL},
};

/******************************************************************************
 *
 * Helper macros.
 *
 ******************************************************************************/

/* Use the client connected to the server with the given ID. */
#define SELECT(ID) f->client = test_server_client(&f->servers[ID - 1])

/* Send the initial client handshake. */
#define HANDSHAKE                                     \
	{                                             \
		int rv_;                              \
		rv_ = clientSendHandshake(f->client); \
		munit_assert_int(rv_, ==, 0);         \
	}

/* Send a join request. */
#define JOIN(ID, ADDRESS)                                     \
	{                                                     \
		int rv_;                                      \
		rv_ = clientSendJoin(f->client, ID, ADDRESS); \
		munit_assert_int(rv_, ==, 0);                 \
		rv_ = clientRecvEmpty(f->client);             \
		munit_assert_int(rv_, ==, 0);                 \
	}

/* Send an assign role request. */
#define ASSIGN(ID, ROLE)                                     \
	{                                                    \
		int rv_;                                     \
		rv_ = clientSendAssign(f->client, ID, ROLE); \
		munit_assert_int(rv_, ==, 0);                \
		rv_ = clientRecvEmpty(f->client);            \
		munit_assert_int(rv_, ==, 0);                \
	}

/* Send a remove request. */
#define REMOVE(ID)                                     \
	{                                              \
		int rv_;                               \
		rv_ = clientSendRemove(f->client, ID); \
		munit_assert_int(rv_, ==, 0);          \
		rv_ = clientRecvEmpty(f->client);      \
		munit_assert_int(rv_, ==, 0);          \
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

/******************************************************************************
 *
 * join
 *
 ******************************************************************************/

struct join_fixture
{
	FIXTURE;
};

TEST_SUITE(join);
TEST_SETUP(join)
{
	struct join_fixture *f = munit_malloc(sizeof *f);
	SETUP;
	return f;
}
TEST_TEAR_DOWN(join)
{
	struct join_fixture *f = data;
	TEAR_DOWN;
	free(f);
}

TEST_CASE(join, success, params)
{
	struct join_fixture *f = data;
	unsigned id = 2;
	const char *address = "@2";
	unsigned stmt_id;
	unsigned last_insert_id;
	unsigned rows_affected;
	(void)params;
	HANDSHAKE;
	JOIN(id, address);
	ASSIGN(id, 1 /* voter */);
	OPEN;
	PREPARE("CREATE TABLE test (n INT)", &stmt_id);
	EXEC(stmt_id, &last_insert_id, &rows_affected);
	PREPARE("INSERT INTO test(n) VALUES(1)", &stmt_id);
	EXEC(stmt_id, &last_insert_id, &rows_affected);

	/* The table is visible from the new node */
	SELECT(2);
	HANDSHAKE;
	OPEN;
	PREPARE("SELECT * FROM test", &stmt_id);

	/* TODO: fix the standalone test for remove */
	SELECT(1);
	REMOVE(id);
	return MUNIT_OK;
}
