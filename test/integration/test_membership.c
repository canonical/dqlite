#include "../../src/client.h"
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
	struct client *client;                 \
	struct rows rows;

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

/******************************************************************************
 *
 * Helper macros.
 *
 ******************************************************************************/

/* Use the client connected to the server with the given ID. */
#define SELECT(ID) f->client = test_server_client(&f->servers[ID - 1])

/* Wait a bounded time until server ID has applied at least the entry at INDEX */
#define AWAIT_REPLICATION(ID, INDEX)							  \
	do {										  \
		struct timespec _start = {0};						  \
		struct timespec _end = {0};						  \
		clock_gettime(CLOCK_MONOTONIC, &_start);				  \
		clock_gettime(CLOCK_MONOTONIC, &_end);					  \
		while((f->servers[ID].dqlite->raft.last_applied < INDEX)		  \
		      && ((_end.tv_sec - _start.tv_sec) < 2)  ) {			  \
			clock_gettime(CLOCK_MONOTONIC, &_end);				  \
		}									  \
		munit_assert_ullong(f->servers[ID].dqlite->raft.last_applied, >=, INDEX); \
	} while(0)

/******************************************************************************
 *
 * join
 *
 ******************************************************************************/

SUITE(membership)

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

TEST(membership, join, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	unsigned id = 2;
	const char *address = "@2";
	unsigned stmt_id;
	unsigned last_insert_id;
	unsigned rows_affected;

	HANDSHAKE;
	ADD(id, address);
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

TEST(membership, transfer, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	unsigned id = 2;
	const char *address = "@2";
	unsigned stmt_id;
	unsigned last_insert_id;
	unsigned rows_affected;
	raft_index last_applied;
	struct client c_transfer; /* Client used for transfer requests */

	HANDSHAKE;
	ADD(id, address);
	ASSIGN(id, 1 /* voter */);
	OPEN;
	PREPARE("CREATE TABLE test (n INT)", &stmt_id);
	EXEC(stmt_id, &last_insert_id, &rows_affected);
	PREPARE("INSERT INTO test(n) VALUES(1)", &stmt_id);
	EXEC(stmt_id, &last_insert_id, &rows_affected);

	/* Transfer leadership and wait until first leader has applied a new
	 * entry replicated from the new leader.  */
	test_server_client_connect(&f->servers[0], &c_transfer);
	HANDSHAKE_C(&c_transfer);
	TRANSFER(2, &c_transfer);
	test_server_client_close(&f->servers[0], &c_transfer);
	last_applied = f->servers[0].dqlite->raft.last_applied;

	SELECT(2);
	HANDSHAKE;
	OPEN;
	PREPARE("INSERT INTO test(n) VALUES(1)", &stmt_id);
	EXEC(stmt_id, &last_insert_id, &rows_affected);

	AWAIT_REPLICATION(0, last_applied + 1);

	return MUNIT_OK;
}

/* Transfer leadership away from a member that has a pending transaction */
TEST(membership, transferPendingTransaction, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	unsigned id = 2;
	const char *address = "@2";
	unsigned stmt_id;
	unsigned last_insert_id;
	unsigned rows_affected;
	raft_index last_applied;
	struct client c_transfer; /* Client used for transfer requests */

	HANDSHAKE;
	ADD(id, address);
	ASSIGN(id, 1 /* voter */);
	OPEN;
	PREPARE("CREATE TABLE test (n INT)", &stmt_id);
	EXEC(stmt_id, &last_insert_id, &rows_affected);
	PREPARE("INSERT INTO test(n) VALUES(1)", &stmt_id);
	EXEC(stmt_id, &last_insert_id, &rows_affected);

	/* Pending transaction */
	PREPARE("BEGIN", &stmt_id);
	EXEC(stmt_id, &last_insert_id, &rows_affected);
	PREPARE("SELECT * FROM test", &stmt_id);
	QUERY(stmt_id, &f->rows);
	clientCloseRows(&f->rows);

	/* Transfer leadership and wait until first leader has applied a new
	 * entry replicated from the new leader.  */
	test_server_client_connect(&f->servers[0], &c_transfer);
	HANDSHAKE_C(&c_transfer);
	TRANSFER(2, &c_transfer);
	test_server_client_close(&f->servers[0], &c_transfer);
	last_applied = f->servers[0].dqlite->raft.last_applied;

	SELECT(2);
	HANDSHAKE;
	OPEN;
	PREPARE("INSERT INTO test(n) VALUES(2)", &stmt_id);
	EXEC(stmt_id, &last_insert_id, &rows_affected);

	AWAIT_REPLICATION(0, last_applied + 1);

	return MUNIT_OK;
}

/* Transfer leadership back and forth from a member that has a pending transaction */
TEST(membership, transferTwicePendingTransaction, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	unsigned id = 2;
	const char *address = "@2";
	unsigned stmt_id;
	unsigned last_insert_id;
	unsigned rows_affected;
	raft_index last_applied;
	struct client c_transfer; /* Client used for transfer requests */

	HANDSHAKE;
	ADD(id, address);
	ASSIGN(id, 1 /* voter */);
	OPEN;
	PREPARE("CREATE TABLE test (n INT)", &stmt_id);
	EXEC(stmt_id, &last_insert_id, &rows_affected);
	PREPARE("INSERT INTO test(n) VALUES(1)", &stmt_id);
	EXEC(stmt_id, &last_insert_id, &rows_affected);

	/* Pending transaction */
	PREPARE("BEGIN", &stmt_id);
	EXEC(stmt_id, &last_insert_id, &rows_affected);
	PREPARE("SELECT * FROM test", &stmt_id);
	QUERY(stmt_id, &f->rows);
	clientCloseRows(&f->rows);

	/* Transfer leadership and wait until first leader has applied a new
	 * entry replicated from the new leader.  */
	test_server_client_connect(&f->servers[0], &c_transfer);
	HANDSHAKE_C(&c_transfer);
	TRANSFER(2, &c_transfer);
	test_server_client_close(&f->servers[0], &c_transfer);
	last_applied = f->servers[0].dqlite->raft.last_applied;

	SELECT(2);
	HANDSHAKE;
	OPEN;
	PREPARE("INSERT INTO test(n) VALUES(2)", &stmt_id);
	EXEC(stmt_id, &last_insert_id, &rows_affected);

	AWAIT_REPLICATION(0, last_applied + 1);

	/* Transfer leadership back to original node, reconnect the client and
	 * ensure queries can be executed. */
	test_server_client_connect(&f->servers[1], &c_transfer);
	HANDSHAKE_C(&c_transfer);
	TRANSFER(1, &c_transfer);
	test_server_client_close(&f->servers[1], &c_transfer);

	last_applied = f->servers[1].dqlite->raft.last_applied;
	test_server_client_reconnect(&f->servers[0], &f->servers[0].client);
	SELECT(1);
	HANDSHAKE;
	OPEN;
	PREPARE("INSERT INTO test(n) VALUES(3)", &stmt_id);
	EXEC(stmt_id, &last_insert_id, &rows_affected);

	AWAIT_REPLICATION(1, last_applied + 1);

	return MUNIT_OK;
}
