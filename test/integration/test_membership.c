#include "../../src/client.h"
#include "../../src/server.h"
#include "../lib/client.h"
#include "../lib/endpoint.h"
#include "../lib/fs.h"
#include "../lib/heap.h"
#include "../lib/runner.h"
#include "../lib/server.h"
#include "../lib/sqlite.h"
#include "../lib/util.h"

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

/******************************************************************************
 *
 * join
 *
 ******************************************************************************/

static char* bools[] = {
    "0", "1", NULL
};

static MunitParameterEnum membership_params[] = {
    { "disk_mode", bools },
    { NULL, NULL },
};

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

TEST(membership, join, setUp, tearDown, 0, membership_params)
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
	TRANSFER(id, f->client);
	SELECT(2);
	HANDSHAKE;
	OPEN;
	PREPARE("SELECT * FROM test", &stmt_id);

	/* TODO: fix the standalone test for remove */
	REMOVE(1);
	return MUNIT_OK;
}

struct id_last_applied {
	struct fixture *f;
	int id;
	raft_index last_applied;

};

static bool last_applied_cond(struct id_last_applied arg)
{
	return arg.f->servers[arg.id].dqlite->raft.last_applied >= arg.last_applied;
}

TEST(membership, transfer, setUp, tearDown, 0, membership_params)
{
	struct fixture *f = data;
	unsigned id = 2;
	const char *address = "@2";
	unsigned stmt_id;
	unsigned last_insert_id;
	unsigned rows_affected;
	raft_index last_applied;
	struct client c_transfer; /* Client used for transfer requests */
	struct id_last_applied await_arg;

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

	await_arg.f = f;
	await_arg.id = 0;
	await_arg.last_applied = last_applied + 1;
	AWAIT_TRUE(last_applied_cond, await_arg, 2);

	return MUNIT_OK;
}

/* Transfer leadership away from a member that has a pending transaction */
TEST(membership, transferPendingTransaction, setUp, tearDown, 0, membership_params)
{
	struct fixture *f = data;
	unsigned id = 2;
	const char *address = "@2";
	unsigned stmt_id;
	unsigned last_insert_id;
	unsigned rows_affected;
	raft_index last_applied;
	struct client c_transfer; /* Client used for transfer requests */
	struct id_last_applied await_arg;

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

	await_arg.f = f;
	await_arg.id = 0;
	await_arg.last_applied = last_applied + 1;
	AWAIT_TRUE(last_applied_cond, await_arg, 2);

	return MUNIT_OK;
}

struct fixture_id {
	struct fixture *f;
	int id;
};

static bool transfer_started_cond(struct fixture_id arg)
{
	return arg.f->servers[arg.id].dqlite->raft.transfer != NULL;
}

/* Transfer leadership away from a member and immediately try to EXEC a
 * prepared SQL statement that needs a barrier */
TEST(membership, transferAndSqlExecWithBarrier, setUp, tearDown, 0, NULL)
{
	int rv;
	struct fixture *f = data;
	unsigned id = 2;
	const char *address = "@2";
	unsigned stmt_id;
	unsigned last_insert_id;
	unsigned rows_affected;
	struct client c_transfer; /* Client used for transfer requests */
	struct fixture_id arg;

	HANDSHAKE;
	ADD(id, address);
	ASSIGN(id, 1 /* voter */);
	OPEN;
	PREPARE("CREATE TABLE test (n INT)", &stmt_id);

	/* Iniate transfer of leadership. This will cause a raft_barrier
	 * failure while the node is technically still the leader, so the gateway
	 * functionality that checks for leadership still succeeds. */
	test_server_client_connect(&f->servers[0], &c_transfer);
	HANDSHAKE_C(&c_transfer);
	rv = clientSendTransfer(&c_transfer, 2);
	munit_assert_int(rv, ==, 0);

	/* Wait until transfer is started by raft so the barrier can fail. */
	arg.f = f;
	arg.id = 0;
	AWAIT_TRUE(transfer_started_cond, arg, 2);

	/* Force a barrier.
	 * TODO this is hacky, but I can't seem to hit the codepath otherwise */
	f->servers[0].dqlite->raft.last_applied = 0;

	rv = clientSendExec(f->client, stmt_id);
	munit_assert_int(rv, ==, 0);
	rv = clientRecvResult(f->client, &last_insert_id, &rows_affected);
	munit_assert_int(rv, ==, 1);

	test_server_client_close(&f->servers[1], &c_transfer);
	return MUNIT_OK;
}

/* Transfer leadership back and forth from a member that has a pending transaction */
TEST(membership, transferTwicePendingTransaction, setUp, tearDown, 0, membership_params)
{
	struct fixture *f = data;
	unsigned id = 2;
	const char *address = "@2";
	unsigned stmt_id;
	unsigned last_insert_id;
	unsigned rows_affected;
	raft_index last_applied;
	struct client c_transfer; /* Client used for transfer requests */
	struct id_last_applied await_arg;

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

	await_arg.f = f;
	await_arg.id = 0;
	await_arg.last_applied = last_applied + 1;
	AWAIT_TRUE(last_applied_cond, await_arg, 2);

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

	await_arg.id = 1;
	AWAIT_TRUE(last_applied_cond, await_arg, 2);

	return MUNIT_OK;
}
