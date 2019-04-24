#include "../lib/fs.h"
#include "../lib/heap.h"
#include "../lib/runner.h"
#include "../lib/socket.h"
#include "../lib/sqlite.h"
#include "../lib/thread.h"

#include "../../src/client.h"
#include "../../src/server.h"

TEST_MODULE(membership);

/******************************************************************************
 *
 * Fixture
 *
 ******************************************************************************/

struct server
{
	FIXTURE_THREAD;
	struct test_socket_pair sockets;
	char *dir;
	struct dqlite dqlite;
};

#define N_SERVERS 3

#define FIXTURE struct server servers[N_SERVERS]

#define SETUP                                \
	unsigned i_;                         \
	SETUP_HEAP;                          \
	SETUP_SQLITE;                        \
	for (i_ = 0; i_ < N_SERVERS; i_++) { \
		SETUP_SERVER(i_);            \
	}

#define TEAR_DOWN                            \
	unsigned i_;                         \
	for (i_ = 0; i_ < N_SERVERS; i_++) { \
		TEAR_DOWN_SERVER(i_);        \
	}                                    \
	TEAR_DOWN_SQLITE;                    \
	TEAR_DOWN_HEAP

#define SETUP_SERVER(I)                                           \
	struct server *s = &f->servers[I];                        \
	unsigned id = I + 1;                                      \
	char address[64];                                         \
	int rv_;                                                  \
	test_socket_pair_setup(params, &s->sockets);              \
	sprintf(address, "127.0.0.1:%d", s->sockets.listen_port); \
	s->dir = test_dir_setup();                                \
	rv_ = dqlite__init(&s->dqlite, id, address, s->dir);      \
	munit_assert_int(rv_, ==, 0)

#define TEAR_DOWN_SERVER(I)                \
	struct server *s = &f->servers[I]; \
	dqlite__close(&s->dqlite);         \
	test_dir_tear_down(s->dir);        \
	test_socket_pair_tear_down(&s->sockets)

/******************************************************************************
 *
 * Common parameters.
 *
 ******************************************************************************/

/* Run the test using only TCP. */
char *socket_family[] = {"tcp", NULL};
static MunitParameterEnum params[] = {
    {TEST_SOCKET_FAMILY, socket_family},
    {NULL, NULL},
};

/******************************************************************************
 *
 * Helper macros.
 *
 ******************************************************************************/

static void *run(void *arg)
{
	struct dqlite *d = arg;
	int rc;
	rc = dqlite_run(d);
	if (rc) {
		return (void *)1;
	}
	return NULL;
}

#define BOOTSTRAP(I, N, SERVERS)                                           \
	{                                                                  \
		int rv_;                                                   \
		rv_ = dqlite_bootstrap(&f->servers[I].dqlite, N, SERVERS); \
		munit_assert_int(rv_, ==, 0);                              \
	}

/* Run the given dqlite server in a thread */
#define START(I) THREAD_START(f->servers[I].thread, run, &f->servers[i].dqlite)

/* Wait for the server to be ready */
#define READY(I) munit_assert_true(dqlite_ready(&f->servers[i].dqlite))

/* Stop the server and wait for it to be done */
#define STOP(I)                             \
	dqlite_stop(&f->servers[I].dqlite); \
	THREAD_JOIN(f->servers[I].thread)

/* Handle a new connection */
#define HANDLE(I, FD)                                           \
	{                                                       \
		int rv_;                                        \
		rv_ = dqlite_handle(&f->servers[I].dqlite, FD); \
		munit_assert_int(rv_, ==, 0);                   \
	}

/* Send the initial client handshake. */
#define HANDSHAKE                                      \
	{                                              \
		int rv_;                               \
		rv_ = clientSendHandshake(&f->client); \
		munit_assert_int(rv_, ==, 0);          \
	}

/* Send a join request. */
#define JOIN(ID, ADDRESS)                                      \
	{                                                      \
		int rv_;                                       \
		rv_ = clientSendJoin(&f->client, ID, ADDRESS); \
		munit_assert_int(rv_, ==, 0);                  \
		rv_ = clientRecvEmpty(&f->client);             \
		munit_assert_int(rv_, ==, 0);                  \
	}

/* Send a promote request. */
#define PROMOTE(ID)                                      \
	{                                                \
		int rv_;                                 \
		rv_ = clientSendPromote(&f->client, ID); \
		munit_assert_int(rv_, ==, 0);            \
		rv_ = clientRecvEmpty(&f->client);       \
		munit_assert_int(rv_, ==, 0);            \
	}

/* Send a remove request. */
#define REMOVE(ID)                                      \
	{                                               \
		int rv_;                                \
		rv_ = clientSendRemove(&f->client, ID); \
		munit_assert_int(rv_, ==, 0);           \
		rv_ = clientRecvEmpty(&f->client);      \
		munit_assert_int(rv_, ==, 0);           \
	}

/* Open a test database. */
#define OPEN                                              \
	{                                                 \
		int rv_;                                  \
		rv_ = clientSendOpen(&f->client, "test"); \
		munit_assert_int(rv_, ==, 0);             \
		rv_ = clientRecvDb(&f->client);           \
		munit_assert_int(rv_, ==, 0);             \
	}

/* Prepare a statement. */
#define PREPARE(SQL, STMT_ID)                              \
	{                                                  \
		int rv_;                                   \
		rv_ = clientSendPrepare(&f->client, SQL);  \
		munit_assert_int(rv_, ==, 0);              \
		rv_ = clientRecvStmt(&f->client, STMT_ID); \
		munit_assert_int(rv_, ==, 0);              \
	}

/* Execute a statement. */
#define EXEC(STMT_ID, LAST_INSERT_ID, ROWS_AFFECTED)               \
	{                                                          \
		int rv_;                                           \
		rv_ = clientSendExec(&f->client, STMT_ID);         \
		munit_assert_int(rv_, ==, 0);                      \
		rv_ = clientRecvResult(&f->client, LAST_INSERT_ID, \
				       ROWS_AFFECTED);             \
		munit_assert_int(rv_, ==, 0);                      \
	}

/******************************************************************************
 *
 * join
 *
 ******************************************************************************/

struct join_fixture
{
	FIXTURE;
	struct client client;
};

TEST_SUITE(join);
TEST_SETUP(join)
{
	struct join_fixture *f = munit_malloc(sizeof *f);
	struct dqlite_server server;
	unsigned i;
	int rv;
	SETUP;
	server.id = f->servers[0].dqlite.config.id;
	server.address = f->servers[0].dqlite.config.address;
	BOOTSTRAP(0, 1, &server);
	for (i = 0; i < N_SERVERS; i++) {
		START(i);
		READY(i);
	};
	HANDLE(0, f->servers[0].sockets.server);
	rv = clientInit(&f->client, f->servers[0].sockets.client);
	munit_assert_int(rv, ==, 0);
	return f;
}
TEST_TEAR_DOWN(join)
{
	struct join_fixture *f = data;
	unsigned i;
	clientClose(&f->client);
	for (i = 0; i < N_SERVERS; i++) {
		STOP(i);
	};
	f->servers[0].sockets.server_disconnected = true;
	TEAR_DOWN;
	free(f);
}

TEST_CASE(join, success, params)
{
	struct join_fixture *f = data;
	unsigned id = f->servers[1].dqlite.config.id;
	const char *address = f->servers[1].dqlite.config.address;
	int fd;
	unsigned stmt_id;
	unsigned last_insert_id;
	unsigned rows_affected;
	(void)params;
	HANDSHAKE;
	JOIN(id, address);
	fd = test_socket_client_accept(&f->servers[1].sockets);
	HANDLE(1, fd);
	fd = test_socket_client_accept(&f->servers[0].sockets);
	HANDLE(0, fd);
	PROMOTE(id);
	OPEN;
	PREPARE("CREATE TABLE test (n INT)", &stmt_id);
	EXEC(stmt_id, &last_insert_id, &rows_affected);
	/* TODO: fix the standalone test for remove */
	REMOVE(id);
	return MUNIT_OK;
}

/******************************************************************************
 *
 * remove
 *
 ******************************************************************************/

struct remove_fixture
{
	FIXTURE;
	struct client client;
};

TEST_SUITE(remove);
TEST_SETUP(remove)
{
	struct remove_fixture *f = munit_malloc(sizeof *f);
	struct dqlite_server servers[N_SERVERS];
	unsigned i;
	int rv;
	SETUP;
	for (i = 0; i < N_SERVERS; i++) {
		struct dqlite_server *server = &servers[i];
		server->id = f->servers[i].dqlite.config.id;
		server->address = f->servers[i].dqlite.config.address;
	}
	BOOTSTRAP(0, N_SERVERS, servers);
	for (i = 0; i < N_SERVERS; i++) {
		START(i);
		READY(i);
	};
	HANDLE(0, f->servers[0].sockets.server);
	rv = clientInit(&f->client, f->servers[0].sockets.client);
	munit_assert_int(rv, ==, 0);
	return f;
}

TEST_TEAR_DOWN(remove)
{
	struct join_fixture *f = data;
	unsigned i;
	clientClose(&f->client);
	for (i = 0; i < N_SERVERS; i++) {
		STOP(i);
	};
	f->servers[0].sockets.server_disconnected = true;
	TEAR_DOWN;
	free(f);
}

TEST_CASE(remove, success, params)
{
	struct join_fixture *f = data;
	unsigned id = f->servers[1].dqlite.config.id;
	(void)params;
	HANDSHAKE;
	/* TODO: we need a wait for wait for the leader and automatically
	 * interconnect nodes. */
	return MUNIT_SKIP;
	REMOVE(id);
	return MUNIT_OK;
}
