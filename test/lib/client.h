/* Setup a test dqlite client. */

#include "endpoint.h"

#ifndef TEST_CLIENT_H
#define TEST_CLIENT_H

#define FIXTURE_CLIENT                 \
	struct client client;          \
	struct test_endpoint endpoint; \
	int server

#define SETUP_CLIENT                                                    \
	{                                                               \
		int _rv;                                                \
		int _client;                                            \
		test_endpoint_setup(&f->endpoint, params);              \
		_rv = listen(f->endpoint.fd, 16);                       \
		munit_assert_int(_rv, ==, 0);                           \
		test_endpoint_pair(&f->endpoint, &f->server, &_client); \
		clientInit(&f->client, _client);                        \
	}

#define TEAR_DOWN_CLIENT         \
	clientClose(&f->client); \
	test_endpoint_tear_down(&f->endpoint)

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

/* Send the initial client handshake for a specific client. */
#define HANDSHAKE_C(CLIENT)                           \
	{                                             \
		int rv_;                              \
		rv_ = clientSendHandshake(CLIENT);    \
		munit_assert_int(rv_, ==, 0);         \
	}

/* Send an add request. */
#define ADD(ID, ADDRESS)                                     \
	{                                                    \
		int rv_;                                     \
		rv_ = clientSendAdd(f->client, ID, ADDRESS); \
		munit_assert_int(rv_, ==, 0);                \
		rv_ = clientRecvEmpty(f->client);            \
		munit_assert_int(rv_, ==, 0);                \
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

/* Send a transfer request. */
#define TRANSFER(ID, CLIENT)                             \
	{                                                \
		int rv_;                                 \
		rv_ = clientSendTransfer(CLIENT, ID);    \
		munit_assert_int(rv_, ==, 0);            \
		rv_ = clientRecvEmpty(CLIENT);           \
		munit_assert_int(rv_, ==, 0);            \
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

/* Open a test database with a specific name. */
#define OPEN_NAME(NAME)                                  \
	{                                                \
		int rv_;                                 \
		rv_ = clientSendOpen(f->client, NAME);   \
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

#define PREPARE_FAIL(SQL, STMT_ID, RV, MSG)                  \
	{                                                    \
		int rv_;                                     \
		rv_ = clientSendPrepare(f->client, SQL);     \
		munit_assert_int(rv_, ==, 0);                \
		rv_ = clientRecvFailure(f->client, RV, MSG); \
		munit_assert_int(rv_, ==, 0);                \
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
#endif /* TEST_CLIENT_H */
