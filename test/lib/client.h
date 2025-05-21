/* Setup a test dqlite client. */

#include "endpoint.h"

#ifndef TEST_CLIENT_H
#define TEST_CLIENT_H

#define FIXTURE_CLIENT                 \
	struct client_proto client;    \
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
		memset(&f->client, 0, sizeof f->client);                \
		buffer__init(&f->client.read);                          \
		buffer__init(&f->client.write);                         \
		f->client.fd = _client;                                 \
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
#define HANDSHAKE                                           \
	{                                                   \
		int rv_;                                    \
		rv_ = clientSendHandshake(f->client, NULL); \
		munit_assert_int(rv_, ==, 0);               \
	}

/* Send the initial client handshake for a specific client. */
#define HANDSHAKE_C(CLIENT)                              \
	{                                                \
		int rv_;                                 \
		rv_ = clientSendHandshake(CLIENT, NULL); \
		munit_assert_int(rv_, ==, 0);            \
	}

/* Send an add request. */
#define ADD(ID, ADDRESS)                                           \
	{                                                          \
		int rv_;                                           \
		rv_ = clientSendAdd(f->client, ID, ADDRESS, NULL); \
		munit_assert_int(rv_, ==, 0);                      \
		rv_ = clientRecvEmpty(f->client, NULL);            \
		munit_assert_int(rv_, ==, 0);                      \
	}

/* Send an assign role request. */
#define ASSIGN(ID, ROLE)                                           \
	{                                                          \
		int rv_;                                           \
		rv_ = clientSendAssign(f->client, ID, ROLE, NULL); \
		munit_assert_int(rv_, ==, 0);                      \
		rv_ = clientRecvEmpty(f->client, NULL);            \
		munit_assert_int(rv_, ==, 0);                      \
	}

/* Send a remove request. */
#define REMOVE(ID)                                           \
	{                                                    \
		int rv_;                                     \
		rv_ = clientSendRemove(f->client, ID, NULL); \
		munit_assert_int(rv_, ==, 0);                \
		rv_ = clientRecvEmpty(f->client, NULL);      \
		munit_assert_int(rv_, ==, 0);                \
	}

/* Send a transfer request. */
#define TRANSFER(ID, CLIENT)                                \
	{                                                   \
		int rv_;                                    \
		rv_ = clientSendTransfer(CLIENT, ID, NULL); \
		munit_assert_int(rv_, ==, 0);               \
		rv_ = clientRecvEmpty(CLIENT, NULL);        \
		munit_assert_int(rv_, ==, 0);               \
	}

#define OPEN_C(CLIENT, NAME)                        \
	{                                               \
		int rv_;                                    \
		rv_ = clientSendOpen(CLIENT, NAME, NULL);   \
		munit_assert_int(rv_, ==, 0);               \
		rv_ = clientRecvDb(CLIENT, NULL);           \
		munit_assert_int(rv_, ==, 0);               \
	}

/* Open a test database. */
#define OPEN OPEN_C(f->client, "test")

/* Open a test database with a specific name. */
#define OPEN_NAME(NAME)                                      \
	{                                                    \
		int rv_;                                     \
		rv_ = clientSendOpen(f->client, NAME, NULL); \
		munit_assert_int(rv_, ==, 0);                \
		rv_ = clientRecvDb(f->client, NULL);         \
		munit_assert_int(rv_, ==, 0);                \
	}

#define PREPARE_C(CLIENT, SQL, STMT_ID)                                                \
	{                                                                   \
		int rv_;                                                    \
		rv_ = clientSendPrepare(CLIENT, SQL, NULL);              \
		munit_assert_int(rv_, ==, 0);                               \
		rv_ = clientRecvStmt(CLIENT, STMT_ID, NULL, NULL, NULL); \
		munit_assert_int(rv_, ==, 0);                               \
	}

/* Prepare a statement. */
#define PREPARE(SQL, STMT_ID) PREPARE_C(f->client, SQL, STMT_ID)

#define PREPARE_FAIL(SQL, STMT_ID, RV, MSG)                        \
	{                                                          \
		int rv_;                                           \
		rv_ = clientSendPrepare(f->client, SQL, NULL);     \
		munit_assert_int(rv_, ==, 0);                      \
		rv_ = clientRecvFailure(f->client, RV, MSG, NULL); \
		munit_assert_int(rv_, ==, 0);                      \
	}

#define EXEC_C(CLIENT , STMT_ID, LAST_INSERT_ID, ROWS_AFFECTED)  \
	{                                                            \
		int rv_;                                                 \
		rv_ = clientSendExec(CLIENT, STMT_ID, NULL, 0, NULL); \
		munit_assert_int(rv_, ==, 0);                            \
		rv_ = clientRecvResult(CLIENT, LAST_INSERT_ID,        \
				       ROWS_AFFECTED, NULL);                     \
		munit_assert_int(rv_, ==, 0);                            \
	}

/* Execute a statement. */
#define EXEC(STMT_ID, LAST_INSERT_ID, ROWS_AFFECTED) EXEC_C(f->client, STMT_ID, LAST_INSERT_ID, ROWS_AFFECTED)

#define EXEC_PARAMS(STMT_ID, LAST_INSERT_ID, ROWS_AFFECTED, ...) \
	{ \
		int rv_; \
		struct value vals_[] = {__VA_ARGS__}; \
		size_t len_ = sizeof(vals_) / sizeof(vals_[0]); \
		rv_ = clientSendExec(f->client, STMT_ID, vals_, len_, NULL); \
		munit_assert_int(rv_, ==, 0); \
		rv_ = clientRecvResult(f->client, LAST_INSERT_ID, \
				       ROWS_AFFECTED, NULL); \
		munit_assert_int(rv_, ==, 0); \
	}

#define EXEC_SQL(SQL, LAST_INSERT_ID, ROWS_AFFECTED)                    \
	{                                                               \
		int rv_;                                                \
		rv_ = clientSendExecSQL(f->client, SQL, NULL, 0, NULL); \
		munit_assert_int(rv_, ==, 0);                           \
		rv_ = clientRecvResult(f->client, LAST_INSERT_ID,       \
				       ROWS_AFFECTED, NULL);            \
		munit_assert_int(rv_, ==, 0);                           \
	}

/* Perform a query. */
#define QUERY(STMT_ID, ROWS)                                              \
	{                                                                 \
		int rv_;                                                  \
		rv_ = clientSendQuery(f->client, STMT_ID, NULL, 0, NULL); \
		munit_assert_int(rv_, ==, 0);                             \
		rv_ = clientRecvRows(f->client, ROWS, NULL, NULL);        \
		munit_assert_int(rv_, ==, 0);                             \
	}

#define QUERY_DONE_C(CLIENT, STMT_ID, ROWS, ROWS_HOOK)                                  \
	{                                                                     \
		int rv_;                                                      \
		bool done;                                                    \
		rv_ = clientSendQuery(CLIENT, STMT_ID, NULL, 0, NULL);     \
		munit_assert_int(rv_, ==, 0);                                 \
		do {                                                          \
			rv_ = clientRecvRows(CLIENT, (ROWS), &done, NULL); \
			munit_assert_int(rv_, ==, 0);                         \
			ROWS_HOOK;                                            \
			clientCloseRows((ROWS));                              \
			*(ROWS) = (struct rows){};                            \
		} while (!done);                                              \
	}


#define QUERY_DONE(STMT_ID, ROWS, ROWS_HOOK) QUERY_DONE_C(f->client, STMT_ID, ROWS, ROWS_HOOK)

#define QUERY_SQL(SQL, ROWS)                                             \
	{                                                                \
		int rv_;                                                 \
		rv_ = clientSendQuerySQL(f->client, SQL, NULL, 0, NULL); \
		munit_assert_int(rv_, ==, 0);                            \
		rv_ = clientRecvRows(f->client, ROWS, NULL, NULL);       \
		munit_assert_int(rv_, ==, 0);                            \
	}

#define QUERY_SQL_DONE(SQL, ROWS, ROWS_HOOK)                                  \
	{                                                                     \
		int rv_;                                                      \
		bool done;                                                    \
		rv_ = clientSendQuerySQL(f->client, SQL, NULL, 0, NULL);      \
		munit_assert_int(rv_, ==, 0);                                 \
		do {                                                          \
			rv_ = clientRecvRows(f->client, (ROWS), &done, NULL); \
			munit_assert_int(rv_, ==, 0);                         \
			ROWS_HOOK;                                            \
			clientCloseRows((ROWS));                              \
			*(ROWS) = (struct rows){};                            \
		} while (!done);                                              \
	}

#endif /* TEST_CLIENT_H */
