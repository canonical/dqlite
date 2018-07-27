/******************************************************************************
 *
 * Test client speaking the dqlite wire protocol.
 *
 ******************************************************************************/

#ifndef DQLITE_TEST_CLIENT_H
#define DQLITE_TEST_CLIENT_H

#include <stdint.h>

#include <uv.h>

#include "../src/message.h"
#include "../src/request.h"
#include "../src/response.h"

struct test_client {
	int                     fd;
	struct dqlite__request  request;
	struct dqlite__response response;
	uv_buf_t                bufs[3];
};

struct test_client_result {
	uint64_t last_insert_id;
	uint64_t rows_affected;
};

struct test_client_row {
	uint8_t *               types;
	void **                 values;
	struct test_client_row *next;
};

struct test_client_rows {
	struct dqlite__message *message;
	uint64_t                column_count;
	const char **           column_names;
	struct test_client_row *next;
};

/* Initialize a test client.
 *
 * @fd: The file descriptor for writing requests and reading responses.
 */
void test_client_init(struct test_client *c, int fd);

/* Deallocate the memory used by the test client, if any. */
void test_client_close(struct test_client *c);

/* Initialize the connection by writing the protocol version. */
void test_client_handshake(struct test_client *c);

/* Perform a leader request */
void test_client_leader(struct test_client *c, char **leader);

/* Perform a client request */
void test_client_client(struct test_client *c, uint64_t *heartbeat);

/* Open a database */
void test_client_open(struct test_client *c, const char *name, uint32_t *db_id);

/* Prepare a statement */
void test_client_prepare(struct test_client *c,
                         uint32_t            db_id,
                         const char *        sql,
                         uint32_t *          stmt_id);

/* Execute a prepared statement */
void test_client_exec(struct test_client *       c,
                      uint32_t                   db_id,
                      uint32_t                   stmt_id,
                      struct test_client_result *result);

/* Fetch the result of a prepared statement */
void test_client_query(struct test_client *     c,
                       uint32_t                 db_id,
                       uint32_t                 stmt_id,
                       struct test_client_rows *rows);

/* Reset the underlying message */
void test_client_rows_close(struct test_client_rows *rows);

/* Destroy a prepared statement */
void test_client_finalize(struct test_client *c, uint32_t db_id, uint32_t stmt_id);

#endif /* DQLITE_TEST_CLIENT_H */
