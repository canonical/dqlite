#include <unistd.h>

#include <uv.h>

#include "../include/dqlite.h"
#include "../src/byte.h"
#include "../src/conn.h"
#include "../src/metrics.h"
#include "../src/options.h"

#include "./lib/heap.h"
#include "./lib/runner.h"
#include "./lib/socket.h"
#include "./lib/sqlite.h"
#include "case.h"
#include "cluster.h"
#include "log.h"

TEST_MODULE(conn);

/******************************************************************************
 *
 * Helpers
 *
 ******************************************************************************/

struct fixture
{
	struct test_socket_pair sockets;
	struct dqlite__options options;
	struct dqlite__metrics metrics;
	dqlite_logger *logger;
	dqlite_cluster *cluster;
	uv_loop_t loop;
	struct conn *conn;
	struct response response;
};

/* Run the fixture loop once.
 *
 * If expect_more_callbacks is true, then assert that no more callbacks are
 * expected, otherwise assert that more callbacks are expected. */
static void __run_loop(struct fixture *f, int expect_more_callbacks)
{
	int rc;

	rc = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(rc, ==, expect_more_callbacks);

	if (!expect_more_callbacks) {
		/* The server connection should have been closed, since there's
		 * no more data to process. */
		f->sockets.server_disconnected = 1;
	}
}

/* Send data from the client connection to the server connection.
 *
 * Expect all bytes to be written. */
static void __send_data(struct fixture *f, void *buf, size_t count)
{
	int nwrite;

	nwrite = write(f->sockets.client, buf, count);
	munit_assert_int(nwrite, ==, count);
}

/* Send a full handshake using the given protocol version. */
static void __send_handshake(struct fixture *f, uint64_t protocol)
{
	uint64_t buf = byte__flip64(protocol);

	__send_data(f, &buf, sizeof buf);
}

/* Receive a full response from the server connection. */
static void __recv_response(struct fixture *f)
{
	int err;
	uv_buf_t buf;
	ssize_t nread;

	message__header_recv_start(&f->response.message, &buf);

	nread = read(f->sockets.client, buf.base, buf.len);
	munit_assert_int(nread, ==, buf.len);

	err = message__header_recv_done(&f->response.message);
	munit_assert_int(err, ==, 0);

	err = message__body_recv_start(&f->response.message, &buf);
	munit_assert_int(err, ==, 0);

	nread = read(f->sockets.client, buf.base, buf.len);
	munit_assert_int(nread, ==, buf.len);

	err = response_decode(&f->response);
	munit_assert_int(err, ==, 0);

	message__recv_reset(&f->response.message);
}

/******************************************************************************
 *
 * Parameters
 *
 ******************************************************************************/

/* Run the tests using both TCP and Unix sockets. */
static MunitParameterEnum params[] = {
    {TEST_SOCKET_FAMILY, test_socket_param_values},
    {NULL, NULL},
};

/******************************************************************************
 *
 * Setup and tear down
 *
 ******************************************************************************/

static void *setup(const MunitParameter params[], void *user_data)
{
	struct fixture *f = munit_malloc(sizeof *f);
	int err;

	test_heap_setup(params, user_data);
	test_sqlite_setup(params);
	test_socket_pair_setup(params, &f->sockets);

	f->logger = test_logger();
	f->cluster = test_cluster();

	f->conn = sqlite3_malloc(sizeof *f->conn);
	munit_assert_ptr_not_null(f->conn);

	err = uv_loop_init(&f->loop);
	munit_assert_int(err, ==, 0);

	conn__init(f->conn, f->sockets.server, f->logger, f->cluster, &f->loop,
		   &f->options, &f->metrics);

	response_init(&f->response);

	dqlite__options_defaults(&f->options);
	dqlite__metrics_init(&f->metrics);

	err = conn__start(f->conn);
	munit_assert_int(err, ==, 0);

	return f;
}

static void tear_down(void *data)
{
	struct fixture *f = data;
	int err;

	response_close(&f->response);

	err = uv_loop_close(&f->loop);
	munit_assert_int(err, ==, 0);

	test_socket_pair_tear_down(&f->sockets);
	test_sqlite_tear_down();
	test_heap_tear_down(data);
	test_cluster_close(f->cluster);

	free(f->logger);
	free(f);
}

/******************************************************************************
 *
 * conn__abort
 *
 ******************************************************************************/

TEST_SUITE(abort);
TEST_SETUP(abort, setup);
TEST_TEAR_DOWN(abort, tear_down);

TEST_CASE(abort, immediately, NULL)
{
	struct fixture *f = data;

	(void)params;

	/* Drop the client connection immediately. */
	test_socket_pair_client_disconnect(&f->sockets);

	__run_loop(f, 1);
	__run_loop(f, 0);

	return MUNIT_OK;
}

TEST_CASE(abort, during_handshake, NULL)
{
	struct fixture *f = data;
	uint64_t protocol = byte__flip64(DQLITE_PROTOCOL_VERSION);

	(void)params;

	/* Write part of the handshake then drop the client connection. */
	__send_data(f, &protocol, sizeof protocol - 5);

	__run_loop(f, 1);

	test_socket_pair_client_disconnect(&f->sockets);

	__run_loop(f, 1);
	__run_loop(f, 0);

	return MUNIT_OK;
}

TEST_CASE(abort, after_handshake, NULL)
{
	struct fixture *f = data;

	(void)params;

	/* Write the handshake then drop the client connection. */
	__send_handshake(f, DQLITE_PROTOCOL_VERSION);

	__run_loop(f, 1);

	test_socket_pair_client_disconnect(&f->sockets);

	__run_loop(f, 1);
	__run_loop(f, 0);

	return MUNIT_OK;
}

TEST_CASE(abort, during_header, NULL)
{
	struct fixture *f = data;

	uint8_t buf[][8] = {
	    {0, 0, 0, 0, 0, 0, 0},
	};

	(void)params;

	/* Write the handshake. */
	__send_handshake(f, DQLITE_PROTOCOL_VERSION);

	/* Write only a part of the header then drop the client connection. */
	__send_data(f, buf, sizeof buf - 1);

	__run_loop(f, 1);

	test_socket_pair_client_disconnect(&f->sockets);

	__run_loop(f, 1);
	__run_loop(f, 0);

	return MUNIT_OK;
}

TEST_CASE(abort, after_header, NULL)
{
	struct fixture *f = data;

	uint8_t buf[][8] = {
	    {1, 0, 0, 0, 0, 0, 0, 0},
	};

	(void)params;

	/* Write the handshake. */
	__send_handshake(f, DQLITE_PROTOCOL_VERSION);

	/* Write a full request header then drop the connection. */
	__send_data(f, buf, sizeof buf);

	__run_loop(f, 1);

	test_socket_pair_client_disconnect(&f->sockets);

	__run_loop(f, 1);
	__run_loop(f, 0);

	return MUNIT_OK;
}

TEST_CASE(abort, during_body, NULL)
{
	struct fixture *f = data;

	uint8_t buf[][8] = {
	    {1, 0, 0, 0, 0, 0, 0, 0},
	    {0, 0, 0, 0, 0, 0, 0, 0},
	};

	(void)params;

	/* Write the handshake. */
	__send_handshake(f, DQLITE_PROTOCOL_VERSION);

	/* Write the header and just a part of the body. */
	__send_data(f, buf, sizeof buf - 5);

	__run_loop(f, 1);

	test_socket_pair_client_disconnect(&f->sockets);

	__run_loop(f, 1);
	__run_loop(f, 0);

	return MUNIT_OK;
}

TEST_CASE(abort, after_body, NULL)
{
	struct fixture *f = data;

	uint8_t buf[][8] = {
	    {1, 0, 0, 0, 0, 0, 0, 0},
	    {0, 0, 0, 0, 0, 0, 0, 0},
	};

	(void)params;

	/* Write the handshake. */
	__send_handshake(f, DQLITE_PROTOCOL_VERSION);

	/* Write a full leader request */
	__send_data(f, buf, sizeof buf);

	__run_loop(f, 1);

	test_socket_pair_client_disconnect(&f->sockets);

	__run_loop(f, 1);
	__run_loop(f, 0);

	return MUNIT_OK;
}

TEST_CASE(abort, after_heartbeat_timeout, NULL)
{
	struct fixture *f = data;
	uint64_t protocol = byte__flip64(DQLITE_PROTOCOL_VERSION);
	uint8_t buf[3] = {
	    /* Incomplete header */
	    0,
	    0,
	    0,
	};
	ssize_t nwrite;

	__send_handshake(f, 0x123456); /* Only for SKIP, to remove */
	__run_loop(f, 1);	      /* Only for SKIP, to remove */
	__run_loop(f, 0);	      /* Only for SKIP, to remove */
	return MUNIT_SKIP;

	(void)params;

	f->conn->options->heartbeat_timeout = 1; /* Abort after a millisecond */

	nwrite = write(f->sockets.client, &protocol, sizeof(protocol));
	munit_assert_int(nwrite, ==, sizeof(protocol));

	nwrite = write(f->sockets.client, buf, 3);
	munit_assert_int(nwrite, ==, 3);

	usleep(2 * 1000);

	__run_loop(f, 0);

	return MUNIT_OK;
}

/******************************************************************************
 *
 * conn__read_cb
 *
 ******************************************************************************/

TEST_SUITE(read_cb);
TEST_SETUP(read_cb, setup);
TEST_TEAR_DOWN(read_cb, tear_down);

TEST_CASE(read_cb, bad_protocol, NULL)
{
	struct fixture *f = data;

	(void)params;

	/* Write an unknonw protocol version. */
	__send_handshake(f, 0x123456);

	__run_loop(f, 1);
	__run_loop(f, 0);

	return MUNIT_OK;
}

TEST_CASE(read_cb, empty_body, NULL)
{
	struct fixture *f = data;

	uint8_t buf[][8] = {
	    {0, 0, 0, 0, 0, 0, 0, 0},
	};

	(void)params;

	/* Write the handshake. */
	__send_handshake(f, DQLITE_PROTOCOL_VERSION);

	__run_loop(f, 1);

	/* Write a header whose body words count field is zero. */
	__send_data(f, buf, sizeof buf);

	__run_loop(f, 1);

	__recv_response(f);

	munit_assert_int(f->response.type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response.failure.code, ==, DQLITE_PROTO);
	munit_assert_string_equal(
	    f->response.failure.message,
	    "failed to parse request header: empty message body");

	test_socket_pair_client_disconnect(&f->sockets);

	__run_loop(f, 1);
	__run_loop(f, 0);

	return MUNIT_OK;
}

TEST_CASE(read_cb, body_too_big, NULL)
{
	struct fixture *f = data;

	/* Header indicating a body which is too large */
	uint8_t buf[][8] = {
	    {0xf, 0xf, 0xf, 0xf, 0, 0, 0, 0},
	};

	(void)params;

	/* Write the handshake. */
	__send_handshake(f, DQLITE_PROTOCOL_VERSION);

	__run_loop(f, 1);

	/* Write a header whose body words count is way to large. */
	__send_data(f, buf, sizeof buf);

	__run_loop(f, 1);

	__recv_response(f);

	munit_assert_int(f->response.type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response.failure.code, ==, DQLITE_PROTO);
	munit_assert_string_equal(
	    f->response.failure.message,
	    "failed to parse request header: message body too large");

	test_socket_pair_client_disconnect(&f->sockets);

	__run_loop(f, 1);
	__run_loop(f, 0);

	return MUNIT_OK;
}

TEST_CASE(read_cb, bad_body, NULL)
{
	struct fixture *f = data;
	uint8_t buf[][8] = {
	    {3, 0, 0, 0, DQLITE_REQUEST_OPEN, 0, 0, 0},
	    {'t', 'e', 's', 't', '.', 'd', 'b', 0},
	    {0, 0, 0, 0, 0, 0, 0, 0},
	    {'v', 'o', 'l', 'a', 't', 'i', 'e', 'x'},
	};

	(void)params;

	/* Write the handshake. */
	__send_handshake(f, DQLITE_PROTOCOL_VERSION);

	__run_loop(f, 1);

	/* Send a full open request whose vfs name is invalid. */
	__send_data(f, buf, sizeof buf);

	__run_loop(f, 1);

	__recv_response(f);

	munit_assert_int(f->response.type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response.failure.code, ==, DQLITE_PARSE);
	munit_assert_string_equal(
	    f->response.failure.message,
	    "failed to decode request: failed to decode 'open': "
	    "failed to get 'vfs' field: no string found");

	test_socket_pair_client_disconnect(&f->sockets);

	__run_loop(f, 1);
	__run_loop(f, 0);

	return MUNIT_OK;
}

TEST_CASE(read_cb, invalid_db_id, NULL)
{
	struct fixture *f = data;
	uint8_t buf[][8] = {
	    {2, 0, 0, 0, DQLITE_REQUEST_PREPARE, 0, 0, 0},
	    {1, 0, 0, 0, 0, 0, 0, 0},
	    {'S', 'E', 'L', 'E', 'C', 'T', '1', 0},
	};

	(void)params;

	/* Write the handshake. */
	__send_handshake(f, DQLITE_PROTOCOL_VERSION);

	__run_loop(f, 1);

	/* Send a full Prepare request with and invalid db_id */
	__send_data(f, buf, sizeof buf);

	__run_loop(f, 1);

	__recv_response(f);

	munit_assert_int(f->response.type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response.failure.code, ==, SQLITE_NOTFOUND);
	munit_assert_string_equal(f->response.failure.message,
				  "no db with id 1");

	test_socket_pair_client_disconnect(&f->sockets);

	__run_loop(f, 1);
	__run_loop(f, 0);

	return MUNIT_OK;
}

TEST_CASE(read_cb, throttle, NULL)
{
	struct fixture *f = data;
	uint8_t buf[][8] = {
	    {1, 0, 0, 0, 0, 0, 0, 0},
	    {0, 0, 0, 0, 0, 0, 0, 0},
	    {1, 0, 0, 0, 0, 0, 0, 0},
	    {0, 0, 0, 0, 0, 0, 0, 0},
	};

	(void)params;

	__send_handshake(f, 0x123456); /* Only for SKIP, to remove */
	__run_loop(f, 1);	      /* Only for SKIP, to remove */
	__run_loop(f, 0);	      /* Only for SKIP, to remove */
	return MUNIT_SKIP;

	/* Write the handshake. */
	__send_handshake(f, DQLITE_PROTOCOL_VERSION);

	__run_loop(f, 1);

	/* Send two full consecutive leader requests, without waiting for the
	 * response first. */
	__send_data(f, buf, sizeof buf);

	__run_loop(f, 1);

	__recv_response(f);
	__run_loop(f, 1);

	__recv_response(f);

	test_socket_pair_client_disconnect(&f->sockets);

	__run_loop(f, 1);
	__run_loop(f, 0);

	return MUNIT_OK;
}
