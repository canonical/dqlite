#include <assert.h>
#include <unistd.h>

#include <uv.h>

#include "../include/dqlite.h"
#include "../src/binary.h"
#include "../src/conn.h"
#include "../src/metrics.h"
#include "../src/options.h"

#include "case.h"
#include "cluster.h"
#include "log.h"
#include "munit.h"
#include "socket.h"

/******************************************************************************
 *
 * Helpers
 *
 ******************************************************************************/

struct fixture {
	struct test_socket_pair sockets;
	struct dqlite__options  options;
	struct dqlite__metrics  metrics;
	uv_loop_t               loop;
	struct dqlite__conn *   conn;
	struct dqlite__response response;
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
	uint64_t buf = dqlite__flip64(protocol);

	__send_data(f, &buf, sizeof buf);
}

/* Receive a full response from the server connection. */
static void __recv_response(struct fixture *f)
{
	int      err;
	uv_buf_t buf;
	ssize_t  nread;

	dqlite__message_header_recv_start(&f->response.message, &buf);

	nread = read(f->sockets.client, buf.base, buf.len);
	munit_assert_int(nread, ==, buf.len);

	err = dqlite__message_header_recv_done(&f->response.message);
	munit_assert_int(err, ==, 0);

	err = dqlite__message_body_recv_start(&f->response.message, &buf);
	munit_assert_int(err, ==, 0);

	nread = read(f->sockets.client, buf.base, buf.len);
	munit_assert_int(nread, ==, buf.len);

	err = dqlite__response_decode(&f->response);
	munit_assert_int(err, ==, 0);

	dqlite__message_recv_reset(&f->response.message);
}

/******************************************************************************
 *
 * Parameters
 *
 ******************************************************************************/

/* Run the tests using both TCP and Unix sockets. */
static MunitParameterEnum params[] = {
    {TEST_SOCKET_PARAM, test_socket_param_values},
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
	int             err;

	test_case_setup(params, user_data);
	test_socket_pair_setup(params, &f->sockets);

	f->conn = sqlite3_malloc(sizeof *f->conn);
	munit_assert_ptr_not_null(f->conn);

	err = uv_loop_init(&f->loop);
	munit_assert_int(err, ==, 0);

	dqlite__conn_init(f->conn,
	                  f->sockets.server,
	                  test_logger(),
	                  test_cluster(),
	                  &f->loop,
	                  &f->options,
	                  &f->metrics);

	dqlite__response_init(&f->response);

	dqlite__options_defaults(&f->options);
	dqlite__metrics_init(&f->metrics);

	err = dqlite__conn_start(f->conn);
	munit_assert_int(err, ==, 0);

	return f;
}

static void tear_down(void *data)
{
	struct fixture *f = data;
	int             err;

	dqlite__response_close(&f->response);

	err = uv_loop_close(&f->loop);
	munit_assert_int(err, ==, 0);

	test_socket_pair_tear_down(&f->sockets);
	test_case_tear_down(data);
}

/******************************************************************************
 *
 * dqlite__conn_abort
 *
 ******************************************************************************/

static MunitResult test_abort_immediately(const MunitParameter params[],
                                          void *               data)
{
	struct fixture *f = data;

	(void)params;

	/* Drop the client connection immediately. */
	test_socket_pair_client_disconnect(&f->sockets);

	__run_loop(f, 1);
	__run_loop(f, 0);

	return MUNIT_OK;
}

static MunitResult test_abort_during_handshake(const MunitParameter params[],
                                               void *               data)
{
	struct fixture *f = data;
	uint64_t        protocol = dqlite__flip64(DQLITE_PROTOCOL_VERSION);

	(void)params;

	/* Write part of the handshake then drop the client connection. */
	__send_data(f, &protocol, sizeof protocol - 5);

	__run_loop(f, 1);

	test_socket_pair_client_disconnect(&f->sockets);

	__run_loop(f, 1);
	__run_loop(f, 0);

	return MUNIT_OK;
}

static MunitResult test_abort_after_handshake(const MunitParameter params[],
                                              void *               data)
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

static MunitResult test_abort_during_header(const MunitParameter params[],
                                            void *               data)
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

static MunitResult test_abort_after_header(const MunitParameter params[],
                                           void *               data)
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

static MunitResult test_abort_during_body(const MunitParameter params[],
                                          void *               data)
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

static MunitResult test_abort_after_body(const MunitParameter params[],
                                         void *               data)
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

static MunitResult
test_abort_after_heartbeat_timeout(const MunitParameter params[], void *data)
{
	struct fixture *f = data;
	uint64_t        protocol = dqlite__flip64(DQLITE_PROTOCOL_VERSION);
	uint8_t         buf[3] = {
            /* Incomplete header */
            0,
            0,
            0,
        };
	ssize_t nwrite;

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

static MunitTest dqlite__conn_abort_tests[] = {
    {"/immediately", test_abort_immediately, setup, tear_down, 0, params},
    {"/during-handshake",
     test_abort_during_handshake,
     setup,
     tear_down,
     0,
     params},
    {"/after-handshake",
     test_abort_after_handshake,
     setup,
     tear_down,
     0,
     params},
    {"/during-header", test_abort_during_header, setup, tear_down, 0, params},
    {"/after-header", test_abort_after_header, setup, tear_down, 0, params},
    {"/during-body", test_abort_during_body, setup, tear_down, 0, params},
    {"/after-body", test_abort_after_body, setup, tear_down, 0, params},
    //	{"after heartbeat timeout",
    // test_dqlite__conn_abort_after_heartbeat_timeout},
    {NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL},
};

/******************************************************************************
 *
 * dqlite__conn_read_cb
 *
 ******************************************************************************/

static MunitResult test_read_cb_bad_protocol(const MunitParameter params[],
                                             void *               data)
{
	struct fixture *f = data;

	(void)params;

	/* Write an unknonw protocol version. */
	__send_handshake(f, 0x123456);

	__run_loop(f, 1);
	__run_loop(f, 0);

	return MUNIT_OK;
}

static MunitResult test_read_cb_empty_body(const MunitParameter params[],
                                           void *               data)
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

static MunitResult test_read_cb_body_too_big(const MunitParameter params[],
                                             void *               data)
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

static MunitResult test_read_cb_bad_body(const MunitParameter params[],
                                         void *               data)
{
	struct fixture *f = data;
	uint8_t         buf[][8] = {
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

static MunitResult test_read_cb_invalid_db_id(const MunitParameter params[],
                                              void *               data)
{
	struct fixture *f = data;
	uint8_t         buf[][8] = {
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

static MunitResult test_read_cb_throttle(const MunitParameter params[],
                                         void *               data)
{
	struct fixture *f = data;
	uint8_t         buf[][8] = {
            {1, 0, 0, 0, 0, 0, 0, 0},
            {0, 0, 0, 0, 0, 0, 0, 0},
            {1, 0, 0, 0, 0, 0, 0, 0},
            {0, 0, 0, 0, 0, 0, 0, 0},
        };

	(void)params;

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

static MunitTest dqlite__conn_read_cb_tests[] = {
    {"/bad-protocol", test_read_cb_bad_protocol, setup, tear_down, 0, params},
    {"/empty-body", test_read_cb_empty_body, setup, tear_down, 0, params},
    {"/body-too-big", test_read_cb_body_too_big, setup, tear_down, 0, params},
    {"/bad-body", test_read_cb_bad_body, setup, tear_down, 0, params},
    {"/invalid-db-id", test_read_cb_invalid_db_id, setup, tear_down, 0, params},
    {"/throttle", test_read_cb_throttle, setup, tear_down, 0, params},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * Suite
 *
 ******************************************************************************/

MunitSuite dqlite__conn_suites[] = {
    {"_abort", dqlite__conn_abort_tests, NULL, 1, 0},
    {"_read_cb", dqlite__conn_read_cb_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
