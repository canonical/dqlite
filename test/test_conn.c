#include <assert.h>
#include <unistd.h>

#include <uv.h>

#include "../include/dqlite.h"
#include "../src/binary.h"
#include "../src/conn.h"
#include "../src/metrics.h"
#include "../src/options.h"

#include "cluster.h"
#include "leak.h"
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
	uv_loop_t               loop;
	struct dqlite__conn     conn;
	struct dqlite__response response;
	struct dqlite__options  options;
	struct dqlite__metrics  metrics;
};

static void __recv_response(struct fixture *f) {
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
static char *test_socket_families[] = {"tcp", "unix", NULL};

static MunitParameterEnum test_socket_family_params[] = {
    {"socket-family", test_socket_families},
    {NULL, NULL},
};

/******************************************************************************
 *
 * Setup and tear down
 *
 ******************************************************************************/

static void *setup(const MunitParameter params[], void *user_data) {
	struct fixture *f;
	const char *    socket_family;
	int             err;

	(void)user_data;

	socket_family = munit_parameters_get(params, "socket-family");

	f = munit_malloc(sizeof *f);

	test_socket_pair_init(&f->sockets, socket_family);

	err = uv_loop_init(&f->loop);
	munit_assert_int(err, ==, 0);

	dqlite__conn_init(&f->conn,
	                  f->sockets.server,
	                  test_logger(),
	                  test_cluster(),
	                  &f->loop,
	                  &f->options,
	                  &f->metrics);
	f->conn.logger = test_logger();

	dqlite__response_init(&f->response);

	dqlite__options_defaults(&f->options);
	dqlite__metrics_init(&f->metrics);

	return f;
}

static void tear_down(void *data) {
	struct fixture *f = data;
	int             err;

	dqlite__response_close(&f->response);
	dqlite__conn_close(&f->conn);

	err = uv_loop_close(&f->loop);
	munit_assert_int(err, ==, 0);

	test_socket_pair_close(&f->sockets);
	test_assert_no_leaks();
}

/******************************************************************************
 *
 * Tests for dqlite__conn_abort
 *
 ******************************************************************************/

static MunitResult test_abort_immediately(const MunitParameter params[],
                                          void *               data) {
	struct fixture *f = data;
	int             err;

	(void)params;

	err = dqlite__conn_start(&f->conn);
	munit_assert_int(err, ==, 0);

	test_socket_pair_client_disconnect(&f->sockets);

	err = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(err, ==, 0);

	f->sockets.server_disconnected = 1;

	munit_assert_string_equal(f->conn.error, "read error: end of file (EOF)");

	return MUNIT_OK;
}

static MunitResult test_abort_during_handshake(const MunitParameter params[],
                                               void *               data) {
	struct fixture *f = data;
	int             err;
	uint64_t        protocol = dqlite__flip64(DQLITE_PROTOCOL_VERSION);
	ssize_t         nwrite;

	(void)params;

	err = dqlite__conn_start(&f->conn);
	munit_assert_int(err, ==, 0);

	nwrite = write(f->sockets.client, &protocol, 3);
	munit_assert_int(nwrite, ==, 3);

	err = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(err, ==, 1 /* Number of pending handles */);

	test_socket_pair_client_disconnect(&f->sockets);

	err = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(err, ==, 0);

	f->sockets.server_disconnected = 1;

	munit_assert_string_equal(f->conn.error, "read error: end of file (EOF)");

	return MUNIT_OK;
}

static MunitResult test_abort_after_handshake(const MunitParameter params[],
                                              void *               data) {
	struct fixture *f = data;
	int             err;
	uint64_t        protocol = dqlite__flip64(DQLITE_PROTOCOL_VERSION);
	ssize_t         nwrite;

	(void)params;

	err = dqlite__conn_start(&f->conn);
	munit_assert_int(err, ==, 0);

	nwrite = write(f->sockets.client, &protocol, sizeof(protocol));
	munit_assert_int(nwrite, ==, sizeof(protocol));

	err = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(err, ==, 1 /* Number of pending handles */);

	test_socket_pair_client_disconnect(&f->sockets);

	err = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(err, ==, 0);

	f->sockets.server_disconnected = 1;

	munit_assert_string_equal(f->conn.error, "read error: end of file (EOF)");

	return MUNIT_OK;
}

static MunitResult test_abort_during_header(const MunitParameter params[],
                                            void *               data) {
	struct fixture *f = data;
	int             err;
	uint64_t        protocol = dqlite__flip64(DQLITE_PROTOCOL_VERSION);
	uint8_t         buf[7]   = {/* Partial header */
                          0,
                          0,
                          0,
                          0,
                          0,
                          0,
                          0};
	ssize_t         nwrite;

	(void)params;

	err = dqlite__conn_start(&f->conn);
	munit_assert_int(err, ==, 0);

	nwrite = write(f->sockets.client, &protocol, sizeof(protocol));
	munit_assert_int(nwrite, ==, sizeof(protocol));

	nwrite = write(f->sockets.client, buf, 7);
	munit_assert_int(nwrite, ==, 7);

	err = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(err, ==, 1 /* Number of pending handles */);

	test_socket_pair_client_disconnect(&f->sockets);

	err = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(err, ==, 0);

	f->sockets.server_disconnected = 1;

	munit_assert_string_equal(f->conn.error, "read error: end of file (EOF)");

	return MUNIT_OK;
}

static MunitResult test_abort_after_header(const MunitParameter params[],
                                           void *               data) {
	struct fixture *f = data;
	int             err;
	uint64_t        protocol = dqlite__flip64(DQLITE_PROTOCOL_VERSION);
	uint8_t         buf[8]   = {/* Full header */
                          1,
                          0,
                          0,
                          0,
                          0,
                          0,
                          0,
                          0};
	ssize_t         nwrite;

	(void)params;

	err = dqlite__conn_start(&f->conn);
	munit_assert_int(err, ==, 0);

	nwrite = write(f->sockets.client, &protocol, sizeof(protocol));
	munit_assert_int(nwrite, ==, sizeof(protocol));

	nwrite = write(f->sockets.client, buf, 8);
	munit_assert_int(nwrite, ==, 8);

	err = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(err, ==, 1 /* Number of pending handles */);

	test_socket_pair_client_disconnect(&f->sockets);

	err = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(err, ==, 0);

	f->sockets.server_disconnected = 1;

	munit_assert_string_equal(f->conn.error, "read error: end of file (EOF)");

	return MUNIT_OK;
}

static MunitResult test_abort_during_body(const MunitParameter params[],
                                          void *               data) {
	struct fixture *f = data;
	int             err;
	uint64_t        protocol = dqlite__flip64(DQLITE_PROTOCOL_VERSION);
	uint8_t         buf[13]  = {/* Header and partial body */
                           1,
                           0,
                           0,
                           0,
                           0,
                           0,
                           0,
                           0,
                           0,
                           0,
                           0,
                           0,
                           0};
	ssize_t         nwrite;

	(void)params;

	err = dqlite__conn_start(&f->conn);
	munit_assert_int(err, ==, 0);

	nwrite = write(f->sockets.client, &protocol, sizeof(protocol));
	munit_assert_int(nwrite, ==, sizeof(protocol));

	nwrite = write(f->sockets.client, buf, 13);
	munit_assert_int(nwrite, ==, 13);

	err = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(err, ==, 1 /* Number of pending handles */);

	test_socket_pair_client_disconnect(&f->sockets);

	err = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(err, ==, 0);

	f->sockets.server_disconnected = 1;

	munit_assert_string_equal(f->conn.error, "read error: end of file (EOF)");

	return MUNIT_OK;
}

static MunitResult test_abort_after_body(const MunitParameter params[], void *data) {
	struct fixture *f = data;
	int             err;
	uint64_t        protocol = dqlite__flip64(DQLITE_PROTOCOL_VERSION);
	uint8_t         buf[16]  = {
            /* Header and body (Leader request) */
            1,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        };
	ssize_t nwrite;

	(void)params;

	err = dqlite__conn_start(&f->conn);
	munit_assert_int(err, ==, 0);

	nwrite = write(f->sockets.client, &protocol, sizeof(protocol));
	munit_assert_int(nwrite, ==, sizeof(protocol));

	nwrite = write(f->sockets.client, buf, 16);
	munit_assert_int(nwrite, ==, 16);

	err = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(err, ==, 1 /* Number of pending handles */);

	test_socket_pair_client_disconnect(&f->sockets);

	err = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(err, ==, 0);

	f->sockets.server_disconnected = 1;

	munit_assert_string_equal(
	    f->conn.error, "read error: connection reset by peer (ECONNRESET)");

	return MUNIT_OK;
}

static MunitResult test_abort_after_heartbeat_timeout(const MunitParameter params[],
                                                      void *               data) {
	struct fixture *f = data;
	int             err;
	uint64_t        protocol = dqlite__flip64(DQLITE_PROTOCOL_VERSION);
	uint8_t         buf[3]   = {
            /* Incomplete header */
            0,
            0,
            0,
        };
	ssize_t nwrite;

	(void)params;

	f->conn.options->heartbeat_timeout = 1; /* Abort after a millisecond */

	err = dqlite__conn_start(&f->conn);
	munit_assert_int(err, ==, 0);

	nwrite = write(f->sockets.client, &protocol, sizeof(protocol));
	munit_assert_int(nwrite, ==, sizeof(protocol));

	nwrite = write(f->sockets.client, buf, 3);
	munit_assert_int(nwrite, ==, 3);

	usleep(2 * 1000);

	err = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(err, ==, 0);

	f->sockets.server_disconnected = 1;

	munit_assert_ptr_not_equal(strstr(f->conn.error, "no heartbeat since"),
	                           NULL);

	return MUNIT_OK;
}

static MunitTest dqlite__conn_abort_tests[] = {
    {"/immediately",
     test_abort_immediately,
     setup,
     tear_down,
     0,
     test_socket_family_params},
    {"/during-handshake",
     test_abort_during_handshake,
     setup,
     tear_down,
     0,
     test_socket_family_params},
    {"/after-handshake",
     test_abort_after_handshake,
     setup,
     tear_down,
     0,
     test_socket_family_params},
    {"/during-header",
     test_abort_during_header,
     setup,
     tear_down,
     0,
     test_socket_family_params},
    {"/after-header",
     test_abort_after_header,
     setup,
     tear_down,
     0,
     test_socket_family_params},
    {"/during-body",
     test_abort_during_body,
     setup,
     tear_down,
     0,
     test_socket_family_params},
    {"/after-body",
     test_abort_after_body,
     setup,
     tear_down,
     0,
     test_socket_family_params},
    //	{"after heartbeat timeout", test_dqlite__conn_abort_after_heartbeat_timeout},
    {NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL},
};

/******************************************************************************
 *
 * Tests for dqlite__conn_read_cb
 *
 ******************************************************************************/

static MunitResult test_read_cb_unknown_protocol(const MunitParameter params[],
                                                 void *               data) {
	struct fixture *f = data;
	int             err;
	uint64_t        protocol = 0x123456;
	ssize_t         nwrite;

	(void)params;

	err = dqlite__conn_start(&f->conn);
	munit_assert_int(err, ==, 0);

	nwrite = write(f->sockets.client, &protocol, sizeof(protocol));
	munit_assert_int(nwrite, ==, sizeof protocol);

	err = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(err, ==, 0 /* Number of pending handles */);

	f->sockets.server_disconnected = 1;

	munit_assert_string_equal(f->conn.error, "unknown protocol version: 123456");

	return MUNIT_OK;
}

static MunitResult test_read_cb_empty_body(const MunitParameter params[],
                                           void *               data) {
	struct fixture *f = data;
	int             err;
	uint64_t        protocol = dqlite__flip64(DQLITE_PROTOCOL_VERSION);
	uint8_t         buf[8]   = {
            /* Invalid header (empty body) */
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        };
	ssize_t nwrite;

	(void)params;

	err = dqlite__conn_start(&f->conn);
	munit_assert_int(err, ==, 0);

	nwrite = write(f->sockets.client, &protocol, sizeof(protocol));
	munit_assert_int(nwrite, ==, sizeof protocol);

	err = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(err, ==, 1 /* Number of pending handles */);

	nwrite = write(f->sockets.client, buf, 8);
	munit_assert_int(nwrite, ==, 8);

	err = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(err, ==, 1 /* Number of pending handles */);

	__recv_response(f);

	munit_assert_int(f->response.type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response.failure.code, ==, DQLITE_PROTO);
	munit_assert_string_equal(
	    f->response.failure.message,
	    "failed to parse request header: empty message body");

	test_socket_pair_client_disconnect(&f->sockets);

	err = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(err, ==, 0);

	f->sockets.server_disconnected = 1;

	return MUNIT_OK;
}

static MunitResult test_read_cb_body_too_large(const MunitParameter params[],
                                               void *               data) {
	struct fixture *f = data;
	int             err;
	uint64_t        protocol = dqlite__flip64(DQLITE_PROTOCOL_VERSION);
	uint8_t         buf[8]   = {
            /* Invalid header (body too large) */
            0xf,
            0xf,
            0xf,
            0xf,
            0,
            0,
            0,
            0,
        };
	ssize_t nwrite;

	(void)params;

	err = dqlite__conn_start(&f->conn);
	munit_assert_int(err, ==, 0);

	nwrite = write(f->sockets.client, &protocol, sizeof(protocol));
	munit_assert_int(nwrite, ==, sizeof protocol);

	err = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(err, ==, 1 /* Number of pending handles */);

	nwrite = write(f->sockets.client, buf, 8);
	munit_assert_int(nwrite, ==, 8);

	err = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(err, ==, 1 /* Number of pending handles */);

	__recv_response(f);

	munit_assert_int(f->response.type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response.failure.code, ==, DQLITE_PROTO);
	munit_assert_string_equal(
	    f->response.failure.message,
	    "failed to parse request header: message body too large");

	test_socket_pair_client_disconnect(&f->sockets);

	err = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(err, ==, 0);

	f->sockets.server_disconnected = 1;

	return MUNIT_OK;
}

static MunitResult test_read_cb_malformed_body(const MunitParameter params[],
                                               void *               data) {
	struct fixture *f = data;
	int             err;
	uint64_t        protocol = dqlite__flip64(DQLITE_PROTOCOL_VERSION);
	uint8_t         buf[32]  = {
            /* Valid header for Open request, invalid Open.volatile */
            3,   0,   0,   0,   DQLITE_REQUEST_OPEN,
            0,   0,   0,   't', 'e',
            's', 't', '.', 'd', 'b',
            0,   0,   0,   0,   0,
            0,   0,   0,   0,   'v',
            'o', 'l', 'a', 't', 'i',
            'e', 'x',
        };
	ssize_t nwrite;

	(void)params;

	err = dqlite__conn_start(&f->conn);
	munit_assert_int(err, ==, 0);

	nwrite = write(f->sockets.client, &protocol, sizeof(protocol));
	munit_assert_int(nwrite, ==, sizeof(protocol));

	err = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(err, ==, 1 /* Number of pending handles */);

	nwrite = write(f->sockets.client, buf, 32);
	munit_assert_int(nwrite, ==, 32);

	err = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(err, ==, 1 /* Number of pending handles */);

	__recv_response(f);

	munit_assert_int(f->response.type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response.failure.code, ==, DQLITE_PARSE);
	munit_assert_string_equal(
	    f->response.failure.message,
	    "failed to decode request: failed to decode 'open': "
	    "failed to get 'vfs' field: no string found");

	test_socket_pair_client_disconnect(&f->sockets);

	err = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(err, ==, 0);

	f->sockets.server_disconnected = 1;

	return MUNIT_OK;
}

static MunitResult test_read_cb_invalid_db_id(const MunitParameter params[],
                                              void *               data) {
	struct fixture *f = data;
	int             err;
	uint64_t        protocol = dqlite__flip64(DQLITE_PROTOCOL_VERSION);
	uint8_t         buf[24]  = {
            /* Valid header for Prepare request, invalid Prepare.db_id */
            2,   0,   0,   0,   DQLITE_REQUEST_PREPARE,
            0,   0,   0,   1,   0,
            0,   0,   0,   0,   0,
            0,   'S', 'E', 'L', 'E',
            'C', ' ', '1', 0,
        };
	ssize_t nwrite;

	(void)params;

	err = dqlite__conn_start(&f->conn);
	munit_assert_int(err, ==, 0);

	nwrite = write(f->sockets.client, &protocol, sizeof(protocol));
	munit_assert_int(nwrite, ==, sizeof protocol);

	err = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(err, ==, 1 /* Number of pending handles */);

	nwrite = write(f->sockets.client, buf, 24);
	munit_assert_int(nwrite, ==, 24);

	err = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(err, ==, 1 /* Number of pending handles */);

	__recv_response(f);

	munit_assert_int(f->response.type, ==, DQLITE_RESPONSE_FAILURE);
	munit_assert_int(f->response.failure.code, ==, SQLITE_NOTFOUND);
	munit_assert_string_equal(f->response.failure.message, "no db with id 1");

	test_socket_pair_client_disconnect(&f->sockets);

	err = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(err, ==, 0);

	f->sockets.server_disconnected = 1;

	return MUNIT_OK;
}

static MunitTest dqlite__conn_read_cb_tests[] = {
    {"/unknown-protocol",
     test_read_cb_unknown_protocol,
     setup,
     tear_down,
     0,
     test_socket_family_params},
    {"/empty-body",
     test_read_cb_empty_body,
     setup,
     tear_down,
     0,
     test_socket_family_params},
    {"/body-too-large",
     test_read_cb_body_too_large,
     setup,
     tear_down,
     0,
     test_socket_family_params},
    {"/malformed-body",
     test_read_cb_malformed_body,
     setup,
     tear_down,
     0,
     test_socket_family_params},
    {"/invalid-db-id",
     test_read_cb_invalid_db_id,
     setup,
     tear_down,
     0,
     test_socket_family_params},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * Suite
 *
 ******************************************************************************/

MunitSuite dqlite__conn_suites[] = {
    {"_abort", dqlite__conn_abort_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE},
    {"_read_cb", dqlite__conn_read_cb_tests, NULL, 1, MUNIT_SUITE_OPTION_NONE},
    {NULL, NULL, NULL, 0, MUNIT_SUITE_OPTION_NONE},
};
