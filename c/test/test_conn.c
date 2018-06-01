#include <assert.h>

#include <CUnit/CUnit.h>
#include <capnp_c.h>
#include <uv.h>

#include "../src/conn.h"
#include "../include/dqlite.h"

#include "cluster.h"
#include "socket.h"
#include "suite.h"

static struct test_socket_pair sockets;
static uv_loop_t loop;
static struct dqlite__conn conn;

void test_dqlite__conn_setup()
{
	int err;
	FILE *log = test_suite_dqlite_log();

	err = test_socket_pair_initialize(&sockets);
	if (err != 0)
		CU_FAIL("test setup failed");

	err = uv_loop_init(&loop);
	if (err != 0) {
		test_suite_errorf("failed to init UV loop: %s - %d", uv_strerror(errno), err);
		CU_FAIL("test setup failed");
	}

	dqlite__conn_init(&conn, log, sockets.server, test_cluster());
}

void test_dqlite__conn_teardown()
{
	int err;

	dqlite__conn_close(&conn);

	err = uv_loop_close(&loop);
	if (err != 0) {
		test_suite_errorf("failed to close UV loop: %s - %d", uv_strerror(err), err);
		test_suite_teardown_fail();
		return;
	}

	err = test_socket_pair_cleanup(&sockets);
	if (err != 0) {
		test_suite_teardown_fail();
		return;
	}

	test_suite_teardown_pass();
}

/*
 * dqlite__conn_abort_suite
 */

void test_dqlite__conn_abort_immediately()
{
	int err;

	err = dqlite__conn_read_start(&conn, &loop);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	err = test_socket_pair_client_disconnect(&sockets);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	err = uv_run(&loop, UV_RUN_NOWAIT);
	CU_ASSERT_EQUAL(err, 0);

	sockets.server_disconnected = 1;

	CU_ASSERT_STRING_EQUAL(conn.error, "read error: end of file (EOF)");
}

void test_dqlite__conn_abort_during_handshake()
{
	int err;
	uint32_t version = capn_flip32(DQLITE_PROTOCOL_VERSION);
	ssize_t nwrite;

	err = dqlite__conn_read_start(&conn, &loop);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	nwrite = write(sockets.client, &version, 3);
	CU_ASSERT_EQUAL_FATAL(nwrite, 3);

	err = uv_run(&loop, UV_RUN_NOWAIT);
	CU_ASSERT_EQUAL_FATAL(err, 1 /* Number of pending handles */);

	err = test_socket_pair_client_disconnect(&sockets);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	err = uv_run(&loop, UV_RUN_NOWAIT);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	sockets.server_disconnected = 1;

	CU_ASSERT_STRING_EQUAL(conn.error, "read error: end of file (EOF)");
}

void test_dqlite__conn_abort_after_handshake()
{
	int err;
	uint32_t version = capn_flip32(DQLITE_PROTOCOL_VERSION);
	ssize_t nwrite;

	err = dqlite__conn_read_start(&conn, &loop);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	nwrite = write(sockets.client, &version, 4);
	CU_ASSERT_EQUAL_FATAL(nwrite, 4);

	err = uv_run(&loop, UV_RUN_NOWAIT);
	CU_ASSERT_EQUAL_FATAL(err, 1 /* Number of pending handles */);

	err = test_socket_pair_client_disconnect(&sockets);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	err = uv_run(&loop, UV_RUN_NOWAIT);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	sockets.server_disconnected = 1;

	CU_ASSERT_STRING_EQUAL(conn.error, "read error: end of file (EOF)");
}

void test_dqlite__conn_abort_during_preamble()
{
	int err;
	uint32_t version = capn_flip32(DQLITE_PROTOCOL_VERSION);
	uint8_t buf[4] = {0, 0, 0, 0}; /* Preamble */
	ssize_t nwrite;

	err = dqlite__conn_read_start(&conn, &loop);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	nwrite = write(sockets.client, &version, 4);
	CU_ASSERT_EQUAL_FATAL(nwrite, 4);

	nwrite = write(sockets.client, buf, 2);
	CU_ASSERT_EQUAL_FATAL(nwrite, 2);

	err = uv_run(&loop, UV_RUN_NOWAIT);
	CU_ASSERT_EQUAL_FATAL(err, 1 /* Number of pending handles */);

	err = test_socket_pair_client_disconnect(&sockets);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	err = uv_run(&loop, UV_RUN_NOWAIT);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	sockets.server_disconnected = 1;

	CU_ASSERT_STRING_EQUAL(conn.error, "read error: end of file (EOF)");
}

void test_dqlite__conn_abort_after_preamble()
{
	int err;
	uint32_t version = capn_flip32(DQLITE_PROTOCOL_VERSION);
	uint8_t buf[4] = {0, 0, 0, 0}; /* Preamble */
	ssize_t nwrite;

	err = dqlite__conn_read_start(&conn, &loop);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	nwrite = write(sockets.client, &version, 4);
	CU_ASSERT_EQUAL_FATAL(nwrite, 4);

	nwrite = write(sockets.client, buf, 4);
	CU_ASSERT_EQUAL_FATAL(nwrite, 4);

	err = uv_run(&loop, UV_RUN_NOWAIT);
	CU_ASSERT_EQUAL_FATAL(err, 1 /* Number of pending handles */);

	err = test_socket_pair_client_disconnect(&sockets);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	err = uv_run(&loop, UV_RUN_NOWAIT);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	sockets.server_disconnected = 1;

	CU_ASSERT_STRING_EQUAL(conn.error, "read error: end of file (EOF)");
}

void test_dqlite__conn_abort_during_header()
{
	int err;
	uint32_t version = capn_flip32(DQLITE_PROTOCOL_VERSION);
	uint8_t buf[8] = { /* Preamble and header */
		0, 0, 0, 0,
		8, 0, 0, 0
	};
	ssize_t nwrite;

	err = dqlite__conn_read_start(&conn, &loop);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	nwrite = write(sockets.client, &version, 4);
	CU_ASSERT_EQUAL_FATAL(nwrite, 4);

	nwrite = write(sockets.client, buf, 7);
	CU_ASSERT_EQUAL_FATAL(nwrite, 7);

	err = uv_run(&loop, UV_RUN_NOWAIT);
	CU_ASSERT_EQUAL_FATAL(err, 1 /* Number of pending handles */);

	err = test_socket_pair_client_disconnect(&sockets);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	err = uv_run(&loop, UV_RUN_NOWAIT);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	sockets.server_disconnected = 1;

	CU_ASSERT_STRING_EQUAL(conn.error, "read error: end of file (EOF)");
}

void test_dqlite__conn_abort_after_header()
{
	int err;
	uint32_t version = capn_flip32(DQLITE_PROTOCOL_VERSION);
	uint8_t buf[8] = { /* Preamble and header */
		0, 0, 0, 0,
		8, 0, 0, 0
	};
	ssize_t nwrite;

	err = dqlite__conn_read_start(&conn, &loop);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	nwrite = write(sockets.client, &version, 4);
	CU_ASSERT_EQUAL_FATAL(nwrite, 4);

	nwrite = write(sockets.client, buf, 8);
	CU_ASSERT_EQUAL_FATAL(nwrite, 8);

	err = uv_run(&loop, UV_RUN_NOWAIT);
	CU_ASSERT_EQUAL_FATAL(err, 1 /* Number of pending handles */);

	err = test_socket_pair_client_disconnect(&sockets);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	err = uv_run(&loop, UV_RUN_NOWAIT);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	sockets.server_disconnected = 1;

	CU_ASSERT_STRING_EQUAL(conn.error, "read error: end of file (EOF)");
}

void test_dqlite__conn_abort_during_data()
{
	int err;
	uint32_t version = capn_flip32(DQLITE_PROTOCOL_VERSION);
	uint8_t buf[16] = { /* Preamble, header and data */
		0, 0, 0, 0,
		1, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
	};
	ssize_t nwrite;

	err = dqlite__conn_read_start(&conn, &loop);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	nwrite = write(sockets.client, &version, 4);
	CU_ASSERT_EQUAL_FATAL(nwrite, 4);

	nwrite = write(sockets.client, buf, 13);
	CU_ASSERT_EQUAL_FATAL(nwrite, 13);

	err = uv_run(&loop, UV_RUN_NOWAIT);
	CU_ASSERT_EQUAL_FATAL(err, 1 /* Number of pending handles */);

	err = test_socket_pair_client_disconnect(&sockets);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	err = uv_run(&loop, UV_RUN_NOWAIT);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	sockets.server_disconnected = 1;

	CU_ASSERT_STRING_EQUAL(conn.error, "read error: end of file (EOF)");
}

void test_dqlite__conn_abort_after_data()
{
	int err;
	uint32_t version = capn_flip32(DQLITE_PROTOCOL_VERSION);
	uint8_t buf[16] = { /* Preamble, header and data */
		0, 0, 0, 0,
		1, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
	};
	ssize_t nwrite;

	err = dqlite__conn_read_start(&conn, &loop);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	nwrite = write(sockets.client, &version, 4);
	CU_ASSERT_EQUAL_FATAL(nwrite, 4);

	nwrite = write(sockets.client, buf, 16);
	CU_ASSERT_EQUAL_FATAL(nwrite, 16);

	err = uv_run(&loop, UV_RUN_NOWAIT);
	CU_ASSERT_EQUAL_FATAL(err, 1 /* Number of pending handles */);

	err = test_socket_pair_client_disconnect(&sockets);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	err = uv_run(&loop, UV_RUN_NOWAIT);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	sockets.server_disconnected = 1;

	CU_ASSERT_STRING_EQUAL(conn.error, "read error: connection reset by peer (ECONNRESET)");
}

/*
 * dqlite__conn_read_cb_suite
 */

void test_dqlite__conn_read_cb_unknown_protocol()
{
	int err;
	uint32_t version = 123456;
	ssize_t nwrite;

	err = dqlite__conn_read_start(&conn, &loop);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	nwrite = write(sockets.client, &version, 4);
	CU_ASSERT_EQUAL_FATAL(nwrite, 4);

	err = uv_run(&loop, UV_RUN_NOWAIT);
	CU_ASSERT_EQUAL_FATAL(err, 0 /* Number of pending handles */);

	sockets.server_disconnected = 1;

	CU_ASSERT_STRING_EQUAL(conn.error, "unknown protocol version: 123456");
}

void test_dqlite__conn_read_cb_invalid_preamble()
{
	int err;
	uint32_t version = capn_flip32(DQLITE_PROTOCOL_VERSION);
	uint8_t buf[32] = { /* Invalid preamble, header and Leader request */
		1, 0, 0, 0,
		3, 0, 0, 0,
		0, 0, 0, 0,
		1, 0, 1, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
	};
	ssize_t nwrite;

	err = dqlite__conn_read_start(&conn, &loop);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	nwrite = write(sockets.client, &version, 4);
	CU_ASSERT_EQUAL_FATAL(nwrite, 4);

	nwrite = write(sockets.client, buf, 32);
	CU_ASSERT_EQUAL_FATAL(nwrite, 32);

	err = uv_run(&loop, UV_RUN_NOWAIT);

	/* The connection gets closed by the server */
	CU_ASSERT_EQUAL_FATAL(err, 0 /* Number of pending handles */);

	sockets.server_disconnected = 1;

	CU_ASSERT_STRING_EQUAL(conn.error, "failed to parse request preamble: too many segments: 2");
}

void test_dqlite__conn_read_cb_invalid_header()
{
	int err;
	uint32_t version = capn_flip32(DQLITE_PROTOCOL_VERSION);
	uint8_t buf[32] = { /* Preamble, invalid header (too big) and Leader request */
		0, 0, 0, 0,
		0, 0, 0, 1,
		0, 0, 0, 0,
		1, 0, 1, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
	};
	ssize_t nwrite;

	err = dqlite__conn_read_start(&conn, &loop);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	nwrite = write(sockets.client, &version, 4);
	CU_ASSERT_EQUAL_FATAL(nwrite, 4);

	nwrite = write(sockets.client, buf, 32);
	CU_ASSERT_EQUAL_FATAL(nwrite, 32);

	err = uv_run(&loop, UV_RUN_NOWAIT);

	/* The connection gets closed by the server */
	CU_ASSERT_EQUAL_FATAL(err, 0 /* Number of pending handles */);

	sockets.server_disconnected = 1;

	CU_ASSERT_STRING_EQUAL(conn.error, "failed to parse request header: invalid segment size: 16777216");
}

void test_dqlite__conn_read_cb_unexpected_request()
{
	int err;
	uint32_t version = capn_flip32(DQLITE_PROTOCOL_VERSION);
	uint8_t buf[32] = { /* Preamble, header and unexpected request (Heartbeat) */
		0, 0, 0, 0,
		3, 0, 0, 0,
		0, 0, 0, 0,
		1, 0, 1, 0,
		1, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
	};
	ssize_t nwrite;

	err = dqlite__conn_read_start(&conn, &loop);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	nwrite = write(sockets.client, &version, 4);
	CU_ASSERT_EQUAL_FATAL(nwrite, 4);

	nwrite = write(sockets.client, buf, 32);
	CU_ASSERT_EQUAL_FATAL(nwrite, 32);

	err = uv_run(&loop, UV_RUN_NOWAIT);

	/* The connection gets closed by the server */
	CU_ASSERT_EQUAL_FATAL(err, 0 /* Number of pending handles */);

	sockets.server_disconnected = 1;

	CU_ASSERT_STRING_EQUAL(conn.error, "failed to handle request: expected Leader, got Heartbeat");
}

/*
 * dqlite__conn_write_suite
 */
