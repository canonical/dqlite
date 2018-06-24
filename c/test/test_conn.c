#include <assert.h>
#include <unistd.h>

#include <CUnit/CUnit.h>
#include <uv.h>

#include "../src/binary.h"
#include "../src/conn.h"
#include "../include/dqlite.h"

#include "cluster.h"
#include "socket.h"
#include "suite.h"

static struct test_socket_pair sockets;
static uv_loop_t loop;
static struct dqlite__conn conn;
struct dqlite__response response;

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

	dqlite__conn_init(&conn, log, sockets.server, test_cluster(), &loop);
	dqlite__response_init(&response);
}

void test_dqlite__conn_teardown()
{
	int err;

	dqlite__response_close(&response);
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

	err = dqlite__conn_start(&conn);
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
	uint64_t protocol = dqlite__flip64(DQLITE_PROTOCOL_VERSION);
	ssize_t nwrite;

	err = dqlite__conn_start(&conn);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	nwrite = write(sockets.client, &protocol, 3);
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
	uint64_t protocol = dqlite__flip64(DQLITE_PROTOCOL_VERSION);
	ssize_t nwrite;

	err = dqlite__conn_start(&conn);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	nwrite = write(sockets.client, &protocol, sizeof(protocol));
	CU_ASSERT_EQUAL_FATAL(nwrite, sizeof(protocol));

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
	uint64_t protocol = dqlite__flip64(DQLITE_PROTOCOL_VERSION);
	uint8_t buf[7] = { /* Partial header */
		0, 0, 0, 0,
		0, 0, 0
	};
	ssize_t nwrite;

	err = dqlite__conn_start(&conn);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	nwrite = write(sockets.client, &protocol, sizeof(protocol));
	CU_ASSERT_EQUAL_FATAL(nwrite, sizeof(protocol));

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
	uint64_t protocol = dqlite__flip64(DQLITE_PROTOCOL_VERSION);
	uint8_t buf[8] = { /* Full header */
		1, 0, 0, 0,
		0, 0, 0, 0
	};
	ssize_t nwrite;

	err = dqlite__conn_start(&conn);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	nwrite = write(sockets.client, &protocol, sizeof(protocol));
	CU_ASSERT_EQUAL_FATAL(nwrite, sizeof(protocol));

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

void test_dqlite__conn_abort_during_body()
{
	int err;
	uint64_t protocol = dqlite__flip64(DQLITE_PROTOCOL_VERSION);
	uint8_t buf[13] = { /* Header and partial body */
		1, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
		0
	};
	ssize_t nwrite;

	err = dqlite__conn_start(&conn);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	nwrite = write(sockets.client, &protocol, sizeof(protocol));
	CU_ASSERT_EQUAL_FATAL(nwrite, sizeof(protocol));

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

void test_dqlite__conn_abort_after_body()
{
	int err;
	uint64_t protocol = dqlite__flip64(DQLITE_PROTOCOL_VERSION);
	uint8_t buf[16] = { /* Header and body (Leader request) */
		1, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
	};
	ssize_t nwrite;

	err = dqlite__conn_start(&conn);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	nwrite = write(sockets.client, &protocol, sizeof(protocol));
	CU_ASSERT_EQUAL_FATAL(nwrite, sizeof(protocol));

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

void test_dqlite__conn_abort_after_heartbeat_timeout()
{
	int err;
	uint64_t protocol = dqlite__flip64(DQLITE_PROTOCOL_VERSION);
	uint8_t buf[3] = { /* Incomplete header */
		0, 0, 0,
	};
	ssize_t nwrite;

	conn.gateway.heartbeat_timeout = 1; /* Abort after a millisecond */

	err = dqlite__conn_start(&conn);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	nwrite = write(sockets.client, &protocol, sizeof(protocol));
	CU_ASSERT_EQUAL_FATAL(nwrite, sizeof(protocol));

	nwrite = write(sockets.client, buf, 3);
	CU_ASSERT_EQUAL_FATAL(nwrite, 3);

	usleep(2 * 1000);

	err = uv_run(&loop, UV_RUN_NOWAIT);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	sockets.server_disconnected = 1;

	CU_ASSERT_PTR_NOT_NULL(strstr(conn.error, "no heartbeat since"));
}

/*
 * dqlite__conn_read_cb_suite
 */

static void test_dqlite__conn_recv_response() {
	int err;
	uv_buf_t buf;
	ssize_t nread;

	dqlite__message_header_recv_start(&response.message, &buf);

	nread = read(sockets.client, buf.base, buf.len);
	CU_ASSERT_EQUAL_FATAL(nread, buf.len);

	err = dqlite__message_header_recv_done(&response.message);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	err = dqlite__message_body_recv_start(&response.message, &buf);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	nread = read(sockets.client, buf.base, buf.len);
	CU_ASSERT_EQUAL_FATAL(nread, buf.len);

	err = dqlite__response_decode(&response);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	dqlite__message_recv_reset(&response.message);
}

void test_dqlite__conn_read_cb_unknown_protocol()
{
	int err;
	uint64_t protocol = 0x123456;
	ssize_t nwrite;

	err = dqlite__conn_start(&conn);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	nwrite = write(sockets.client, &protocol, sizeof(protocol));
	CU_ASSERT_EQUAL_FATAL(nwrite, sizeof(protocol));

	err = uv_run(&loop, UV_RUN_NOWAIT);
	CU_ASSERT_EQUAL_FATAL(err, 0 /* Number of pending handles */);

	sockets.server_disconnected = 1;

	CU_ASSERT_STRING_EQUAL(conn.error, "unknown protocol version: 123456");
}

void test_dqlite__conn_read_cb_empty_body()
{
	int err;
	uint64_t protocol = dqlite__flip64(DQLITE_PROTOCOL_VERSION);
	uint8_t buf[8] = { /* Invalid header (empty body) */
		0, 0, 0, 0,
		0, 0, 0, 0,
	};
	ssize_t nwrite;

	err = dqlite__conn_start(&conn);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	nwrite = write(sockets.client, &protocol, sizeof(protocol));
	CU_ASSERT_EQUAL_FATAL(nwrite, sizeof(protocol));

	err = uv_run(&loop, UV_RUN_NOWAIT);
	CU_ASSERT_EQUAL_FATAL(err, 1 /* Number of pending handles */);

	nwrite = write(sockets.client, buf, 8);
	CU_ASSERT_EQUAL_FATAL(nwrite, 8);

	err = uv_run(&loop, UV_RUN_NOWAIT);
	CU_ASSERT_EQUAL_FATAL(err, 1 /* Number of pending handles */);

	test_dqlite__conn_recv_response();

	CU_ASSERT_EQUAL(response.type, DQLITE_RESPONSE_FAILURE);
	CU_ASSERT_EQUAL(response.failure.code, DQLITE_PROTO);
	CU_ASSERT_STRING_EQUAL(
		response.failure.description,
		"failed to parse request header: empty message body");

	err = test_socket_pair_client_disconnect(&sockets);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	err = uv_run(&loop, UV_RUN_NOWAIT);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	sockets.server_disconnected = 1;
}

void test_dqlite__conn_read_cb_body_too_large()
{
	int err;
	uint64_t protocol = dqlite__flip64(DQLITE_PROTOCOL_VERSION);
	uint8_t buf[8] = { /* Invalid header (body too large) */
		0xf, 0xf, 0xf, 0xf,
		0, 0, 0, 0,
	};
	ssize_t nwrite;

	err = dqlite__conn_start(&conn);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	nwrite = write(sockets.client, &protocol, sizeof(protocol));
	CU_ASSERT_EQUAL_FATAL(nwrite, sizeof(protocol));

	err = uv_run(&loop, UV_RUN_NOWAIT);
	CU_ASSERT_EQUAL_FATAL(err, 1 /* Number of pending handles */);

	nwrite = write(sockets.client, buf, 8);
	CU_ASSERT_EQUAL_FATAL(nwrite, 8);

	err = uv_run(&loop, UV_RUN_NOWAIT);
	CU_ASSERT_EQUAL_FATAL(err, 1 /* Number of pending handles */);

	test_dqlite__conn_recv_response();

	CU_ASSERT_EQUAL(response.type, DQLITE_RESPONSE_FAILURE);
	CU_ASSERT_EQUAL(response.failure.code, DQLITE_PROTO);
	CU_ASSERT_STRING_EQUAL(
		response.failure.description,
		"failed to parse request header: message body too large");

	err = test_socket_pair_client_disconnect(&sockets);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	err = uv_run(&loop, UV_RUN_NOWAIT);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	sockets.server_disconnected = 1;
}

void test_dqlite__conn_read_cb_malformed_body()
{
	int err;
	uint64_t protocol = dqlite__flip64(DQLITE_PROTOCOL_VERSION);
	uint8_t buf[32] = { /* Valid header for Open request, invalid Open.volatile */
		3, 0, 0, 0, DQLITE_REQUEST_OPEN, 0, 0, 0,
		't', 'e', 's', 't', '.', 'd', 'b', 0,
		0, 0, 0, 0, 0, 0, 0, 0,
		'v', 'o', 'l', 'a', 't', 'i', 'e', 'x',
	};
	ssize_t nwrite;

	err = dqlite__conn_start(&conn);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	nwrite = write(sockets.client, &protocol, sizeof(protocol));
	CU_ASSERT_EQUAL_FATAL(nwrite, sizeof(protocol));

	err = uv_run(&loop, UV_RUN_NOWAIT);
	CU_ASSERT_EQUAL_FATAL(err, 1 /* Number of pending handles */);

	nwrite = write(sockets.client, buf, 32);
	CU_ASSERT_EQUAL_FATAL(nwrite, 32);

	err = uv_run(&loop, UV_RUN_NOWAIT);
	CU_ASSERT_EQUAL_FATAL(err, 1 /* Number of pending handles */);

	test_dqlite__conn_recv_response();

	CU_ASSERT_EQUAL(response.type, DQLITE_RESPONSE_FAILURE);
	CU_ASSERT_EQUAL(response.failure.code, DQLITE_PARSE);
	CU_ASSERT_STRING_EQUAL(
		response.failure.description,
		"failed to decode request: failed to decode 'open': failed to get 'vfs' field: no string found");

	err = test_socket_pair_client_disconnect(&sockets);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	err = uv_run(&loop, UV_RUN_NOWAIT);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	sockets.server_disconnected = 1;
}

void test_dqlite__conn_read_cb_invalid_db_id()
{
	int err;
	uint64_t protocol = dqlite__flip64(DQLITE_PROTOCOL_VERSION);
	uint8_t buf[24] = { /* Valid header for Prepare request, invalid Prepare.db_id */
		2, 0, 0, 0, DQLITE_REQUEST_PREPARE, 0, 0, 0,
		1, 0, 0, 0, 0, 0, 0, 0,
		'S', 'E', 'L', 'E', 'C', ' ', '1', 0,
	};
	ssize_t nwrite;

	err = dqlite__conn_start(&conn);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	nwrite = write(sockets.client, &protocol, sizeof(protocol));
	CU_ASSERT_EQUAL_FATAL(nwrite, sizeof(protocol));

	err = uv_run(&loop, UV_RUN_NOWAIT);
	CU_ASSERT_EQUAL_FATAL(err, 1 /* Number of pending handles */);

	nwrite = write(sockets.client, buf, 24);
	CU_ASSERT_EQUAL_FATAL(nwrite, 24);

	err = uv_run(&loop, UV_RUN_NOWAIT);
	CU_ASSERT_EQUAL_FATAL(err, 1 /* Number of pending handles */);

	test_dqlite__conn_recv_response();

	CU_ASSERT_EQUAL(response.type, DQLITE_RESPONSE_FAILURE);
	CU_ASSERT_EQUAL(response.failure.code, DQLITE_NOTFOUND);
	CU_ASSERT_STRING_EQUAL(
		response.failure.description,
		"failed to handle request: failed to handle prepare: no db with id 1");

	err = test_socket_pair_client_disconnect(&sockets);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	err = uv_run(&loop, UV_RUN_NOWAIT);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	sockets.server_disconnected = 1;
}

/*
 * dqlite__conn_write_suite
 */
