#include <uv.h>

#include "case.h"
#include "socket.h"
#include <unistd.h>

/******************************************************************************
 *
 * Helpers
 *
 ******************************************************************************/

struct fixture {
	uv_loop_t               loop;
	struct test_socket_pair sockets;
	union {
		uv_tcp_t    tcp;
		uv_pipe_t   pipe;
		uv_stream_t stream;
	};
};

/* Return a buffer of size TEST_SOCKET_MIN_BUF_SIZE */
static uv_buf_t *__buf()
{
	uv_buf_t *buf = munit_malloc(sizeof *buf);

	buf->base = munit_malloc(TEST_SOCKET_MIN_BUF_SIZE);
	buf->len  = TEST_SOCKET_MIN_BUF_SIZE;

	return buf;
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
	int             rv;

	test_case_setup(params, user_data);

	rv = uv_loop_init(&f->loop);
	munit_assert_int(rv, ==, 0);

	test_socket_pair_setup(params, &f->sockets);

	switch (uv_guess_handle(f->sockets.server)) {

	case UV_TCP:
		rv = uv_tcp_init(&f->loop, &f->tcp);
		munit_assert_int(rv, ==, 0);

		rv = uv_tcp_open(&f->tcp, f->sockets.server);
		munit_assert_int(rv, ==, 0);

		break;

	case UV_NAMED_PIPE:
		rv = uv_pipe_init(&f->loop, &f->pipe, 0);
		munit_assert_int(rv, ==, 0);

		rv = uv_pipe_open(&f->pipe, f->sockets.server);
		munit_assert_int(rv, ==, 0);

		break;

	default:
		munit_error("unexpected handle type");
	}

	f->stream.data = NULL;

	return f;
}

static void tear_down(void *data)
{
	struct fixture *f = data;
	int             rv;

	test_socket_pair_tear_down(&f->sockets);

	uv_close((uv_handle_t *)(&f->stream), NULL);

	/* We need to run a loop iteraction in order for the handle to actually
	 * be removed from the loop. */
	rv = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(rv, ==, 0);

	rv = uv_loop_close(&f->loop);
	munit_assert_int(rv, ==, 0);

	test_case_tear_down(data);
}

/******************************************************************************
 *
 * uv_write
 *
 ******************************************************************************/

/* Writing an amount of data below the buffer size makes that data immediately
 * available for reading. */
static MunitResult test_write_sync(const MunitParameter params[], void *data)
{
	struct fixture *f = data;
	uv_write_t      req;
	uv_buf_t *      buf1 = __buf();
	uv_buf_t *      buf2 = __buf();
	int             rv;

	(void)params;

	rv = uv_write(&req, &f->stream, buf1, 1, NULL);
	munit_assert_int(rv, ==, 0);

	rv = read(f->sockets.client, buf2->base, buf2->len);
	munit_assert_int(rv, ==, buf2->len);

	rv = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(rv, ==, 0);

	return MUNIT_OK;
}

static MunitTest dqlite__uv_write_tests[] = {
    {"/sync", test_write_sync, setup, tear_down, 0, params},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * uv_write
 *
 ******************************************************************************/

static void test_read_sync__alloc_cb(uv_handle_t *stream,
                                     size_t       _,
                                     uv_buf_t *   buf)
{
	(void)stream;
	(void)_;

	*buf = *__buf();
}

static void test_read_sync__read_cb(uv_stream_t *   stream,
                                    ssize_t         nread,
                                    const uv_buf_t *buf)
{
	bool *read_cb_called;

	/* Apprently there's an empty read before the actual one. */
	if (nread == 0) {
		return;
	}

	munit_assert_int(nread, ==, TEST_SOCKET_MIN_BUF_SIZE);
	munit_assert_int(buf->len, ==, TEST_SOCKET_MIN_BUF_SIZE);

	read_cb_called = stream->data;

	*read_cb_called = true;
}

/* Reading an amount of data below the buffer happens synchronously. */
static MunitResult test_read_sync(const MunitParameter params[], void *data)
{
	struct fixture *f   = data;
	uv_buf_t *      buf = __buf();
	int             rv;
	bool            read_cb_called;

	(void)params;

	f->stream.data = &read_cb_called;

	rv = uv_read_start(
	    &f->stream, test_read_sync__alloc_cb, test_read_sync__read_cb);

	rv = write(f->sockets.client, buf->base, buf->len);
	munit_assert_int(rv, ==, buf->len);

	rv = uv_run(&f->loop, UV_RUN_NOWAIT);
	munit_assert_int(rv, ==, 1);

	munit_assert_true(read_cb_called);

	return MUNIT_OK;
}

static MunitTest dqlite__uv_read_tests[] = {
    {"/sync", test_read_sync, setup, tear_down, 0, params},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * Test suite
 *
 ******************************************************************************/

MunitSuite dqlite__uv_suites[] = {
    {"_write", dqlite__uv_write_tests, NULL, 1, 0},
    {"_read", dqlite__uv_read_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
