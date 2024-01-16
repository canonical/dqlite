#include <raft.h>
#include <uv.h>

#include <unistd.h>

#include "../../../src/lib/transport.h"
#include "../../lib/endpoint.h"
#include "../../lib/runner.h"
#include "../../lib/uv.h"

TEST_MODULE(ext_uv);

/******************************************************************************
 *
 * Helpers
 *
 ******************************************************************************/

struct fixture
{
	struct uv_loop_s loop;
	struct uv_stream_s *listener;
	struct test_endpoint endpoint;
	int client;
	union {
		uv_tcp_t tcp;
		uv_pipe_t pipe;
		uv_stream_t stream;
	};
};

/* Return a buffer of size TEST_SOCKET_MIN_BUF_SIZE */
static uv_buf_t *buf_malloc(void)
{
	uv_buf_t *buf = munit_malloc(sizeof *buf);
	buf->base = munit_malloc(TEST_SOCKET_MIN_BUF_SIZE);
	buf->len = TEST_SOCKET_MIN_BUF_SIZE;
	return buf;
}

/* Free the buffer returned by buf_malloc() */
static void buf_free(uv_buf_t *buf)
{
	free(buf->base);
	free(buf);
}

/******************************************************************************
 *
 * Parameters
 *
 ******************************************************************************/

/* Run the tests using both TCP and Unix sockets. */
static MunitParameterEnum endpointParams[] = {
    {TEST_ENDPOINT_FAMILY, test_endpoint_family_values},
    {NULL, NULL},
};

/******************************************************************************
 *
 * Setup and tear down
 *
 ******************************************************************************/

static void listenCb(uv_stream_t *listener, int status)
{
	struct fixture *f = listener->data;
	int rv;
	munit_assert_int(status, ==, 0);

	switch (listener->type) {
		case UV_TCP:
			rv = uv_tcp_init(&f->loop, &f->tcp);
			munit_assert_int(rv, ==, 0);
			break;
		case UV_NAMED_PIPE:
			rv = uv_pipe_init(&f->loop, &f->pipe, 0);
			munit_assert_int(rv, ==, 0);
			break;
		default:
			munit_assert(0);
	}

	rv = uv_accept(listener, &f->stream);
	munit_assert_int(rv, ==, 0);
}

static void *setup(const MunitParameter params[], void *user_data)
{
	struct fixture *f = munit_malloc(sizeof *f);
	int rv;
	(void)user_data;

	test_uv_setup(params, &f->loop);
	test_endpoint_setup(&f->endpoint, params);

	rv = transport__stream(&f->loop, f->endpoint.fd, &f->listener);
	munit_assert_int(rv, ==, 0);

	f->listener->data = f;

	rv = uv_listen(f->listener, 128, listenCb);
	munit_assert_int(rv, ==, 0);

	f->client = test_endpoint_connect(&f->endpoint);

	test_uv_run(&f->loop, 1);

	return f;
}

static void tear_down(void *data)
{
	struct fixture *f = data;
	int rv;
	rv = close(f->client);
	munit_assert_int(rv, ==, 0);
	uv_close((struct uv_handle_s *)f->listener, (uv_close_cb)raft_free);
	test_endpoint_tear_down(&f->endpoint);
	uv_close((uv_handle_t *)(&f->stream), NULL);
	test_uv_stop(&f->loop);
	test_uv_tear_down(&f->loop);
	free(f);
}

/******************************************************************************
 *
 * uv_write
 *
 ******************************************************************************/

TEST_SUITE(write);
TEST_SETUP(write, setup);
TEST_TEAR_DOWN(write, tear_down);

/* Writing an amount of data below the buffer size makes that data immediately
 * available for reading. */
TEST_CASE(write, sync, endpointParams)
{
	struct fixture *f = data;
	uv_write_t req;
	uv_buf_t *buf1 = buf_malloc();
	uv_buf_t *buf2 = buf_malloc();
	int rv;

	(void)params;

	rv = uv_write(&req, &f->stream, buf1, 1, NULL);
	munit_assert_int(rv, ==, 0);

	rv = read(f->client, buf2->base, buf2->len);
	munit_assert_int(rv, ==, buf2->len);

	test_uv_run(&f->loop, 1);

	buf_free(buf1);
	buf_free(buf2);

	return MUNIT_OK;
}

/******************************************************************************
 *
 * uv_read
 *
 ******************************************************************************/

TEST_SUITE(read);
TEST_SETUP(read, setup);
TEST_TEAR_DOWN(read, tear_down);

static void test_read_sync__alloc_cb(uv_handle_t *stream,
				     size_t _,
				     uv_buf_t *buf)
{
	(void)stream;
	(void)_;

	buf->len = TEST_SOCKET_MIN_BUF_SIZE;
	buf->base = munit_malloc(TEST_SOCKET_MIN_BUF_SIZE);
}

static void test_read_sync__read_cb(uv_stream_t *stream,
				    ssize_t nread,
				    const uv_buf_t *buf)
{
	bool *read_cb_called;

	/* Apprently there's an empty read before the actual one. */
	if (nread == 0) {
		free(buf->base);
		return;
	}

	munit_assert_int(nread, ==, TEST_SOCKET_MIN_BUF_SIZE);
	munit_assert_int(buf->len, ==, TEST_SOCKET_MIN_BUF_SIZE);

	read_cb_called = stream->data;

	*read_cb_called = true;

	free(buf->base);
}

/* Reading an amount of data below the buffer happens synchronously. */
TEST_CASE(read, sync, endpointParams)
{
	struct fixture *f = data;
	uv_buf_t *buf = buf_malloc();
	int rv;
	bool read_cb_called;

	(void)params;

	f->stream.data = &read_cb_called;

	rv = uv_read_start(&f->stream, test_read_sync__alloc_cb,
			   test_read_sync__read_cb);

	rv = write(f->client, buf->base, buf->len);
	munit_assert_int(rv, ==, buf->len);

	test_uv_run(&f->loop, 1);

	munit_assert_true(read_cb_called);

	buf_free(buf);

	return MUNIT_OK;
}
