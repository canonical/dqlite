#include <unistd.h>

#include "../../../src/lib/transport.h"

#include "../../lib/runner.h"
#include "../../lib/endpoint.h"
#include "../../lib/uv.h"

TEST_MODULE(libTransport);

/******************************************************************************
 *
 * Fixture
 *
 ******************************************************************************/

struct fixture
{
	struct testEndpoint endpoint;
	struct uv_loop_s loop;
	struct transport transport;
	int client;
	struct
	{
		bool invoked;
		int status;
	} read;
	struct
	{
		bool invoked;
		int status;
	} write;
};

static void readCb(struct transport *transport, int status)
{
	struct fixture *f = transport->data;
	f->read.invoked = true;
	f->read.status = status;
}

static void write_cb(struct transport *transport, int status)
{
	struct fixture *f = transport->data;
	f->write.invoked = true;
	f->write.status = status;
}

static void *setup(const MunitParameter params[], void *user_data)
{
	struct fixture *f = munit_malloc(sizeof *f);
	struct uv_stream_s *stream;
	int rv;
	int server;
	(void)user_data;
	testEndpoint_setup(&f->endpoint, params);
	rv = listen(f->endpoint.fd, 16);
	munit_assert_int(rv, ==, 0);
	testEndpoint_pair(&f->endpoint, &server, &f->client);
	testUv_setup(params, &f->loop);
	rv = transport__stream(&f->loop, server, &stream);
	munit_assert_int(rv, ==, 0);
	rv = transport_init(&f->transport, stream);
	munit_assert_int(rv, ==, 0);
	f->transport.data = f;
	f->read.invoked = false;
	f->read.status = -1;
	f->write.invoked = false;
	f->write.status = -1;
	return f;
}

static void tear_down(void *data)
{
	struct fixture *f = data;
	int rv;
	rv = close(f->client);
	munit_assert_int(rv, ==, 0);
	transport_close(&f->transport, NULL);
	testUv_stop(&f->loop);
	testUv_tear_down(&f->loop);
	testEndpoint_tear_down(&f->endpoint);
	free(data);
}

/******************************************************************************
 *
 * Helper macros.
 *
 ******************************************************************************/

/* Allocate a libuv buffer with the given amount of bytes. */
#define BUF_ALLOC(N) {munit_malloc(N), N};

/* Start reading into the current buffer */
#define READ(BUF)                                                  \
	{                                                          \
		int rv2;                                           \
		rv2 = transport__read(&f->transport, BUF, readCb); \
		munit_assert_int(rv2, ==, 0);                      \
	}

/* Start writing the current buffer into the stream */
#define WRITE(BUF)                                                    \
	{                                                             \
		int rv2;                                              \
		rv2 = transport__write(&f->transport, BUF, write_cb); \
		munit_assert_int(rv2, ==, 0);                         \
	}

/* Write N bytes into the client buffer. Each byte will contain a progressive
 * number starting from 1. */
#define CLIENT_WRITE(N)                          \
	{                                        \
		uint8_t *buf_ = munit_malloc(N); \
		unsigned i_;                     \
		int rv_;                         \
		for (i_ = 0; i_ < N; i_++) {     \
			buf_[i_] = i_ + 1;       \
		}                                \
		rv_ = write(f->client, buf_, N); \
		munit_assert_int(rv_, ==, N);    \
		free(buf_);                      \
	}

/******************************************************************************
 *
 * Assertions.
 *
 ******************************************************************************/

/* Assert that the read callback was invoked with the given status. */
#define ASSERT_READ(STATUS)                           \
	munit_assert_true(f->read.invoked);           \
	munit_assert_int(f->read.status, ==, STATUS); \
	f->read.invoked = false;                      \
	f->read.status = -1

/* Assert that the write callback was invoked with the given status. */
#define ASSERT_WRITE(STATUS)                           \
	munit_assert_true(f->write.invoked);           \
	munit_assert_int(f->write.status, ==, STATUS); \
	f->write.invoked = false;                      \
	f->write.status = -1

/******************************************************************************
 *
 * transport__read
 *
 ******************************************************************************/

TEST_SUITE(read);
TEST_SETUP(read, setup);
TEST_TEAR_DOWN(read, tear_down);

TEST_CASE(read, success, NULL)
{
	struct fixture *f = data;
	uv_buf_t buf = BUF_ALLOC(2);
	(void)params;
	CLIENT_WRITE(2);
	READ(&buf);
	testUv_run(&f->loop, 1);
	ASSERT_READ(0);
	munit_assert_int(((uint8_t *)buf.base)[0], ==, 1);
	munit_assert_int(((uint8_t *)buf.base)[1], ==, 2);
	free(buf.base);
	return MUNIT_OK;
}

/******************************************************************************
 *
 * transport__write
 *
 ******************************************************************************/

TEST_SUITE(write);
TEST_SETUP(write, setup);
TEST_TEAR_DOWN(write, tear_down);

TEST_CASE(write, success, NULL)
{
	struct fixture *f = data;
	uv_buf_t buf = BUF_ALLOC(2);
	(void)params;
	WRITE(&buf);
	testUv_run(&f->loop, 1);
	ASSERT_WRITE(0);
	free(buf.base);
	return MUNIT_OK;
}
