#include "../lib/cluster.h"
#include "../lib/runner.h"

#include "../../src/gateway.h"
#include "../../src/request.h"
#include "../../src/response.h"

TEST_MODULE(gateway);

/******************************************************************************
 *
 * Fixture.
 *
 ******************************************************************************/

#define FIXTURE                              \
	FIXTURE_CLUSTER;                     \
	struct gateway gateway;              \
	void *payload; /* Request payload */ \
	struct cursor cursor;                \
	struct buffer buffer;                \
	struct handle req;                   \
	struct context context;

#define SETUP                                                             \
	int rc;                                                           \
	SETUP_CLUSTER;                                                    \
	gateway__init(&f->gateway, CLUSTER_LOGGER(0), CLUSTER_OPTIONS(0), \
		      CLUSTER_REGISTRY(0), CLUSTER_RAFT(0));              \
	rc = buffer__init(&f->buffer);                                    \
	munit_assert_int(rc, ==, 0);                                      \
	f->payload = NULL;                                                \
	f->req.data = &f->context;                                        \
	f->context.invoked = false;                                       \
	f->context.status = -1;                                           \
	f->context.type = -1;

#define TEAR_DOWN                    \
	if (f->payload != NULL) {    \
		free(f->payload);    \
	}                            \
	buffer__close(&f->buffer);   \
	gateway__close(&f->gateway); \
	TEAR_DOWN_CLUSTER;

/* Context for the fixture's handle request */
struct context
{
	bool invoked;
	int status;
	int type;
};

static void fixture_handle_cb(struct handle *req, int status, int type)
{
	struct context *c = req->data;
	c->invoked = true;
	c->status = status;
	c->type = type;
}

/******************************************************************************
 *
 * Helper macros.
 *
 ******************************************************************************/

/* Allocate the payload buffer, encode a request of the given lower case name
 * and initialize the fixture cursor. */
#define ENCODE(LOWER)                                               \
	{                                                           \
		size_t n2 = request_##LOWER##__sizeof(&f->request); \
		void *cursor;                                       \
		f->payload = munit_malloc(n2);                      \
		cursor = f->payload;                                \
		request_##LOWER##__encode(&f->request, &cursor);    \
		f->cursor.p = f->payload;                           \
		f->cursor.cap = n2;                                 \
	}

/* Decode a response of the given lower/upper case name using the buffer that
 * was written by the gateway. */
#define DECODE(LOWER, UPPER)                                       \
	{                                                          \
		struct cursor cursor;                              \
		cursor.p = buffer__cursor(&f->buffer, 0);          \
		cursor.cap = buffer__offset(&f->buffer);           \
		munit_assert_int(f->context.type, ==,              \
				 DQLITE_RESPONSE_##UPPER);         \
		response_##LOWER##__decode(&cursor, &f->response); \
	}

/* Handle a request of the given type and check that no error occurs. */
#define HANDLE(TYPE)                                                     \
	{                                                                \
		int rc2;                                                 \
		rc2 = gateway__handle(&f->gateway, &f->req,              \
				      DQLITE_REQUEST_##TYPE, &f->cursor, \
				      &f->buffer, fixture_handle_cb);    \
		munit_assert_int(rc2, ==, 0);                            \
	}

/******************************************************************************
 *
 * Assertions.
 *
 ******************************************************************************/

/* Assert that the handle callback has been invoked with the given status code
 */
#define ASSERT_STATUS(STATUS)                  \
	munit_assert_true(f->context.invoked); \
	munit_assert_int(f->context.status, ==, STATUS)

/******************************************************************************
 *
 * leader
 *
 ******************************************************************************/

struct leader_fixture
{
	FIXTURE;
	struct request_leader request;
	struct response_server response;
};

TEST_SUITE(leader);
TEST_SETUP(leader)
{
	struct leader_fixture *f = munit_malloc(sizeof *f);
	SETUP;
	return f;
}
TEST_TEAR_DOWN(leader)
{
	struct leader_fixture *f = data;
	TEAR_DOWN;
	free(f);
}

/* If the leader is not available, an empty string is returned. */
TEST_CASE(leader, not_available, NULL)
{
	struct leader_fixture *f = data;
	(void)params;
	ENCODE(leader);
	HANDLE(LEADER);
	ASSERT_STATUS(0);
	DECODE(server, SERVER);
	munit_assert_string_equal(f->response.address, "");
	return MUNIT_OK;
}

/* The leader is the same node serving the request. */
TEST_CASE(leader, same_node, NULL)
{
	struct leader_fixture *f = data;
	(void)params;
	CLUSTER_ELECT(0);
	ENCODE(leader);
	HANDLE(LEADER);
	ASSERT_STATUS(0);
	DECODE(server, SERVER);
	munit_assert_string_equal(f->response.address, "1");
	return MUNIT_OK;
}

/* The leader is a different node than the one serving the request. */
TEST_CASE(leader, other_node, NULL)
{
	struct leader_fixture *f = data;
	(void)params;
	CLUSTER_ELECT(1);
	ENCODE(leader);
	HANDLE(LEADER);
	ASSERT_STATUS(0);
	DECODE(server, SERVER);
	munit_assert_string_equal(f->response.address, "2");
	return MUNIT_OK;
}

/******************************************************************************
 *
 * open
 *
 ******************************************************************************/

struct open_fixture
{
	FIXTURE;
	struct request_open request;
	struct response_db response;
};

TEST_SUITE(open);
TEST_SETUP(open)
{
	struct open_fixture *f = munit_malloc(sizeof *f);
	SETUP;
	return f;
}
TEST_TEAR_DOWN(open)
{
	struct open_fixture *f = data;
	TEAR_DOWN;
	free(f);
}

/* Successfully open a database connection. */
TEST_CASE(open, success, NULL)
{
	struct open_fixture *f = data;
	(void)params;
	f->request.filename = "test";
	f->request.vfs = "";
	ENCODE(open);
	HANDLE(OPEN);
	ASSERT_STATUS(0);
	DECODE(db, DB);
	munit_assert_int(f->response.id, ==, 0);
	return MUNIT_OK;
}
