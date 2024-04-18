#include <pthread.h>
#include <sys/socket.h>
#include <unistd.h>
#include "../lib/client_protocol.h"
#include "../lib/fs.h"
#include "../lib/heap.h"
#include "../lib/runner.h"
#include "../lib/server.h"
#include "../lib/sqlite.h"

#include "include/dqlite.h"
#include "src/client.h"
#include "src/client/protocol.h"
#include "src/lib/serialize.h"
#include "src/message.h"
#include "src/protocol.h"
#include "src/request.h"
#include "src/response.h"
#include "src/server.h"
#include "src/tracing.h"
#include "src/transport.h"

TEST_MODULE(client);
TEST_SUITE(client);

#define N_SERVERS 3

/* Allocate the payload buffer, encode a request of the given lower case name
 * and initialize the fixture cursor. */
#define ENCODE(REQUEST, LOWER)                                  \
	{                                                       \
		size_t n2 = request_##LOWER##__sizeof(REQUEST); \
		char *cursor;                                   \
		buffer__reset(f->buf1);                         \
		cursor = buffer__advance(f->buf1, n2);          \
		munit_assert_ptr_not_null(cursor);              \
		request_##LOWER##__encode(REQUEST, &cursor);    \
	}

/* Decode a response of the given lower/upper case name using the buffer that
 * was written by the gateway. */
#define DECODE(RESPONSE, LOWER)                                        \
	{                                                              \
		int rc2;                                               \
		rc2 = response_##LOWER##__decode(f->cursor, RESPONSE); \
		munit_assert_int(rc2, ==, 0);                          \
	}

struct fixture {
	char *dirs[N_SERVERS];
	dqlite_server *servers[N_SERVERS];
	int socket_fd[2];
};

int connect_to_fake_server(void *arg, const char *addr, int *fd)
{
	struct fixture *f = (struct fixture *)arg;
	(void)addr;

	*fd = dup(f->socket_fd[1]);
	if (*fd == -1) {
		return 1;
	}
	tracef("CONNECT TO FAKE SERVER");

	return 0;
}

static void start_each_server(struct fixture *f)
{
	const char *addrs[] = { "127.0.0.1:8880", "127.0.0.1:8881" };
	int rv;

	rv = dqlite_server_set_address(f->servers[0], "127.0.0.1:8880");
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_set_auto_bootstrap(f->servers[0], true);
	munit_assert_int(rv, ==, 0);
	f->servers[0]->refresh_period = 100;
	rv = dqlite_server_start(f->servers[0]);
	munit_assert_int(rv, ==, 0);

	rv = dqlite_server_set_address(f->servers[1], "127.0.0.1:8881");
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_set_auto_join(f->servers[1], addrs, 1);
	munit_assert_int(rv, ==, 0);
	f->servers[1]->refresh_period = 100;
	rv = dqlite_server_start(f->servers[1]);
	munit_assert_int(rv, ==, 0);

	rv = dqlite_server_set_address(f->servers[2], "127.0.0.1:8882");
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_set_auto_join(f->servers[2], addrs, 2);
	munit_assert_int(rv, ==, 0);
	f->servers[2]->refresh_period = 100;
	rv = dqlite_server_start(f->servers[2]);
	munit_assert_int(rv, ==, 0);
}

static void stop_each_server(struct fixture *f)
{
	int rv;

	rv = dqlite_server_stop(f->servers[2]);
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_stop(f->servers[1]);
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_stop(f->servers[0]);
	munit_assert_int(rv, ==, 0);
}

TEST_SETUP(client)
{
	(void)params;
	(void)user_data;
	struct fixture *f = munit_malloc(sizeof *f);
	unsigned i;
	int rv;

	rv = socketpair(AF_UNIX, SOCK_STREAM, 0, f->socket_fd);
	munit_assert_int(rv, ==, 0);

	for (i = 0; i < N_SERVERS; i += 1) {
		f->dirs[i] = test_dir_setup();
		rv = dqlite_server_create(f->dirs[i], &f->servers[i]);
		munit_assert_int(rv, ==, 0);
	}
	start_each_server(f);

	return f;
}

TEST_TEAR_DOWN(client)
{
	struct fixture *f = data;
	unsigned i;

	stop_each_server(f);
	for (i = 0; i < N_SERVERS; i += 1) {
		dqlite_server_destroy(f->servers[i]);
		test_dir_tear_down(f->dirs[i]);
	}

	close(f->socket_fd[0]);
	close(f->socket_fd[1]);
	free(f);
}

void *prepare_reconnect_thread(void *arg)
{
	int rv;
	int fd = *(int *)arg;
	char buf[4096];
	size_t buf_cap = 4096;

	tracef("attempting read");
	rv = read(fd, buf, buf_cap);
	tracef("read %d bytes", rv);
	munit_assert_int(rv, >, 0);

	tracef("attempting decode");
	struct request_leader request = { 0 };
	struct cursor cursor = { buf, buf_cap };
	rv = request_leader__decode(&cursor, &request);
	munit_assert_int(rv, ==, 0);

	// Make this a new pointer because we dont want the response to be
	// changing the offsets.
	struct response_server response = { 0 };
	char *response_cursor = buf;
	response.id = 1;
	response.address = "127.0.0.1:8880";
	struct message message = { 0 };
	message.words = response_server__sizeof(&response);
	message.type = DQLITE_RESPONSE_SERVER;
	message.schema = 1;
	message__encode(&message, &response_cursor);
	tracef("attempting write message");
	rv = write(fd, &message, message__sizeof(&message));
	tracef("wrote %d bytes", rv);
	munit_assert_int(rv, >, 0);

	response_server__encode(&response, &response_cursor);
	tracef("attempting write response");
	rv = write(fd, buf, response_cursor - buf);
	tracef("wrote %d bytes", rv);
	munit_assert_int(rv, >, 0);

	return NULL;
}

TEST_CASE(client, prepare_reconnect, NULL)
{
	dqlite *db;
	dqlite_stmt *stmt;
	int rv;
	struct fixture *f = data;
	(void)params;

	alarm(5);
	rv = dqlite_open(f->servers[0], "test", &db, 0);
	munit_assert_int(rv, ==, SQLITE_OK);

	// Set up the fake connections.
	db->server->connect = connect_to_fake_server;
	db->server->connect_arg = f;

	pthread_t thread;
	pthread_create(&thread, NULL, prepare_reconnect_thread,
		       &f->socket_fd[0]);
	// Regular statement.
	rv = dqlite_prepare(
	    db, "CREATE TABLE pairs (k TEXT, v INTEGER, f FLOAT, b BLOB)", -1,
	    &stmt, NULL);
	munit_assert_int(rv, ==, SQLITE_OK);
	rv = dqlite_finalize(stmt);
	munit_assert_int(rv, ==, SQLITE_OK);

	rv = dqlite_close(db);
	munit_assert_int(rv, ==, SQLITE_OK);

	return MUNIT_OK;
}
