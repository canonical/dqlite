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

struct fixture {
	char *dirs[N_SERVERS];
	dqlite_server *servers[N_SERVERS];
	int socket_fd[2];
};

int connect_to_mock_server(void *arg, const char *addr, int *fd)
{
	struct fixture *f = (struct fixture *)arg;
	(void)addr;

	*fd = dup(f->socket_fd[1]);
	if (*fd == -1) {
		return 1;
	}
	tracef("Grab connection to mock server");

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

/* Reads from `fd` and decodes the request into `REQUEST`. */
#define READ_DECODE(REQUEST, LOWER, UPPER)                         \
	{                                                          \
		tracef("attempting read");                         \
		rv = read(fd, buf, buf_cap);                       \
		tracef("read %d bytes", rv);                       \
		munit_assert_int(rv, >, 0);                        \
                                                                   \
		tracef("attempting decode " #UPPER);               \
		struct cursor cursor = { buf, buf_cap };           \
		rv = request_##LOWER##__decode(&cursor, &REQUEST); \
		munit_assert_int(rv, ==, 0);                       \
	}

/* Encodes `RESPONSE` and then writes it to `fd`. */
#define ENCODE_WRITE(RESPONSE, LOWER, UPPER)                                 \
	{                                                                    \
		/* Make this a new pointer because we dont want the response \
		   to be changing the offsets. */                            \
		struct message message = { 0 };                              \
		message.words = response_##LOWER##__sizeof(&response) / 8;   \
		message.type = DQLITE_RESPONSE_##UPPER;                      \
		message.schema = 1;                                          \
		response_cursor = buf;                                       \
		message__encode(&message, &response_cursor);                 \
		tracef("attempting write message");                          \
		rv = write(fd, &message, message__sizeof(&message));         \
		tracef("wrote %d bytes", rv);                                \
		munit_assert_int(rv, >, 0);                                  \
                                                                             \
		tracef("attempting encode " #UPPER);                         \
		response_cursor = buf;                                       \
		response_##LOWER##__encode(&response, &response_cursor);     \
		tracef("attempting write response");                         \
		rv = write(fd, buf, response_cursor - buf);                  \
		tracef("wrote %d bytes", rv);                                \
		munit_assert_int(rv, >, 0);                                  \
	}

void *prepare_reconnect_thread(void *arg)
{
	int rv;
	char *response_cursor;
	char buf[4096];

	size_t buf_cap = 4096;
	int fd = *(int *)arg;
	int db_id = 1;

	{
		struct request_leader request = { 0 };
		READ_DECODE(request, leader, LEADER);
	}
	{
		struct response_server response = { 0 };
		response.id = 1;
		response.address = "127.0.0.1:8880";
		ENCODE_WRITE(response, server, SERVER);
	}
	{
		struct request_open request = { 0 };
		READ_DECODE(request, open, OPEN);
	}
	{
		struct response_failure response = { 0 };
		response.code = 1;
		response.message = "Not leader anymore";
		ENCODE_WRITE(response, failure, FAILURE);
	}

	{
		struct request_leader request = { 0 };
		READ_DECODE(request, leader, LEADER);
	}
	{
		struct response_server response = { 0 };
		response.id = 1;
		response.address = "127.0.0.1:8880";
		ENCODE_WRITE(response, server, SERVER);
	}
	{
		struct request_open request = { 0 };
		READ_DECODE(request, open, OPEN);
	}
	{
		struct response_db response = { 0 };
		response.id = db_id;
		ENCODE_WRITE(response, db, DB);
	}
	{
		struct request_prepare request = { 0 };
		READ_DECODE(request, prepare, PREPARE);
	}
	{
		struct response_stmt_with_offset response = { 0 };
		response.db_id = db_id;
		response.id = 2;
		response.offset = 8;
		ENCODE_WRITE(response, stmt_with_offset, STMT_WITH_OFFSET);
	}
	{
		struct request_finalize request = { 0 };
		READ_DECODE(request, finalize, FINALIZE);
	}
	{
		struct response_empty response = { 0 };
		ENCODE_WRITE(response, empty, EMPTY);
	}

	return NULL;
}

TEST_CASE(client, prepare_reconnect, NULL)
{
	dqlite *db;
	dqlite_stmt *stmt;
	int rv;
	struct fixture *f = data;
	(void)params;

	/* Alarm in case the test hangs waiting for a read or write. */
	alarm(2);

	rv = dqlite_open(f->servers[0], "test", &db, 0);
	munit_assert_int(rv, ==, SQLITE_OK);

	/* Set up the fake connections. We only want to fake the "client"
	 * connections, `db->server->proto->connect` will continue being the
	 * default connect. That way we do not have to fake the Raft traffic
	 * happening in the background. */
	db->server->connect = connect_to_mock_server;
	db->server->connect_arg = f;
	pthread_t thread;
	pthread_create(&thread, NULL, prepare_reconnect_thread,
		       &f->socket_fd[0]);

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
