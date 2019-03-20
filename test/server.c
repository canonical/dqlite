#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <sqlite3.h>

#include "../include/dqlite.h"

#include "client.h"
#include "cluster.h"
#include "log.h"
#include "munit.h"
#include "replication.h"
#include "server.h"
#include "fs.h"

dqlite_logger *logger;

static struct test_server *test_server__create(const MunitParameter params[])
{
	int err = 0;
	struct test_server *s;
	uint32_t checkpoint_threshold = 100;
	uint8_t metrics = 1;
	const char *name;

	(void)params;

	s = munit_malloc(sizeof *s);

	logger = test_logger();

#ifdef DQLITE_EXPERIMENTAL
	s->dir = test_dir_setup();
#else
	s->replication = test_replication();

	err = sqlite3_wal_replication_register(s->replication, 0);
	if (err != 0) {
		munit_errorf("failed to register wal replication: %d", err);
	}

	s->vfs = dqlite_vfs_create(s->replication->zName, logger);
	if (s->vfs == NULL) {
		munit_error("failed to create volatile VFS: out of memory");
	}

	sqlite3_vfs_register(s->vfs, 0);
#endif /* DQLITE_EXPERIMENTAL */

#ifdef DQLITE_EXPERIMENTAL
	err = dqlite_server_create2(s->dir, 1, "1", &s->service);
	dqlite_server_bootstrap(s->service);
#else
	err = dqlite_server_create(test_cluster(), &s->service);
#endif /* DQLITE_EXPERIMENTAL */
	if (err != 0) {
		munit_errorf("failed to create dqlite server: %d", err);
	}

	err = dqlite_server_config(s->service, DQLITE_CONFIG_LOGGER, logger);
	if (err != 0) {
		munit_errorf("failed to set logger: %d", err);
	}

	err =
	    dqlite_server_config(s->service, DQLITE_CONFIG_CHECKPOINT_THRESHOLD,
				 (void *)(&checkpoint_threshold));
	if (err != 0) {
		munit_errorf("failed to set checkpoint threshold: %d", err);
	}

#ifdef DQLITE_EXPERIMENTAL
	name = "dqlite-1";
#else
	munit_assert_string_equal(s->vfs->zName, s->replication->zName);
	name = s->vfs->zName;
#endif /* DQLITE_EXPERIMENTAL */
	err = dqlite_server_config(s->service, DQLITE_CONFIG_VFS, (void *)name);
	if (err != 0) {
		munit_errorf("failed to set VFS name: %d", err);
	}

	err = dqlite_server_config(s->service, DQLITE_CONFIG_WAL_REPLICATION,
				   (void *)name);
	if (err != 0) {
		munit_errorf("failed to set WAL replication name: %d", err);
	}

	err = dqlite_server_config(s->service, DQLITE_CONFIG_METRICS,
				   (void *)(&metrics));
	if (err != 0) {
		munit_errorf("failed to enable metrics: %d", err);
	}

	s->socket = 0;

	return s;
}

static void test_server__destroy(struct test_server *s)
{
	assert(s != NULL);
	assert(s->service != NULL);

#ifdef DQLITE_EXPERIMENTAL
	test_dir_tear_down(s->dir);
#else
	sqlite3_wal_replication_unregister(s->replication);
	sqlite3_vfs_unregister(s->vfs);

	dqlite_vfs_destroy(s->vfs);
#endif

	dqlite_server_destroy(s->service);

	sqlite3_free(s);
	free(logger);
}

static void test_server__listen(struct test_server *s)
{
	int rc;
	struct sockaddr *address;
	socklen_t size;

	assert(s != NULL);

	switch (s->family) {
		case AF_INET:
			memset(&s->in_address, 0, sizeof s->in_address);

			s->in_address.sin_family = AF_INET;
			s->in_address.sin_addr.s_addr = inet_addr("127.0.0.1");
			s->in_address.sin_port = 0;

			address = (struct sockaddr *)(&s->in_address);
			size = sizeof(s->in_address);

			break;

		case AF_UNIX:
			memset(&s->un_address, 0, sizeof s->un_address);

			s->un_address.sun_family = AF_UNIX;
			strcpy(s->un_address.sun_path, "");

			address = (struct sockaddr *)(&s->un_address);
			size = sizeof(s->un_address);

			break;

		default:
			munit_errorf("unexpected socket family: %d", s->family);
	}

	s->socket = socket(s->family, SOCK_STREAM, 0);
	if (s->socket < 0) {
		munit_errorf("failed to open server socket: %s",
			     strerror(errno));
	}

	rc = bind(s->socket, address, size);
	if (rc) {
		munit_errorf("failed to bind server socket: %s",
			     strerror(errno));
	}

	rc = listen(s->socket, 1);
	if (rc) {
		munit_errorf("failed to listen server socket: %s",
			     strerror(errno));
	}

	rc = getsockname(s->socket, address, &size);
	if (rc) {
		munit_errorf("failed to get server address: %s",
			     strerror(errno));
	}
}

static int test_server__connect(struct test_server *s)
{
	int fd;
	int err;
	struct sockaddr *address;
	socklen_t size;

	assert(s != NULL);
	assert(s->socket > 0);

	fd = socket(s->family, SOCK_STREAM, 0);
	if (fd < 0) {
		munit_errorf("failed to open client socket: %s",
			     strerror(errno));
	}

	switch (s->family) {
		case AF_INET:
			address = (struct sockaddr *)(&s->in_address);
			size = sizeof(s->in_address);
			break;

		case AF_UNIX:
			address = (struct sockaddr *)(&s->un_address);
			size = sizeof(s->un_address);
			break;

		default:
			munit_errorf("unexpected socket family: %d", s->family);
	}

	err = connect(fd, address, size);
	if (err) {
		munit_errorf("failed to connect to server socket: %s",
			     strerror(errno));
	}

	return fd;
}

static int test_server__accept(struct test_server *s)
{
	int fd;
	int err;
	struct sockaddr_in address;
	socklen_t size;

	assert(s != NULL);
	assert(s->socket > 0);

	size = sizeof(address);

	fd = accept(s->socket, (struct sockaddr *)&address, &size);
	if (fd < 0) {
		munit_errorf("failed to accept client connection: %s",
			     strerror(errno));
	}

	err = fcntl(fd, F_SETFL, O_NONBLOCK);
	if (err) {
		munit_errorf(
		    "failed to set non-blocking mode on client connection: %s",
		    strerror(errno));
	}

	return fd;
}

static void test__server_close(struct test_server *s)
{
	int rc;

	assert(s != NULL);
	assert(s->socket > 0);

	rc = close(s->socket);
	if (rc != 0) {
		munit_errorf("failed to close server socket: %s",
			     strerror(errno));
	}
}

static void *test__server_run(void *arg)
{
	struct test_server *s;
	int rc;

	s = (struct test_server *)(arg);
	assert(s != NULL);

	rc = dqlite_server_run(s->service);
	if (rc) {
		return (void *)1;
	}

	return 0;
}

struct test_server *test_server_start(const char *family,
				      const MunitParameter params[])
{
	int err;
	int ready;
	struct test_server *s = test_server__create(params);

	assert(s != NULL);

	if (strcmp(family, "tcp") == 0) {
		s->family = AF_INET;
	} else if (strcmp(family, "unix") == 0) {
		s->family = AF_UNIX;
	} else {
		munit_errorf("unexpected socket family: %s", family);
	}

	test_server__listen(s);

	err = pthread_create(&s->thread, 0, &test__server_run, s);
	if (err) {
		munit_errorf("failed to spawn server thread: %s",
			     strerror(errno));
		return 0;
	}

	ready = dqlite_server_ready(s->service);
	if (!ready) {
		munit_errorf("server did not start: %s",
			     dqlite_server_errmsg(s->service));
		return 0;
	}

	return s;
}

void test_server_connect(struct test_server *s, struct test_client **client)
{
	int clientFd;
	int serverFd;
	int err;
	char *errmsg;

	assert(s != NULL);
	assert(client != NULL);

	clientFd = test_server__connect(s);
	serverFd = test_server__accept(s);

	err = dqlite_server_handle(s->service, serverFd, &errmsg);
	if (err) {
		munit_errorf("failed to notify server about new client: %s",
			     errmsg);
	}

	*client = munit_malloc(sizeof **client);

	test_client_init(*client, clientFd);
}

void test_server_stop(struct test_server *t)
{
	int err;
	char *errmsg;
	void *retval = 0;

	assert(t != NULL);

	err = dqlite_server_stop(t->service, &errmsg);
	if (err) {
		munit_errorf("failed to stop dqlite: %s", errmsg);
	}

	err = pthread_join(t->thread, &retval);
	if (err) {
		munit_errorf("failed to join test thread: %s", strerror(errno));
	}

	test__server_close(t);

	if (retval) {
		munit_errorf("test thread error: %s",
			     dqlite_server_errmsg(t->service));
	}

	test_server__destroy(t);
}
