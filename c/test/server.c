#include <arpa/inet.h>
#include <assert.h>
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
#include "munit.h"
#include "replication.h"
#include "server.h"

struct test_server {
	pthread_t                thread;
	sqlite3_wal_replication *replication;
	sqlite3_vfs *            vfs;
	dqlite_server *          service;
	struct sockaddr_in       address;
	int                      socket;
	struct test_client       client;
};

static struct test_server *test_server__create() {
	int                 err = 0;
	struct test_server *s;

	s = munit_malloc(sizeof *s);

	s->replication = test_replication();

	err = sqlite3_wal_replication_register(s->replication, 0);
	if (err != 0) {
		munit_errorf("failed to register wal replication: %d", err);
	}

	s->vfs = dqlite_vfs_create(s->replication->zName);
	if (s->vfs == NULL) {
		munit_error("failed to create volatile VFS: out of memory");
	}

	sqlite3_vfs_register(s->vfs, 0);

	err = dqlite_server_create(test_cluster(), &s->service);
	if (err != 0) {
		munit_errorf("failed to create dqlite server: %d", err);
	}

	err = dqlite_server_config(
	    s->service, DQLITE_CONFIG_VFS, (void *)s->vfs->zName);
	if (err != 0) {
		munit_errorf("failed to set VFS name: %d", err);
	}

	err = dqlite_server_config(s->service,
	                           DQLITE_CONFIG_WAL_REPLICATION,
	                           (void *)s->replication->zName);
	if (err != 0) {
		munit_errorf("failed to set WAL replication name: %d", err);
	}

	s->address.sin_family      = AF_INET;
	s->address.sin_addr.s_addr = inet_addr("127.0.0.1");
	s->address.sin_port        = 0;

	s->socket = 0;

	return s;
}

static void test_server__destroy(struct test_server *s) {
	assert(s != NULL);
	assert(s->service != NULL);

	sqlite3_wal_replication_unregister(s->replication);
	sqlite3_vfs_unregister(s->vfs);

	dqlite_vfs_destroy(s->vfs);

	dqlite_server_destroy(s->service);

	sqlite3_free(s);
}

static void test_server__listen(struct test_server *s) {
	int              rc;
	struct sockaddr *address;
	socklen_t        size;

	assert(s);

	s->socket = socket(AF_INET, SOCK_STREAM, 0);
	if (s->socket < 0) {
		munit_errorf("failed to open server socket: %s", strerror(errno));
	}

	address = (struct sockaddr *)(&s->address);
	size    = sizeof(s->address);

	rc = bind(s->socket, address, size);
	if (rc) {
		munit_errorf("failed to bind server socket: %s", strerror(errno));
	}

	rc = listen(s->socket, 1);
	if (rc) {
		munit_errorf("failed to listen server socket: %s", strerror(errno));
	}

	rc = getsockname(s->socket, address, &size);
	if (rc) {
		munit_errorf("failed to get server address: %s", strerror(errno));
	}
}

static int test_server__connect(struct test_server *s) {
	int              fd;
	int              err;
	struct sockaddr *address;
	socklen_t        size;

	assert(s);
	assert(s->socket);

	fd = socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0) {
		munit_errorf("failed to open client socket: %s", strerror(errno));
	}

	address = (struct sockaddr *)(&s->address);
	size    = sizeof(s->address);

	err = connect(fd, address, size);
	if (err) {
		munit_errorf("failed to connect to server socket: %s",
		             strerror(errno));
	}

	return fd;
}

static int test_server__accept(struct test_server *s) {
	int                fd;
	int                err;
	struct sockaddr_in address;
	socklen_t          size;

	assert(s);
	assert(s->socket);

	size = sizeof(address);
	fd   = accept(s->socket, (struct sockaddr *)&address, &size);
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

static void test__server_close(struct test_server *s) {
	int rc;

	assert(s);
	assert(s->socket);

	rc = close(s->socket);
	if (rc != 0) {
		munit_errorf("failed to close server socket: %s", strerror(errno));
	}
}

static void *test__server_run(void *arg) {
	struct test_server *s;
	int                 rc;

	s = (struct test_server *)(arg);
	assert(s);

	rc = dqlite_server_run(s->service);
	if (rc) {
		return (void *)1;
	}

	return 0;
}

struct test_server *test_server_start() {
	int                 err;
	int                 ready;
	struct test_server *s = test_server__create();

	assert(s);
	assert(s->service);

	test_server__listen(s);

	err = pthread_create(&s->thread, 0, &test__server_run, (void *)s);
	if (err) {
		munit_errorf("failed to spawn server thread: %s", strerror(errno));
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

void test_server_connect(struct test_server *s, struct test_client **client) {
	int   clientFd;
	int   serverFd;
	int   err;
	char *errmsg;

	assert(s);
	assert(client);

	clientFd = test_server__connect(s);
	serverFd = test_server__accept(s);

	err = dqlite_server_handle(s->service, serverFd, &errmsg);
	if (err) {
		munit_errorf("failed to notify server about new client: %s", errmsg);
	}

	test_client_init(&s->client, clientFd);

	*client = &s->client;
}

void test_server_stop(struct test_server *t) {
	int   err;
	char *errmsg;
	void *retval = 0;

	assert(t);
	assert(t->service);

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
