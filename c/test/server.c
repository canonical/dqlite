#include <assert.h>
#include <pthread.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>

#include <sqlite3.h>
#include <CUnit/CUnit.h>

#include "dqlite.h"
#include "cluster.h"
#include "replication.h"
#include "server.h"
#include "client.h"
#include "suite.h"

struct test_server {
	pthread_t                thread;
	sqlite3_wal_replication *replication;
	sqlite3_vfs             *vfs;
	dqlite_server           *service;
	struct sockaddr_in       address;
	int                      socket;
	struct test_client       client;
};

test_server *testServerCreate(){
	int err = 0;
	test_server *s;
	FILE *log = test_suite_dqlite_log();

	s = (test_server*)(sqlite3_malloc(sizeof(test_server)));
	if (s == NULL)
		return NULL;

	s->replication = test_replication();
	err = sqlite3_wal_replication_register(s->replication, 0);
	if (err != 0) {
		return 0;
	}

	err = dqlite_vfs_register(s->replication->zName, &s->vfs);
	if (err != 0) {
		return 0;
	}

	s->service = dqlite_server_alloc();
	if (s->service == NULL) {
		sqlite3_free(s);
		return 0;
	}

	err = dqlite_server_init(s->service, log, test_cluster());
	if (err != 0) {
		dqlite_server_free(s->service);
		sqlite3_free(s);
	}

	s->address.sin_family = AF_INET;
	s->address.sin_addr.s_addr = inet_addr("127.0.0.1");
	s->address.sin_port = 0;

	s->socket = 0;

	return s;
}

void testServerDestroy(test_server *s){
	assert(s != NULL);
	assert(s->service != NULL);

	sqlite3_wal_replication_unregister(s->replication);
	dqlite_vfs_unregister(s->vfs);

	dqlite_server_close(s->service);
	dqlite_server_free(s->service);

	sqlite3_free(s);
}

static int testServerListen(test_server *s){
	int rc;
	struct sockaddr* address;
	socklen_t size;

	assert( s );

	s->socket = socket(AF_INET, SOCK_STREAM, 0);
	if( s->socket<0 ){
		test_suite_printf("failed to open server socket: %s", strerror(errno));
		return 1;
	}

	address = (struct sockaddr*)(&s->address);
	size = sizeof(s->address);

	rc = bind(s->socket, address, size);
	if( rc ){
		test_suite_printf("failed to bind server socket: %s", strerror(errno));
		return 1;
	}

	rc = listen(s->socket, 1);
	if( rc ){
		test_suite_printf("failed to listen server socket: %s", strerror(errno));
		return 1;
	}

	rc = getsockname(s->socket, address, &size);
	if( rc ){
		test_suite_printf("failed to get server address: %s", strerror(errno));
		return 1;
	}

	return 0;
}

static int testServerConnect(test_server *s){
	int fd;
	int err;
	struct sockaddr *address;
	socklen_t size;

	assert( s );
	assert( s->socket );

	fd = socket(AF_INET, SOCK_STREAM, 0);
	if ( fd<0 ){
		test_suite_printf("failed to open client socket: %s", strerror(errno));
		return -1;
	}

	address = (struct sockaddr*)(&s->address);
	size = sizeof(s->address);

	err = connect(fd, address, size);
	if ( err ){
		test_suite_printf("failed to connect to server socket: %s", strerror(errno));
		return -1;
	}

	return fd;
}

static int testServerAccept(test_server *s){
	int fd;
	int err;
	struct sockaddr_in address;
	socklen_t size;

	assert( s );
	assert( s->socket );

	size = sizeof(address);
	fd = accept(s->socket, (struct sockaddr *)&address, &size);
	if ( fd<0 ){
		test_suite_printf("failed to accept client connection: %s", strerror(errno));
		return -1;
	}

	err = fcntl(fd, F_SETFL, O_NONBLOCK);
	if( err ){
		test_suite_printf("failed to set non-blocking mode on client connection: %s", strerror(errno));
		return -1;
	}

	return fd;
}

static int testServerClose(test_server *s){
	int rc;

	assert( s );
	assert( s->socket );

	rc = close(s->socket);
	if( rc ){
		test_suite_printf("failed to close server socket: %s", strerror(errno));
		return 1;
	}

	return 0;
}

static void *testServerRun(void *arg){
	test_server *s;
	int rc;

	s = (test_server*)(arg);
	assert( s );

	rc = dqlite_server_run(s->service);
	if( rc ){
		return (void*)1;
	}

	return 0;
}

test_server *test_server_start(){
	int err;
	int ready;
	test_server *s = testServerCreate();

	assert( s );
	assert( s->service );

	err = testServerListen(s);
	if( err ){
		return 0;
	}

	err = pthread_create(&s->thread, 0, &testServerRun, (void*)s);
	if( err ){
		test_suite_printf("failed to spawn server thread: %s", strerror(errno));
		return 0;
	}

	ready = dqlite_server_ready(s->service);
	if (!ready) {
		test_suite_printf("server did not start: %s", dqlite_server_errmsg(s->service));
		return 0;
	}

	return s;
}

int test_server_connect(test_server *s, struct test_client **client){
	int clientFd;
	int serverFd;
	int err;
	char *errmsg;

	assert( s );
	assert( client );

	clientFd = testServerConnect(s);
	if( clientFd<0 ){
		return 1;
	}

	serverFd = testServerAccept(s);
	if( serverFd<0 ){
		return 1;
	}

	err = dqlite_server_handle(s->service, serverFd, &errmsg);
	if( err ){
		test_suite_printf("failed to notify new client: %s", errmsg);
		return 1;
	}

	test_client_init(&s->client, clientFd);

	*client = &s->client;

	return 0;
}

int test_server_stop(test_server *t){
	int err;
	char *errmsg;
	void *retval = 0;

	assert( t );
	assert( t->service );

	err = dqlite_server_stop(t->service, &errmsg);
	if( err ){
		test_suite_printf("failed to stop dqlite: %s", errmsg);
		return 1;
	}

	err = pthread_join(t->thread, &retval);
	if( err ){
		test_suite_printf("failed to join test thread: %s", strerror(errno));
		return 1;
	}

	err = testServerClose(t);
	if( err ){
		return 1;
	}

	if( retval ){
		test_suite_printf("test thread error: %s", dqlite_server_errmsg(t->service));
		return 1;
	}

	testServerDestroy(t);

	return 0;
}
