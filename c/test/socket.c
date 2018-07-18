#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include "munit.h"
#include "socket.h"

struct test_socket__server {
	struct sockaddr_in in_address;
	int                fd;        /* Listener file descriptor */
	int                client_fd; /* Accepted client connection */
};

struct test_socket__client {
	struct sockaddr_in in_server_address;
	int                fd;
};

static int test_socket__server_bind_and_listen(struct test_socket__server *s) {
	int              err;
	struct sockaddr *address;
	socklen_t        size;

	s->in_address.sin_family      = AF_INET;
	s->in_address.sin_addr.s_addr = inet_addr("127.0.0.1");
	s->in_address.sin_port        = 0;

	address = (struct sockaddr *)(&s->in_address);
	size    = sizeof(s->in_address);

	s->fd = socket(AF_INET, SOCK_STREAM, 0);
	if (s->fd < 0) {
		munit_errorf("failed to open server socket: %s", strerror(errno));
		return -1;
	}

	err = bind(s->fd, address, size);
	if (err != 0) {
		munit_errorf("failed to bind server socket: %s", strerror(errno));
		return -1;
	}

	err = listen(s->fd, 1);
	if (err != 0) {
		munit_errorf("failed to listen server socket: %s", strerror(errno));
		return -1;
	}

	/* Get the actual addressed assigned by the kernel and save it back in
	 * the test_socket__server.address field (pointed to by address). */
	err = getsockname(s->fd, address, &size);
	if (err != 0) {
		munit_errorf("failed to get server address: %s", strerror(errno));
		return -1;
	}

	return 0;
}

static int test_socket__server_accept(struct test_socket__server *s) {
	int                err;
	struct sockaddr_in address; /* Client addressed, unused */
	socklen_t          size;

	size = sizeof(address);

	s->client_fd = accept(s->fd, (struct sockaddr *)&address, &size);

	if (s->client_fd < 0) {
		munit_errorf("failed to accept client connection: %s",
		             strerror(errno));
		return -1;
	}

	/* Set non-blocking mode */
	err = fcntl(s->client_fd, F_SETFL, O_NONBLOCK);
	if (err != 0) {
		munit_errorf(
		    "failed to set non-blocking mode on client connection: %s",
		    strerror(errno));
		return -1;
	}

	return 0;
}

static int test_socket__client_connect(struct test_socket__client *c) {
	int              err;
	struct sockaddr *address;
	socklen_t        size;

	c->fd = socket(AF_INET, SOCK_STREAM, 0);
	if (c->fd < 0) {
		munit_errorf("failed to open client socket: %s", strerror(errno));
		return -1;
	}

	address = (struct sockaddr *)(&c->in_server_address);
	size    = sizeof(c->in_server_address);

	err = connect(c->fd, address, size);
	if (err != 0) {
		munit_errorf("failed to connect to server socket: %s",
		             strerror(errno));
		return -1;
	}

	return 0;
}

int test_socket_pair_initialize(struct test_socket_pair *p) {
	int                        err;
	struct test_socket__server server;
	struct test_socket__client client;

	err = test_socket__server_bind_and_listen(&server);
	if (err != 0) {
		return err;
	}

	client.in_server_address = server.in_address;

	err = test_socket__client_connect(&client);
	if (err != 0) {
		return err;
	}

	err = test_socket__server_accept(&server);
	if (err != 0) {
		return err;
	}

	p->server = server.client_fd;
	p->client = client.fd;

	p->server_disconnected = 0;
	p->client_disconnected = 0;

	p->listen = server.fd;

	return 0;
}

int test_socket_pair_cleanup(struct test_socket_pair *p) {
	int err;

	err = close(p->client);
	if (err != 0) {
		if (!p->client_disconnected || errno != EBADF) {
			munit_errorf("failed to close client socket: %s - %d",
			             strerror(errno),
			             errno);
			return -1;
		}
	}

	err = close(p->server);
	if (err != 0) {
		if (!p->server_disconnected || errno != EBADF) {
			munit_errorf("failed to close server socket: %s - %d",
			             strerror(errno),
			             errno);
			return -1;
		}
	}

	err = close(p->listen);
	if (err != 0) {
		munit_errorf("failed to close listen socket: %s", strerror(errno));
		return -1;
	}

	return 0;
}

int test_socket_pair_client_disconnect(struct test_socket_pair *p) {
	int err;

	assert(!p->client_disconnected);

	err = close(p->client);
	if (err != 0) {
		munit_errorf(
		    "failed to disconnect client: %s - %d", strerror(errno), errno);
		return -1;
	}

	p->client_disconnected = 1;

	return 0;
}

int test_socket_pair_server_disconnect(struct test_socket_pair *p) {
	int err;

	assert(!p->server_disconnected);

	err = close(p->server);
	if (err != 0) {
		munit_errorf(
		    "failed to disconnect server: %s - %d", strerror(errno), errno);
		return -1;
	}

	p->server_disconnected = 1;

	return 0;
}
