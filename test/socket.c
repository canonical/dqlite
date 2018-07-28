#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "munit.h"
#include "socket.h"

struct test_socket__server {
	int family; /* Address family (either AF_INET or AF_UNIX) */
	union {
		struct sockaddr_in in_address;
		struct sockaddr_un un_address;
	};             /* Server address (either a TCP or a Unix address) */
	int fd;        /* Listener file descriptor */
	int client_fd; /* Accepted client connection */
};

struct test_socket__client {
	int family; /* Address family (either AF_INET or AF_UNIX) */
	union {
		struct sockaddr_in in_server_address;
		struct sockaddr_un un_server_address;
	};      /* Server address (either a TCP or a Unix address) */
	int fd; /* Connection to the server */
};

static void test_socket__server_bind_and_listen(struct test_socket__server *s) {
	int              err;
	struct sockaddr *address;
	socklen_t        size;

	switch (s->family) {

	case AF_INET:
		memset(&s->in_address, 0, sizeof s->in_address);

		s->in_address.sin_family      = AF_INET;
		s->in_address.sin_addr.s_addr = inet_addr("127.0.0.1");
		s->in_address.sin_port        = 0;

		address = (struct sockaddr *)(&s->in_address);
		size    = sizeof(s->in_address);

		break;

	case AF_UNIX:
		memset(&s->un_address, 0, sizeof s->un_address);

		s->un_address.sun_family = AF_UNIX;
		strcpy(s->un_address.sun_path, "");

		address = (struct sockaddr *)(&s->un_address);
		size    = sizeof(s->un_address);

		break;

	default:
		munit_errorf("unexpected socket family: %d", s->family);
	}

	s->fd = socket(s->family, SOCK_STREAM, 0);
	if (s->fd < 0) {
		munit_errorf("failed to open server socket: %s", strerror(errno));
	}

	err = bind(s->fd, address, size);
	if (err != 0) {
		munit_errorf("failed to bind server socket: %s", strerror(errno));
	}

	err = listen(s->fd, 1);
	if (err != 0) {
		munit_errorf("failed to listen server socket: %s", strerror(errno));
	}

	/* Get the actual addressed assigned by the kernel and save it back in
	 * the relevant test_socket__server field (pointed to by address). */
	err = getsockname(s->fd, address, &size);
	if (err != 0) {
		munit_errorf("failed to get server address: %s", strerror(errno));
	}
}

static void test_socket__client_connect(struct test_socket__client *c) {
	int              err;
	struct sockaddr *address;
	socklen_t        size;

	c->fd = socket(c->family, SOCK_STREAM, 0);
	if (c->fd < 0) {
		munit_errorf("failed to open client socket: %s", strerror(errno));
	}

	switch (c->family) {
	case AF_INET:
		address = (struct sockaddr *)(&c->in_server_address);
		size    = sizeof(c->in_server_address);
		break;

	case AF_UNIX:
		address = (struct sockaddr *)(&c->un_server_address);
		size    = sizeof(c->un_server_address);
		break;

	default:
		munit_errorf("unexpected socket family: %d", c->family);
	}

	err = connect(c->fd, address, size);
	if (err != 0) {
		munit_errorf("failed to connect to server socket: %s",
		             strerror(errno));
	}
}

static void test_socket__server_accept(struct test_socket__server *s) {
	int                err;
	struct sockaddr_in address; /* Client addressed, unused */
	socklen_t          size;

	size = sizeof(address);

	s->client_fd = accept(s->fd, (struct sockaddr *)&address, &size);

	if (s->client_fd < 0) {
		munit_errorf("failed to accept client connection: %s",
		             strerror(errno));
	}

	/* Set non-blocking mode */
	err = fcntl(s->client_fd, F_SETFL, O_NONBLOCK);
	if (err != 0) {
		munit_errorf(
		    "failed to set non-blocking mode on client connection: %s",
		    strerror(errno));
	}
}

void test_socket_pair_init(struct test_socket_pair *p, const char *family) {
	struct test_socket__server server;
	struct test_socket__client client;

	if (strcmp(family, "tcp") == 0) {
		server.family = AF_INET;
		client.family = AF_INET;
	} else if (strcmp(family, "unix") == 0) {
		server.family = AF_UNIX;
		client.family = AF_UNIX;
	} else {
		munit_errorf("unexpected socket family: %s", family);
	}

	/* Initialize the server side of the pair. */
	test_socket__server_bind_and_listen(&server);

	/* Initialize the client side of the pair. */
	switch (client.family) {
	case AF_INET:
		client.in_server_address = server.in_address;
		break;
	case AF_UNIX:
		client.un_server_address = server.un_address;
		break;
	default:
		munit_errorf("unexpected socket family: %d", client.family);
	}

	/* Connect the pair */
	test_socket__client_connect(&client);
	test_socket__server_accept(&server);

	p->server = server.client_fd;
	p->client = client.fd;

	p->server_disconnected = 0;
	p->client_disconnected = 0;

	p->listen = server.fd;
}

void test_socket_pair_close(struct test_socket_pair *p) {
	int err;

	err = close(p->client);
	if (err != 0) {
		if (!p->client_disconnected || errno != EBADF) {
			munit_errorf("failed to close client socket: %s - %d",
			             strerror(errno),
			             errno);
		}
	}

	err = close(p->server);
	if (err != 0) {
		if (!p->server_disconnected || errno != EBADF) {
			munit_errorf("failed to close server socket: %s - %d",
			             strerror(errno),
			             errno);
		}
	}

	err = close(p->listen);
	if (err != 0) {
		munit_errorf("failed to close listen socket: %s", strerror(errno));
	}
}

void test_socket_pair_client_disconnect(struct test_socket_pair *p) {
	int err;

	assert(!p->client_disconnected);

	err = close(p->client);
	if (err != 0) {
		munit_errorf(
		    "failed to disconnect client: %s - %d", strerror(errno), errno);
	}

	p->client_disconnected = 1;
}

void test_socket_pair_server_disconnect(struct test_socket_pair *p) {
	int err;

	assert(!p->server_disconnected);

	err = close(p->server);
	if (err != 0) {
		munit_errorf(
		    "failed to disconnect server: %s - %d", strerror(errno), errno);
	}

	p->server_disconnected = 1;
}
