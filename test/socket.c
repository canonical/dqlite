#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "munit.h"
#include "socket.h"

char *test_socket_param_values[] = {"tcp", "unix", NULL};

struct test_socket__server {
	sa_family_t family; /* Address family (either AF_INET or AF_UNIX) */
	union {
		struct sockaddr_in in_address;
		struct sockaddr_un un_address;
	};             /* Server address (either a TCP or a Unix address) */
	int fd;        /* Listener file descriptor */
	int client_fd; /* Accepted client connection */
};

struct test_socket__client {
	sa_family_t      family;              /* Address family */
	struct sockaddr *server_address;      /* Address value */
	socklen_t        server_address_size; /* Address struct size */
	int              fd;                  /* Connection to the server */
};

/* Assert that the read and write buffer size of the given socket is at least
 * TEST_SOCKET_MIN_BUF_SIZE. */
static void test_socket__assert_socket_buf_size(int fd)
{
	int       n;
	socklen_t size = sizeof n;
	int       rv;

	/* Read */
	rv = getsockopt(fd, SOL_SOCKET, SO_RCVBUF, &n, &size);
	munit_assert_int(rv, ==, 0);

	munit_assert_int(n, >=, TEST_SOCKET_MIN_BUF_SIZE);

	/* Write */
	rv = getsockopt(fd, SOL_SOCKET, SO_SNDBUF, &n, &size);
	munit_assert_int(rv, ==, 0);

	munit_assert_int(n, >=, TEST_SOCKET_MIN_BUF_SIZE);
}

/* Bind s->fd and start listening on it. */
static void test_socket__server_bind_and_listen(struct test_socket__server *s)
{
	struct sockaddr *address;
	socklen_t        size;
	int              rv;

	/* Initialize the appropriate socket address structure, depending on the
	 * selected socket family. */
	switch (s->family) {

	case AF_INET:
		/* TCP socket on loopback device */
		memset(&s->in_address, 0, sizeof s->in_address);

		s->in_address.sin_family      = AF_INET;
		s->in_address.sin_addr.s_addr = inet_addr("127.0.0.1");
		s->in_address.sin_port        = 0; /* Get a random free port */

		address = (struct sockaddr *)(&s->in_address);
		size    = sizeof s->in_address;

		break;

	case AF_UNIX:
		/* Abstract Unix socket */
		memset(&s->un_address, 0, sizeof s->un_address);

		s->un_address.sun_family = AF_UNIX;
		strcpy(s->un_address.sun_path, ""); /* Get a random address */

		address = (struct sockaddr *)(&s->un_address);
		size    = sizeof s->un_address;

		break;

	default:
		munit_errorf("unexpected socket family: %d", s->family);
	}

	/* Create the listener fd. */
	s->fd = socket(s->family, SOCK_STREAM, 0);
	if (s->fd < 0) {
		munit_errorf("failed to open server socket: %s",
		             strerror(errno));
	}

	/* Bind the listener fd. */
	rv = bind(s->fd, address, size);
	if (rv != 0) {
		munit_errorf("failed to bind server socket: %s",
		             strerror(errno));
	}

	/* Start listening. */
	rv = listen(s->fd, 1);
	if (rv != 0) {
		munit_errorf("failed to listen server socket: %s",
		             strerror(errno));
	}

	/* Get the actual addressed assigned by the kernel and save it back in
	 * the relevant test_socket__server field (pointed to by address). */
	rv = getsockname(s->fd, address, &size);
	if (rv != 0) {
		munit_errorf("failed to get server address: %s",
		             strerror(errno));
	}
}

/* Create a client connection to the server. */
static void test_socket__client_connect(struct test_socket__client *c)
{
	int rv;

	/* Create the socket. */
	c->fd = socket(c->family, SOCK_STREAM, 0);
	if (c->fd < 0) {
		munit_errorf("failed to open client socket: %s",
		             strerror(errno));
	}

	/* Connect to the server */
	rv = connect(c->fd, c->server_address, c->server_address_size);
	if (rv != 0) {
		munit_errorf("failed to connect to server socket: %s",
		             strerror(errno));
	}
}

/* Accept a client connection established with test_socket__client_connect. */
static void test_socket__server_accept(struct test_socket__server *s)
{
	struct sockaddr_in address; /* Client addressed, unused */
	socklen_t          size;
	int                rv;

	size = sizeof address;

	s->client_fd = accept(s->fd, (struct sockaddr *)&address, &size);

	if (s->client_fd < 0) {
		munit_errorf("failed to accept client connection: %s",
		             strerror(errno));
	}

	/* Set non-blocking mode */
	rv = fcntl(s->client_fd, F_SETFL, O_NONBLOCK);
	if (rv != 0) {
		munit_errorf(
		    "failed to set non-blocking mode on client connection: %s",
		    strerror(errno));
	}
}

void test_socket_pair_setup(const MunitParameter     params[],
                            struct test_socket_pair *p)
{
	const char *family = munit_parameters_get(params, TEST_SOCKET_PARAM);

	struct test_socket__server server;
	struct test_socket__client client;

	if (family == NULL) {
		family = "unix";
	}

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
		client.server_address = (struct sockaddr *)&server.in_address;
		client.server_address_size = sizeof server.in_address;
		break;
	case AF_UNIX:
		client.server_address = (struct sockaddr *)&server.un_address;
		client.server_address_size = sizeof server.un_address;
		break;
	default:
		munit_errorf("unexpected socket family: %d", client.family);
	}

	/* Connect the pair */
	test_socket__client_connect(&client);
	test_socket__server_accept(&server);

	p->server = server.client_fd;
	p->client = client.fd;

	test_socket__assert_socket_buf_size(p->server);
	test_socket__assert_socket_buf_size(p->client);

	p->server_disconnected = false;
	p->client_disconnected = false;

	p->listen = server.fd;
}

void test_socket_pair_tear_down(struct test_socket_pair *p)
{
	int rv;

	rv = close(p->client);
	if (rv != 0) {
		if (p->client_disconnected == false || errno != EBADF) {
			munit_errorf("failed to close client socket: %s - %d",
			             strerror(errno),
			             errno);
		}
	}

	rv = close(p->server);
	if (rv != 0) {
		if (p->server_disconnected == false || errno != EBADF) {
			munit_errorf("failed to close server socket: %s - %d",
			             strerror(errno),
			             errno);
		}
	}

	rv = close(p->listen);
	if (rv != 0) {
		munit_errorf("failed to close listen socket: %s",
		             strerror(errno));
	}
}

void test_socket_pair_client_disconnect(struct test_socket_pair *p)
{
	int rv;

	munit_assert(p->client_disconnected == false);

	rv = close(p->client);
	if (rv != 0) {
		munit_errorf("failed to disconnect client: %s - %d",
		             strerror(errno),
		             errno);
	}

	p->client_disconnected = true;
}

void test_socket_pair_server_disconnect(struct test_socket_pair *p)
{
	int rv;

	munit_assert(p->server_disconnected == false);

	rv = close(p->server);
	if (rv != 0) {
		munit_errorf("failed to disconnect server: %s - %d",
		             strerror(errno),
		             errno);
	}

	p->server_disconnected = true;
}
