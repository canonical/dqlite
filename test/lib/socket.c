#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "munit.h"
#include "socket.h"

char *test_socket_param_values[] = {"tcp", "unix", NULL};

struct server
{
	sa_family_t family; /* Address family (either AF_INET or AF_UNIX) */
	int fd;		    /* Listener file descriptor */
	int client_fd;      /* Accepted client connection */
	union {		    /* Server  address (either a TCP or Unix) */
		struct sockaddr_in in_address;
		struct sockaddr_un un_address;
	};
};

struct client
{
	sa_family_t family;		 /* Address family */
	struct sockaddr *server_address; /* Address value */
	socklen_t server_address_size;   /* Address struct size */
	int fd;				 /* Connection to the server */
};

/* Assert that the read and write buffer size of the given socket is at least
 * TEST_SOCKET_MIN_BUF_SIZE. */
static void assert_socket_buf_size(int fd)
{
	int n;
	socklen_t size = sizeof n;
	int rv;

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
static void bind_and_listen(struct server *s, int family)
{
	struct sockaddr *address;
	socklen_t size;
	int rv;

	s->family = family;

	/* Initialize the appropriate socket address structure, depending on the
	 * selected socket family. */
	switch (s->family) {
		case AF_INET:
			/* TCP socket on loopback device */
			memset(&s->in_address, 0, sizeof s->in_address);
			s->in_address.sin_family = AF_INET;
			s->in_address.sin_addr.s_addr = inet_addr("127.0.0.1");
			s->in_address.sin_port = 0; /* Get a random free port */
			address = (struct sockaddr *)(&s->in_address);
			size = sizeof s->in_address;
			break;

		case AF_UNIX:
			/* Abstract Unix socket */
			memset(&s->un_address, 0, sizeof s->un_address);
			s->un_address.sun_family = AF_UNIX;
			strcpy(s->un_address.sun_path, ""); /* Random address */
			address = (struct sockaddr *)(&s->un_address);
			size = sizeof s->un_address;
			break;
		default:
			munit_errorf("unexpected socket family: %d", s->family);
	}

	/* Create the listener fd. */
	s->fd = socket(s->family, SOCK_STREAM, 0);
	if (s->fd < 0) {
		munit_errorf("socket(): %s", strerror(errno));
	}

	/* Bind the listener fd. */
	rv = bind(s->fd, address, size);
	if (rv != 0) {
		munit_errorf("bind(): %s", strerror(errno));
	}

	/* Start listening. */
	rv = listen(s->fd, 1);
	if (rv != 0) {
		munit_errorf("listen(): %s", strerror(errno));
	}

	/* Get the actual addressed assigned by the kernel and save it back in
	 * the relevant struct server field (pointed to by address). */
	rv = getsockname(s->fd, address, &size);
	if (rv != 0) {
		munit_errorf("getsockname(): %s", strerror(errno));
	}
}

/* Create a client connection to the server. */
static void connect_client(struct client *c, struct server *s)
{
	int rv;

	c->family = s->family;

	/* Initialize the client side of the pair. */
	switch (c->family) {
		case AF_INET:
			c->server_address = (struct sockaddr *)&s->in_address;
			c->server_address_size = sizeof s->in_address;
			break;
		case AF_UNIX:
			c->server_address = (struct sockaddr *)&s->un_address;
			c->server_address_size = sizeof s->un_address;
			break;
	}

	/* Create the socket. */
	c->fd = socket(c->family, SOCK_STREAM, 0);
	if (c->fd < 0) {
		munit_errorf("socket(): %s", strerror(errno));
	}

	/* Connect to the server */
	rv = connect(c->fd, c->server_address, c->server_address_size);
	if (rv != 0) {
		munit_errorf("connect(): %s", strerror(errno));
	}
}

/* Accept a client connection established with connect_client. */
static void accept_client(struct server *s)
{
	struct sockaddr_in address; /* Client addressed, unused */
	socklen_t size;
	int rv;

	size = sizeof address;

	s->client_fd = accept(s->fd, (struct sockaddr *)&address, &size);

	if (s->client_fd < 0) {
		munit_errorf("accept client: %s", strerror(errno));
	}

	/* Set non-blocking mode */
	rv = fcntl(s->client_fd, F_SETFL, O_NONBLOCK);
	if (rv != 0) {
		munit_errorf("set non-blocking mode: %s", strerror(errno));
	}
}

static int parse_socket_family_param(const MunitParameter params[])
{
	const char *family =
	    munit_parameters_get(params, TEST_SOCKET_FAMILY);
	if (family == NULL) {
		family = "unix";
	}
	if (strcmp(family, "tcp") == 0) {
		return AF_INET;
	} else if (strcmp(family, "unix") == 0) {
		return AF_UNIX;
	}
	munit_errorf("unexpected socket family: %s", family);
	return -1;
}

void test_socket_pair_setup(const MunitParameter params[],
			    struct test_socket_pair *p)
{
	int family = parse_socket_family_param(params);
	struct server server;
	struct client client;

	/* Initialize the server side of the pair. */
	bind_and_listen(&server, family);

	/* Connect the pair */
	connect_client(&client, &server);
	accept_client(&server);

	p->server = server.client_fd;
	p->client = client.fd;

	assert_socket_buf_size(p->server);
	assert_socket_buf_size(p->client);

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
			munit_errorf("close client: %s", strerror(errno));
		}
	}
	rv = close(p->server);
	if (rv != 0) {
		if (p->server_disconnected == false || errno != EBADF) {
			munit_errorf("close server: %s", strerror(errno));
		}
	}
	rv = close(p->listen);
	if (rv != 0) {
		munit_errorf("close listener: %s", strerror(errno));
	}
}

void test_socket_pair_client_disconnect(struct test_socket_pair *p)
{
	int rv;
	munit_assert(p->client_disconnected == false);
	rv = close(p->client);
	if (rv != 0) {
		munit_errorf("disconnect client: %s", strerror(errno));
	}

	p->client_disconnected = true;
}

void test_socket_pair_server_disconnect(struct test_socket_pair *p)
{
	int rv;
	munit_assert(p->server_disconnected == false);
	rv = close(p->server);
	if (rv != 0) {
		munit_errorf("disconnect server: %s", strerror(errno));
	}
	p->server_disconnected = true;
}
