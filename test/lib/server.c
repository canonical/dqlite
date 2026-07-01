#ifdef _WIN32
#include "../../src/transport.h"
#else
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#endif

#include "fs.h"
#include "server.h"

const char *test_server_address(unsigned id)
{
#if defined(_WIN32) || (defined(__APPLE__) && defined(__MACH__))
	/* Test server helpers use TCP on platforms without abstract sockets. */
	static char addresses[16][64];
	munit_assert(id < 16);
	snprintf(addresses[id], sizeof addresses[id], "127.0.0.1:%u", 9000 + id);
	return addresses[id];
#else
	static char addresses[16][64];
	munit_assert(id < 16);
	snprintf(addresses[id], sizeof addresses[id], "@%u", id);
	return addresses[id];
#endif
}


static int endpointConnect(void *data, const char *address, int *fd)
{
	(void)data;
#ifdef _WIN32
	return transportDefaultConnect(NULL, address, fd);
#elif defined(__APPLE__) && defined(__MACH__)
	/* Avoid linking this helper against dqlite transport internals. */
	struct sockaddr_in addr;
	const char *port;
	int rv;

	port = strchr(address, ':');
	munit_assert_ptr_not_null(port);
	port++;

	memset(&addr, 0, sizeof addr);
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	addr.sin_port = htons((uint16_t)atoi(port));

	*fd = socket(AF_INET, SOCK_STREAM, 0);
	munit_assert_int(*fd, !=, -1);
	rv = fcntl(*fd, F_SETFD, FD_CLOEXEC);
	munit_assert_int(rv, ==, 0);
	rv = connect(*fd, (struct sockaddr *)&addr, sizeof addr);
	return rv;
#else
	struct sockaddr_un addr;
	int type = SOCK_STREAM;
	int rv;
	(void)address;
	memset(&addr, 0, sizeof addr);
	addr.sun_family = AF_UNIX;
	strcpy(addr.sun_path + 1, address + 1);
#ifdef SOCK_CLOEXEC
	type |= SOCK_CLOEXEC;
#endif
	*fd = socket(AF_UNIX, type, 0);
	munit_assert_int(*fd, !=, -1);
#ifndef SOCK_CLOEXEC
	rv = fcntl(*fd, F_SETFD, FD_CLOEXEC);
	munit_assert_int(rv, ==, 0);
#endif
	rv = connect(*fd, (struct sockaddr *)&addr,
		     sizeof(sa_family_t) + strlen(address + 1) + 1);
	return rv;
#endif
}

void test_server_setup(struct test_server *s,
		       const unsigned id,
		       const MunitParameter params[])
{
	(void)params;

	s->id = id;
	snprintf(s->address, sizeof s->address, "%s", test_server_address(id));

	s->dir = test_dir_setup();
	s->role_management = false;

	memset(s->others, 0, sizeof s->others);
}

void test_server_stop(struct test_server *s)
{
	int rv;

	clientClose(&s->client);

	if (s->role_management) {
		dqlite_node_handover(s->dqlite);
		rv = dqlite_node_stop(s->dqlite);
	} else {
		rv = dqlite_node_stop(s->dqlite);
	}
	munit_assert_int(rv, ==, 0);

	dqlite_node_destroy(s->dqlite);
}

void test_server_tear_down(struct test_server *s)
{
	test_server_stop(s);
	test_dir_tear_down(s->dir);
}

void test_server_prepare(struct test_server *s, const MunitParameter params[])
{
	int rv;

	rv = dqlite_node_create(s->id, s->address, s->dir, &s->dqlite);
	munit_assert_int(rv, ==, 0);

	rv = dqlite_node_set_bind_address(s->dqlite, s->address);
	munit_assert_int(rv, ==, 0);

	rv = dqlite_node_set_connect_func(s->dqlite, endpointConnect, s);
	munit_assert_int(rv, ==, 0);

	rv = dqlite_node_set_network_latency_ms(s->dqlite, 10);
	munit_assert_int(rv, ==, 0);

	const char *snapshot_threshold_param =
	    munit_parameters_get(params, SNAPSHOT_THRESHOLD_PARAM);
	if (snapshot_threshold_param != NULL) {
		unsigned threshold = (unsigned)atoi(snapshot_threshold_param);
		rv = dqlite_node_set_snapshot_params(s->dqlite, threshold,
						     threshold);
		munit_assert_int(rv, ==, 0);
	}

	const char *snapshot_compression_param =
	    munit_parameters_get(params, SNAPSHOT_COMPRESSION_PARAM);
	if (snapshot_compression_param != NULL) {
		bool snapshot_compression =
		    (bool)atoi(snapshot_compression_param);
		rv = dqlite_node_set_snapshot_compression(s->dqlite,
							  snapshot_compression);
		munit_assert_int(rv, ==, 0);
	}

	const char *target_voters_param =
	    munit_parameters_get(params, "target_voters");
	if (target_voters_param != NULL) {
		int n = atoi(target_voters_param);
		rv = dqlite_node_set_target_voters(s->dqlite, n);
		munit_assert_int(rv, ==, 0);
	}

	const char *target_standbys_param =
	    munit_parameters_get(params, "target_standbys");
	if (target_standbys_param != NULL) {
		int n = atoi(target_standbys_param);
		rv = dqlite_node_set_target_standbys(s->dqlite, n);
		munit_assert_int(rv, ==, 0);
	}

	const char *role_management_param =
	    munit_parameters_get(params, "role_management");
	if (role_management_param != NULL) {
		bool role_management = (bool)atoi(role_management_param);
		s->role_management = role_management;
		if (role_management) {
			rv = dqlite_node_enable_role_management(s->dqlite);
			munit_assert_int(rv, ==, 0);
		}
	}
}

void test_server_run(struct test_server *s)
{
	int rv;

	rv = dqlite_node_start(s->dqlite);
	if (rv != 0) {
		munit_errorf("dqlite_node_start(): %s", dqlite_node_errmsg(s->dqlite));
	}

	test_server_client_connect(s, &s->client);
}

void test_server_start(struct test_server *s, const MunitParameter params[])
{
	test_server_prepare(s, params);
	test_server_run(s);
}

struct client_proto *test_server_client(struct test_server *s)
{
	return &s->client;
}

void test_server_client_reconnect(struct test_server *s, struct client_proto *c)
{
	clientClose(c);
	test_server_client_connect(s, c);
}

void test_server_client_connect(struct test_server *s, struct client_proto *c)
{
	int rv;
	int fd;

	rv = endpointConnect(NULL, s->address, &fd);
	munit_assert_int(rv, ==, 0);

	memset(c, 0, sizeof *c);
	buffer__init(&c->read);
	buffer__init(&c->write);
	c->fd = fd;
}

static void setOther(struct test_server *s, struct test_server *other)
{
	unsigned i = other->id - 1;
	munit_assert_ptr_null(s->others[i]);
	s->others[i] = other;
}
void test_server_network(struct test_server *servers, unsigned n_servers)
{
	unsigned i;
	unsigned j;
	for (i = 0; i < n_servers; i++) {
		for (j = 0; j < n_servers; j++) {
			struct test_server *server = &servers[i];
			struct test_server *other = &servers[j];
			if (i == j) {
				continue;
			}
			setOther(server, other);
		}
	}
}
