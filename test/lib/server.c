#include "unistd.h"

#include "fs.h"
#include "server.h"

static int endpointConnect(void *data,
			   const char *address,
			   int *fd)
{
	struct sockaddr_un addr;
	int rv;
	(void)address;
	(void)data;
	memset(&addr, 0, sizeof addr);
	addr.sun_family = AF_UNIX;
	strcpy(addr.sun_path + 1, address + 1);
	*fd = socket(AF_UNIX, SOCK_STREAM, 0);
	munit_assert_int(*fd, !=, -1);
	rv = connect(*fd, (struct sockaddr *)&addr, sizeof(sa_family_t) + strlen(address + 1) + 1);
	munit_assert_int(rv, ==, 0);
	return 0;
}

void test_server_setup(struct test_server *s,
		       const unsigned id,
		       const MunitParameter params[])
{
	s->id = id;
	sprintf(s->address, "@%u", id);

	s->dir = test_dir_setup();
	test_endpoint_setup(&s->endpoint, params);

	memset(s->others, 0, sizeof s->others);
}

void test_server_tear_down(struct test_server *s)
{
	int rv;

	test_endpoint_tear_down(&s->endpoint);
	clientClose(&s->client);
	close(s->client.fd);
	rv = dqlite_node_stop(s->dqlite);
	munit_assert_int(rv, ==, 0);

	dqlite_node_destroy(s->dqlite);

	test_dir_tear_down(s->dir);
}

void test_server_start(struct test_server *s)
{
	int client;
	int rv;

	rv = dqlite_node_create(s->id, s->address, s->dir, &s->dqlite);
	munit_assert_int(rv, ==, 0);

	rv = dqlite_node_set_bind_address(s->dqlite, s->address);
	munit_assert_int(rv, ==, 0);

	rv = dqlite_node_set_connect_func(s->dqlite, endpointConnect, s);
	munit_assert_int(rv, ==, 0);

	rv = dqlite_node_start(s->dqlite);
	munit_assert_int(rv, ==, 0);

	/* Connect a client. */
	rv = endpointConnect(NULL, s->address, &client);
	munit_assert_int(rv, ==, 0);

	rv = clientInit(&s->client, client);
	munit_assert_int(rv, ==, 0);
}

struct client *test_server_client(struct test_server *s)
{
	return &s->client;
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
