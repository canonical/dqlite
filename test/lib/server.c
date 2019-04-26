#include "unistd.h"

#include "fs.h"
#include "server.h"

static void *run(void *arg)
{
	struct test_server *s = arg;
	int rv;
	rv = dqlite_run(s->dqlite);
	if (rv != 0) {
		return (void *)1;
	}
	return NULL;
}

static int endpointConnect(void *data,
			   const struct dqlite_server *server,
			   int *fd)
{
	struct test_server *s = data;
	struct test_server *other = s->others[server->id - 1];
	int fd_server;
	int rv;
	munit_assert_ptr_not_null(other);
	test_endpoint_pair(&other->endpoint, &fd_server, fd);
	rv = dqlite_handle(other->dqlite, fd_server);
	munit_assert_int(rv, ==, 0);
	return 0;
}

void test_server_setup(struct test_server *s,
		       const unsigned id,
		       struct dqlite_server *servers,
		       unsigned n_servers,
		       const MunitParameter params[])
{
	int rv;

	s->id = id;
	sprintf(s->address, "%u", id);

	s->dir = test_dir_setup();
	test_endpoint_setup(&s->endpoint, params);

	rv = dqlite_create(id, s->address, s->dir, &s->dqlite);
	munit_assert_int(rv, ==, 0);

	rv =
	    dqlite_config(s->dqlite, DQLITE_CONFIG_CONNECT, endpointConnect, s);
	munit_assert_int(rv, ==, 0);

	if (servers != NULL) {
		rv = dqlite_bootstrap(s->dqlite, n_servers, servers);
		munit_assert_int(rv, ==, 0);
	}

	memset(s->others, 0, sizeof s->others);
}

void test_server_tear_down(struct test_server *s)
{
	void *retval;
	int rv;

	test_endpoint_tear_down(&s->endpoint);
	clientClose(&s->client);
	close(s->client.fd);
	dqlite_stop(s->dqlite);

	rv = pthread_join(s->run, &retval);
	munit_assert_int(rv, ==, 0);
	munit_assert_ptr_null(retval);

	dqlite_destroy(s->dqlite);

	test_dir_tear_down(s->dir);
}

void test_server_start(struct test_server *s)
{
	int rv;
	int client;
	int server;

	rv = pthread_create(&s->run, 0, &run, s);
	munit_assert_int(rv, ==, 0);

	/* Wait for the server to be ready */
	munit_assert_true(dqlite_ready(s->dqlite));

	/* Connect a client. */
	test_endpoint_pair(&s->endpoint, &server, &client);
	rv = clientInit(&s->client, client);
	munit_assert_int(rv, ==, 0);
	rv = dqlite_handle(s->dqlite, server);
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
