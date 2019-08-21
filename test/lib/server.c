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
			   const unsigned id,
			   const char *address,
			   int *fd)
{
	struct test_server *s = data;
	struct test_server *other = s->others[id - 1];
	int fd_server;
	int rv;
	(void)address;
	munit_assert_ptr_not_null(other);
	test_endpoint_pair(&other->endpoint, &fd_server, fd);
	rv = dqlite_handle(other->dqlite, fd_server);
	munit_assert_int(rv, ==, 0);
	return 0;
}

static void stateWatch(void *data, int old_state, int new_state)
{
	struct test_server *s = data;
	(void)old_state;
	s->state = new_state;
}

void test_server_setup(struct test_server *s,
		       const unsigned id,
		       const MunitParameter params[])
{
	s->id = id;
	sprintf(s->address, "%u", id);

	s->dir = test_dir_setup();
	test_endpoint_setup(&s->endpoint, params);

	memset(s->others, 0, sizeof s->others);

	s->state = -1;
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

	dqlite_task_destroy(s->dqlite);

	test_dir_tear_down(s->dir);
}

void test_server_start(struct test_server *s)
{
	dqlite_task_attr *attr;
	int client;
	int server;
	int rv;

	attr = dqlite_task_attr_create();
	munit_assert_ptr_not_null(attr);

	dqlite_task_attr_set_connect_func(attr, endpointConnect, s);

	rv = dqlite_task_create(s->id, s->address, s->dir, attr, &s->dqlite);
	munit_assert_int(rv, ==, 0);

	rv = dqlite_config(s->dqlite, DQLITE_CONFIG_WATCHER, stateWatch, s);
	munit_assert_int(rv, ==, 0);

	dqlite_task_attr_destroy(attr);

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
