#include "../../src/client/protocol.h"
#include "../../src/server.h"
#include "../lib/client.h"
#include "../lib/endpoint.h"
#include "../lib/fs.h"
#include "../lib/heap.h"
#include "../lib/runner.h"
#include "../lib/server.h"
#include "../lib/sqlite.h"
#include "../lib/util.h"

#define N_SERVERS 5
#define FIXTURE                                \
	struct test_server servers[N_SERVERS]; \
	struct client_proto *client;           \
	struct rows rows;

#define SETUP                                                 \
	unsigned i_;                                          \
	test_heap_setup(params, user_data);                   \
	test_sqlite_setup(params);                            \
	for (i_ = 0; i_ < N_SERVERS; i_++) {                  \
		struct test_server *server = &f->servers[i_]; \
		test_server_setup(server, i_ + 1, params);    \
	}                                                     \
	test_server_network(f->servers, N_SERVERS);           \
	for (i_ = 0; i_ < N_SERVERS; i_++) {                  \
		struct test_server *server = &f->servers[i_]; \
		test_server_start(server, params);            \
	}                                                     \
	SELECT(1)

#define TEAR_DOWN                                        \
	unsigned i_;                                     \
	for (i_ = 0; i_ < N_SERVERS; i_++) {             \
		tracef("test_server_tear_down(%u)", i_); \
		test_server_tear_down(&f->servers[i_]);  \
	}                                                \
	test_sqlite_tear_down();                         \
	test_heap_tear_down(data)

#define SELECT(ID) f->client = test_server_client(&f->servers[ID - 1])

#define TRIES 5

static char *trueonly[] = {"1", NULL};

static char *threeonly[] = {"3", NULL};

static MunitParameterEnum role_management_params[] = {
    {"role_management", trueonly},
    {"target_voters", threeonly},
    {"target_standbys", threeonly},
    {NULL, NULL},
};

SUITE(role_management)

struct fixture
{
	FIXTURE;
};

static void *setUp(const MunitParameter params[], void *user_data)
{
	struct fixture *f = munit_malloc(sizeof *f);
	SETUP;
	return f;
}

static void tearDown(void *data)
{
	struct fixture *f = data;
	TEAR_DOWN;
	free(f);
}

static bool hasRole(struct fixture *f, dqlite_node_id id, int role)
{
	struct client_node_info *servers;
	uint64_t n_servers;
	struct client_context context;
	unsigned i;
	bool ret = false;
	int rv;

	clientContextMillis(&context, 5000);
	rv = clientSendCluster(f->client, &context);
	munit_assert_int(rv, ==, 0);
	rv = clientRecvServers(f->client, &servers, &n_servers, &context);
	munit_assert_int(rv, ==, 0);
	for (i = 0; i < n_servers; i += 1) {
		if (servers[i].id == id) {
			ret = servers[i].role == role;
			break;
		}
	}
	for (i = 0; i < n_servers; i += 1) {
		free(servers[i].addr);
	}
	free(servers);

	return ret;
}

TEST(role_management, promote, setUp, tearDown, 0, role_management_params)
{
	struct fixture *f = data;
	unsigned id = 2;
	const char *address = "@2";
	int tries;

	HANDSHAKE;

	id = 2;
	address = "@2";
	ADD(id, address);
	for (tries = 0; tries < TRIES && !hasRole(f, 2, DQLITE_VOTER);
	     tries += 1) {
		sleep(1);
	}
	if (tries == TRIES) {
		return MUNIT_FAIL;
	};

	id = 3;
	address = "@3";
	ADD(id, address);
	for (tries = 0; tries < TRIES && !hasRole(f, 3, DQLITE_VOTER);
	     tries += 1) {
		sleep(1);
	}
	if (tries == TRIES) {
		return MUNIT_FAIL;
	};

	id = 4;
	address = "@4";
	ADD(id, address);
	for (tries = 0; tries < TRIES && !hasRole(f, 4, DQLITE_STANDBY);
	     tries += 1) {
		sleep(1);
	}
	if (tries == TRIES) {
		return MUNIT_FAIL;
	};

	id = 5;
	address = "@5";
	ADD(id, address);
	for (tries = 0; tries < TRIES && !hasRole(f, 5, DQLITE_STANDBY);
	     tries += 1) {
		sleep(1);
	}
	if (tries == TRIES) {
		return MUNIT_FAIL;
	};

	return MUNIT_OK;
}
