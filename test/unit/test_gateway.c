#include "../lib/cluster.h"
#include "../lib/runner.h"

#include "../../src/gateway.h"

TEST_MODULE(gateway);

struct fixture
{
	FIXTURE_CLUSTER;
	struct gateway gateways[N_SERVERS];
};

static void *setup(const MunitParameter params[], void *user_data)
{
	struct fixture *f = munit_malloc(sizeof *f);
	unsigned i;
	SETUP_CLUSTER;
	for (i = 0; i < N_SERVERS; i++) {
		struct gateway *g = &f->gateways[i];
		gateway__init(g, CLUSTER_LOGGER(i), CLUSTER_OPTIONS(i),
			      CLUSTER_REGISTRY(i));
	}
	return f;
}

static void tear_down(void *data)
{
	struct fixture *f = data;
	unsigned i;
	for (i = 0; i < N_SERVERS; i++) {
		struct gateway *g = &f->gateways[i];
		gateway__close(g);
	}
	TEAR_DOWN_CLUSTER;
	free(f);
}

/******************************************************************************
 *
 * Handle an open request.
 *
 ******************************************************************************/

TEST_SUITE(open);
TEST_SETUP(open, setup);
TEST_TEAR_DOWN(open, tear_down);

/* Successfully open a database connection. */
TEST_CASE(open, success, NULL)
{
	struct fixture *f = data;
	(void)params;
	(void)f;
	return MUNIT_OK;
}
