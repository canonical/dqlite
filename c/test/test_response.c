#include <stdint.h>

#include <CUnit/CUnit.h>
#include <capnp_c.h>

#include "../src/response.h"
#include "../src/protocol.capnp.h"

static struct dqlite__response response;
static struct capn session;
static capn_ptr root;

void test_dqlite__response_setup()
{
	dqlite__response_init(&response);
	capn_init_malloc(&session);
}

void test_dqlite__response_teardown()
{
	dqlite__response_close(&response);
}

static void test_dqlite__response_read() {
	int err;
	uint8_t *data;
	size_t size;

	data = dqlite__response_data(&response);
	CU_ASSERT_PTR_NOT_NULL_FATAL(data);

	size = dqlite__response_size(&response);
	CU_ASSERT_FATAL(size);

	err = capn_init_mem(&session, data, size, 0);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	root = capn_root(&session);
}

#define TEST_DQLITE__RESPONSE_READ(PTR) \
	test_dqlite__response_read(); \
	PTR.p = capn_getp(root, 0, 1);

void test_dqlite__response_cluster()
{
	int err;
	Cluster_ptr ptr;
	struct Cluster cluster;

	err = dqlite__response_cluster(&response, "1.2.3.4:666", 15);
	CU_ASSERT_EQUAL_FATAL(err, 0);

	TEST_DQLITE__RESPONSE_READ(ptr);

	read_Cluster(&cluster, ptr);

	CU_ASSERT_STRING_EQUAL(cluster.leader.str, "1.2.3.4:666");
	CU_ASSERT_EQUAL(cluster.heartbeat, 15);
}
