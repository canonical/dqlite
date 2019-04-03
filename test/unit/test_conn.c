#include <raft.h>
#include <raft/io_uv.h>

#include "../lib/heap.h"
#include "../lib/logger.h"
#include "../lib/options.h"
#include "../lib/raft.h"
#include "../lib/registry.h"
#include "../lib/runner.h"
#include "../lib/socket.h"
#include "../lib/sqlite.h"
#include "../lib/vfs.h"

#include "../../src/conn.h"
#include "../../src/client.h"
#include "../../src/gateway.h"
#include "../../src/lib/transport.h"
#include "../../src/transport.h"

TEST_MODULE(conn);

/******************************************************************************
 *
 * Fixture
 *
 ******************************************************************************/

struct fixture
{
	FIXTURE_LOGGER;
	FIXTURE_VFS;
	FIXTURE_OPTIONS;
	FIXTURE_REGISTRY;
	FIXTURE_RAFT;
	struct test_socket_pair sockets;
	struct conn conn;
	struct client client;
};

static void *setup(const MunitParameter params[], void *user_data)
{
	struct fixture *f = munit_malloc(sizeof *f);
	int rv;
	SETUP_HEAP;
	SETUP_SQLITE;
	SETUP_LOGGER;
	SETUP_VFS;
	SETUP_OPTIONS;
	SETUP_REGISTRY;
	SETUP_RAFT;
	test_socket_pair_setup(params, &f->sockets);
	rv = conn__start(&f->conn, &f->logger, &f->loop, &f->options,
			 &f->registry, &f->raft, f->sockets.server,
			 &f->raft_transport, NULL);
	client__init(&f->client, f->sockets.client);
	return f;
}

static void tear_down(void *data)
{
	struct fixture *f = data;
	client__close(&f->client);
	conn__stop(&f->conn);
	f->sockets.client_disconnected = true;
	f->sockets.server_disconnected = true;
	test_socket_pair_tear_down(&f->sockets);
	TEAR_DOWN_RAFT;
	TEAR_DOWN_REGISTRY;
	TEAR_DOWN_OPTIONS;
	TEAR_DOWN_VFS;
	TEAR_DOWN_LOGGER;
	TEAR_DOWN_SQLITE;
	TEAR_DOWN_HEAP;
	free(data);
}

/******************************************************************************
 *
 * Helper macros.
 *
 ******************************************************************************/

/******************************************************************************
 *
 * Assertions.
 *
 ******************************************************************************/

/******************************************************************************
 *
 * Handle the handshake
 *
 ******************************************************************************/

TEST_SUITE(handshake);
TEST_SETUP(handshake, setup);
TEST_TEAR_DOWN(handshake, tear_down);

TEST_CASE(handshake, success, NULL)
{
	struct fixture *f = data;
	(void)params;
	return MUNIT_OK;
}
