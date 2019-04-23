/**
 * Setup a test dqlite client.
 */

#ifndef TEST_CLIENT_H
#define TEST_CLIENT_H

#define FIXTURE_CLIENT        \
	struct client client; \
	struct test_socket_pair sockets

#define SETUP_CLIENT                                 \
	test_socket_pair_setup(params, &f->sockets); \
	clientInit(&f->client, f->sockets.client)

#define TEAR_DOWN_CLIENT                       \
	clientClose(&f->client);             \
	f->sockets.server_disconnected = true; \
	test_socket_pair_tear_down(&f->sockets)

#endif /* TEST_CLIENT_H */
