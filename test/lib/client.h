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
	client__init(&f->client, f->sockets.client)

#define TEAR_DOWN_CLIENT                       \
	client__close(&f->client);             \
	f->sockets.client_disconnected = true; \
	f->sockets.server_disconnected = true; \
	test_socket_pair_tear_down(&f->sockets)

#endif /* TEST_CLIENT_H */
