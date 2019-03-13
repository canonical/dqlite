/**
 * Helpers for using a raft instance in test fixtures.
 */

#ifndef TEST_RAFT_H
#define TEST_RAFT_H

#include <raft.h>
#include <raft/io_stub.h>

#include "munit.h"

#define FIXTURE_RAFT                    \
	struct raft_logger raft_logger; \
	struct raft_io raft_io;         \
	struct raft_fsm fsm;            \
	struct raft raft

#define SETUP_RAFT                                                     \
	{                                                              \
		uint64_t id = 1;                                       \
		const char *address = "1";                             \
		int rv;                                                \
		f->raft_logger = raft_default_logger;                  \
		rv = raft_io_stub_init(&f->raft_io, &f->raft_logger);  \
		munit_assert_int(rv, ==, 0);                           \
		rv = raft_init(&f->raft, &f->raft_logger, &f->raft_io, \
			       &f->fsm, f, id, address);               \
		munit_assert_int(rv, ==, 0);                           \
		raft_set_rand(&f->raft, (int (*)())munit_rand_uint32); \
		RAFT_BOOTSTRAP(2);                                     \
		RAFT_START;                                            \
	}

#define TEAR_DOWN_RAFT                           \
	{                                        \
		raft_close(&f->raft, NULL);      \
		raft_io_stub_close(&f->raft_io); \
	}

/**
 * Bootstrap the fixture raft instance.
 *
 * The initial configuration will have the given amount of servers and will be
 * saved as first entry in the log. The server IDs are assigned sequentially
 * starting from 1 up to @N_SERVERS. All servers will be voting servers.
 */
#define RAFT_BOOTSTRAP(N_SERVERS)                                       \
	{                                                               \
		struct raft_configuration configuration;                \
		int i;                                                  \
		int rv;                                                 \
		raft_configuration_init(&configuration);                \
		for (i = 0; i < N_SERVERS; i++) {                       \
			unsigned id = i + 1;                            \
			char address[4];                                \
			sprintf(address, "%d", id);                     \
			rv = raft_configuration_add(&configuration, id, \
						    address, true);     \
			munit_assert_int(rv, ==, 0);                    \
		}                                                       \
		rv = raft_bootstrap(&f->raft, &configuration);          \
		munit_assert_int(rv, ==, 0);                            \
		raft_configuration_close(&configuration);               \
	}

#define RAFT_START                           \
	{                                    \
		int rc;                      \
		rc = raft_start(&f->raft);   \
		munit_assert_int(rc, ==, 0); \
	}

#define RAFT_BECOME_CANDIDATE                                       \
	raft_io_stub_advance(&f->raft_io,                           \
			     f->raft.election_timeout_rand + 100);  \
	munit_assert_int(raft_state(&f->raft), ==, RAFT_CANDIDATE); \
	raft_io_stub_flush(&f->raft_io);

#define RAFT_BECOME_LEADER                                                  \
	{                                                                   \
		struct raft *r = &f->raft;                                  \
		size_t votes = r->configuration.n / 2;                      \
		size_t i;                                                   \
		RAFT_BECOME_CANDIDATE;                                      \
		for (i = 0; i < r->configuration.n; i++) {                  \
			struct raft_server *server =                        \
			    &r->configuration.servers[i];                   \
			struct raft_message message;                        \
			if (server->id == f->raft.id) {                     \
				continue;                                   \
			}                                                   \
			message.type = RAFT_IO_REQUEST_VOTE_RESULT;         \
			message.server_id = server->id;                     \
			message.server_address = server->address;           \
			message.request_vote_result.term = r->current_term; \
			message.request_vote_result.vote_granted = 1;       \
			raft_io_stub_dispatch(r->io, &message);             \
			votes--;                                            \
			if (votes == 0) {                                   \
				break;                                      \
			}                                                   \
		}                                                           \
		munit_assert_int(raft_state(r), ==, RAFT_LEADER);           \
		raft_io_stub_flush(r->io);                                  \
	}

#endif /* TEST_RAFT_H */
