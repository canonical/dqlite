/**
 * Helpers for using a raft instance in test fixtures.
 */

#ifndef TEST_RAFT_H
#define TEST_RAFT_H

#include <raft.h>
#include <raft/io_stub.h>

#include "../../src/fsm.h"

#include "munit.h"

#define FIXTURE_RAFT            \
	struct raft_io raft_io; \
	struct raft_fsm fsm;    \
	struct raft raft

#define SETUP_RAFT               \
	SETUP_RAFT_X(f, 1, "1"); \
	RAFT_BOOTSTRAP(f, 2);    \
	RAFT_START(f);

#define TEAR_DOWN_RAFT TEAR_DOWN_RAFT_X(f)

#define SETUP_RAFT_X(F, ID, ADDRESS)                                         \
	{                                                                    \
		int rv;                                                      \
		rv = raft_io_stub_init(&F->raft_io);                         \
		munit_assert_int(rv, ==, 0);                                 \
		raft_io_stub_set_random(&F->raft_io, munit_rand_int_range);  \
		rv = fsm__init(&F->fsm, &F->logger, &F->registry);           \
		munit_assert_int(rv, ==, 0);                                 \
		rv = raft_init(&F->raft, &F->raft_io, &F->fsm, ID, ADDRESS); \
		munit_assert_int(rv, ==, 0);                                 \
		F->raft.data = F;                                            \
	}

#define TEAR_DOWN_RAFT_X(F)                      \
	{                                        \
		raft_close(&F->raft, NULL);      \
		fsm__close(&F->fsm);             \
		raft_io_stub_close(&F->raft_io); \
	}

/**
 * Bootstrap the fixture raft instance.
 *
 * The initial configuration will have the given amount of servers and will be
 * saved as first entry in the log. The server IDs are assigned sequentially
 * starting from 1 up to @N_SERVERS. All servers will be voting servers.
 */
#define RAFT_BOOTSTRAP(F, N_SERVERS)                                    \
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
		rv = raft_bootstrap(&F->raft, &configuration);          \
		munit_assert_int(rv, ==, 0);                            \
		raft_configuration_close(&configuration);               \
	}

#define RAFT_START(F)                        \
	{                                    \
		int rc;                      \
		rc = raft_start(&F->raft);   \
		munit_assert_int(rc, ==, 0); \
	}

#define RAFT_FLUSH(F) raft_io_stub_flush_all(&F->raft_io)

#define RAFT_CONNECT(F1, F2) raft_io_stub_connect(&F1->raft_io, &F2->raft_io)

#define RAFT_BECOME_CANDIDATE(F)                                         \
	raft_io_stub_advance(&F->raft_io,                                \
			     F->raft.randomized_election_timeout + 100); \
	munit_assert_int(raft_state(&F->raft), ==, RAFT_CANDIDATE)

#define RAFT_BECOME_LEADER                                                  \
	{                                                                   \
		struct raft *r = &f->raft;                                  \
		size_t votes = r->configuration.n / 2;                      \
		size_t i;                                                   \
		RAFT_BECOME_CANDIDATE(f);                                   \
		RAFT_FLUSH(f);                                              \
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
			raft_io_stub_deliver(r->io, &message);              \
			votes--;                                            \
			if (votes == 0) {                                   \
				break;                                      \
			}                                                   \
		}                                                           \
		munit_assert_int(raft_state(r), ==, RAFT_LEADER);           \
		raft_io_stub_flush_all(r->io);                              \
	}

/* Reach a quorum for all outstanding entries. */
#define RAFT_COMMIT                                              \
	{                                                        \
		const char *address = "2";                       \
		struct raft_message message;                     \
		struct raft_append_entries_result *result =      \
		    &message.append_entries_result;              \
		raft_io_stub_flush_all(&f->raft_io);             \
		message.type = RAFT_IO_APPEND_ENTRIES_RESULT;    \
		message.server_id = 2;                           \
		message.server_address = address;                \
		result->term = f->raft.current_term;             \
		result->rejected = 0;                            \
		result->last_log_index =                         \
		    f->raft.leader_state.progress[1].next_index; \
		raft_io_stub_deliver(&f->raft_io, &message);     \
	}

void raft_copy_entries(const struct raft_entry *src,
		       struct raft_entry **dst,
		       unsigned n);

#endif /* TEST_RAFT_H */
