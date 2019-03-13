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

#endif /* TEST_RAFT_H */
