/**
 * Helpers for using a raft instance in test fixtures.
 */

#ifndef TEST_RAFT_H
#define TEST_RAFT_H

#include <raft.h>
#include <raft/io_stub.h>

#include "munit.h"

/**
 * Fields common to all fixtures setting up a raft instance.
 */
#define FIXTURE_RAFT                    \
	struct raft_logger raft_logger; \
	struct raft_io raft_io;         \
	struct raft_fsm fsm;            \
	struct raft raft

/**
 * Setup the raft instance of a fixture.
 */
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
	}

#define TEAR_DOWN_RAFT                           \
	{                                        \
		raft_close(&f->raft, NULL);      \
		raft_io_stub_close(&f->raft_io); \
	}

#endif /* TEST_RAFT_H */
