/**
 * Helpers for setting up a standalone raft instance with a libuv transport.
 */

#ifndef TEST_RAFT_H
#define TEST_RAFT_H

#include <raft.h>
#include <raft/uv.h>
#include <uv.h>

#include "../../src/fsm.h"
#include "../../src/transport.h"
#include "fs.h"
#include "logger.h"
#include "munit.h"
#include "uv.h"

#define FIXTURE_RAFT                             \
	char *dir;                               \
	struct uv_loop_s loop;                   \
	struct raft_uv_transport raft_transport; \
	struct raft_io raft_io;                  \
	struct raft_fsm fsm;                     \
	struct raft raft

#define SETUP_RAFT                                                       \
	{                                                                \
		int rv2;                                                 \
		f->dir = test_dir_setup();                               \
		test_uv_setup(params, &f->loop);                         \
		rv2 = raftProxyInit(&f->raft_transport, &f->loop);       \
		munit_assert_int(rv2, ==, 0);                            \
		rv2 = raft_uv_init(&f->raft_io, &f->loop, f->dir,        \
				   &f->raft_transport);                  \
		munit_assert_int(rv2, ==, 0);                            \
		rv2 = fsm__init(&f->fsm, &f->config, &f->registry);      \
		munit_assert_int(rv2, ==, 0);                            \
		rv2 = raft_init(&f->raft, &f->raft_io, &f->fsm, 1, "1"); \
		munit_assert_int(rv2, ==, 0);                            \
	}

#define TEAR_DOWN_RAFT                              \
	{                                           \
		raft_close(&f->raft, NULL);         \
		test_uv_stop(&f->loop);             \
		raft_uv_close(&f->raft_io);         \
		fsm__close(&f->fsm);                \
		test_uv_tear_down(&f->loop);        \
		raftProxyClose(&f->raft_transport); \
		test_dir_tear_down(f->dir);         \
	}

/**
 * Bootstrap the fixture raft instance with a configuration containing only
 * itself.
 */
#define RAFT_BOOTSTRAP                                               \
	{                                                            \
		struct raft_configuration configuration;             \
		int rv2;                                             \
		raft_configuration_init(&configuration);             \
		rv2 = raft_configuration_add(&configuration, 1, "1", \
					     RAFT_VOTER);            \
		munit_assert_int(rv2, ==, 0);                        \
		rv2 = raft_bootstrap(&f->raft, &configuration);      \
		munit_assert_int(rv2, ==, 0);                        \
		raft_configuration_close(&configuration);            \
	}

#define RAFT_START                            \
	{                                     \
		int rv2;                      \
		rv2 = raft_start(&f->raft);   \
		munit_assert_int(rv2, ==, 0); \
	}

#endif /* TEST_RAFT_H */
