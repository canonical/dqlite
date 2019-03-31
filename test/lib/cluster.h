/**
 * Helpers to setup a raft cluster in test fixtures.
 *
 * Each raft instance will use its own dqlite FSM, which in turn will be created
 * using its own registry, options and logger.
 *
 * The fixture will also register a VFS and a SQLite replication object for each
 * raft instance, using "test<i>" as registration name, where <i> is the raft
 * instance index.
 *
 * This fixture is meant to be used as base-line fixture for most higher-level
 * tests.
 */

#ifndef TEST_CLUSTER_H
#define TEST_CLUSTER_H

#include <raft.h>
#include <raft/fixture.h>

#include "../../src/fsm.h"
#include "../../src/options.h"
#include "../../src/registry.h"
#include "../../src/replication.h"
#include "../../src/vfs.h"

#include "../lib/heap.h"
#include "../lib/logger.h"
#include "../lib/sqlite.h"

#define N_SERVERS 3

struct server
{
	char name[8];
	struct dqlite_logger logger;
	sqlite3_vfs *vfs;
	struct options options;
	struct registry registry;
	sqlite3_wal_replication replication;
};

#define FIXTURE_CLUSTER                   \
	struct server servers[N_SERVERS]; \
	struct raft_fsm fsms[N_SERVERS];  \
	struct raft_fixture cluster;

#define SETUP_CLUSTER                                                     \
	{                                                                 \
		struct raft_configuration configuration;                  \
		unsigned i;                                               \
		int rc;                                                   \
		SETUP_HEAP;                                               \
		SETUP_SQLITE;                                             \
		rc = raft_fixture_init(&f->cluster, N_SERVERS, f->fsms);  \
		munit_assert_int(rc, ==, 0);                              \
		for (i = 0; i < N_SERVERS; i++) {                         \
			SETUP_SERVER(i);                                  \
		}                                                         \
		rc = raft_fixture_configuration(&f->cluster, N_SERVERS,   \
						&configuration);          \
		munit_assert_int(rc, ==, 0);                              \
		rc = raft_fixture_bootstrap(&f->cluster, &configuration); \
		munit_assert_int(rc, ==, 0);                              \
		raft_configuration_close(&configuration);                 \
		rc = raft_fixture_start(&f->cluster);                     \
		munit_assert_int(rc, ==, 0);                              \
	}

#define SETUP_SERVER(I)                                                        \
	{                                                                      \
		struct server *s = &f->servers[I];                             \
		struct raft_fsm *fsm = &f->fsms[I];                            \
		struct raft *raft = raft_fixture_get(&f->cluster, I);          \
		int rc;                                                        \
                                                                               \
		raft_fixture_set_random(&f->cluster, I, munit_rand_int_range); \
                                                                               \
		test_logger_setup(params, &s->logger);                         \
                                                                               \
		sprintf(s->name, "test%d", I);                                 \
                                                                               \
		s->vfs = dqlite_vfs_create(s->name, &s->logger);               \
		munit_assert_ptr_not_null(s->vfs);                             \
		sqlite3_vfs_register(s->vfs, 0);                               \
                                                                               \
		options__init(&s->options);                                    \
		rc = options__set_vfs(&s->options, s->name);                   \
		munit_assert_int(rc, ==, 0);                                   \
		rc = options__set_replication(&s->options, s->name);           \
		munit_assert_int(rc, ==, 0);                                   \
                                                                               \
		registry__init(&s->registry, &s->options);                     \
                                                                               \
		rc = fsm__init(fsm, &s->logger, &s->registry);                 \
		munit_assert_int(rc, ==, 0);                                   \
                                                                               \
		rc = replication__init(&s->replication, &s->logger, raft);     \
		s->replication.zName = s->name;                                \
		munit_assert_int(rc, ==, 0);                                   \
		sqlite3_wal_replication_register(&s->replication, 0);          \
	}

#define TEAR_DOWN_CLUSTER                         \
	{                                         \
		int i;                            \
		for (i = 0; i < N_SERVERS; i++) { \
			TEAR_DOWN_SERVER(i);      \
		}                                 \
		raft_fixture_close(&f->cluster);  \
		TEAR_DOWN_SQLITE;                 \
		TEAR_DOWN_HEAP;                   \
	}

#define TEAR_DOWN_SERVER(I)                                          \
	{                                                            \
		struct server *s = &f->servers[I];                   \
		struct raft_fsm *fsm = &f->fsms[I];                  \
		sqlite3_wal_replication_unregister(&s->replication); \
		replication__close(&s->replication);                 \
		fsm__close(fsm);                                     \
		registry__close(&s->registry);                       \
		options__close(&s->options);                         \
		sqlite3_vfs_unregister(s->vfs);                      \
		dqlite_vfs_destroy(s->vfs);                          \
		test_logger_tear_down(&s->logger);                   \
	}

#define CLUSTER_LEADER(I) &f->servers[I].leader
#define CLUSTER_REGISTRY(I) &f->servers[I].registry

#define CLUSTER_ELECT(I) raft_fixture_elect(&f->cluster, I)
#define CLUSTER_APPLIED(N)                                                     \
	{                                                                      \
		int i;                                                         \
		for (i = 0; i < N_SERVERS; i++) {                              \
			bool done;                                             \
			done = raft_fixture_step_until_applied(&f->cluster, i, \
							       N, 1000);       \
			munit_assert_true(done);                               \
		}                                                              \
	}

#define CLUSTER_STEP raft_fixture_step(&f->cluster)

#endif /* TEST_CLUSTER_H */
