/**
 * Helpers to setup a raft cluster in test fixtures.
 */

#ifndef TEST_CLUSTER_H
#define TEST_CLUSTER_H

#include <raft.h>
#include <raft/fixture.h>

#include "../lib/leader.h"
#include "../lib/logger.h"
#include "../lib/options.h"
#include "../lib/raft.h"
#include "../lib/registry.h"
#include "../lib/replication.h"
#include "../lib/stmt.h"
#include "../lib/vfs.h"

#define N_SERVERS 2

struct server_fixture
{
	char name[8];
	FIXTURE_LOGGER;
	FIXTURE_VFS;
	FIXTURE_OPTIONS;
	FIXTURE_REGISTRY;
	FIXTURE_LEADER;
	sqlite3_wal_replication replication;
};

#define FIXTURE_CLUSTER                           \
	struct server_fixture servers[N_SERVERS]; \
	struct raft_fsm fsms[N_SERVERS];          \
	struct raft_fixture cluster;              \
	FIXTURE_STMT

#define SETUP_CLUSTER                                                          \
	{                                                                      \
		unsigned i;                                                    \
		int rc;                                                        \
		rc = raft_fixture_setup(&f->cluster, N_SERVERS, N_SERVERS,     \
					f->fsms, munit_rand_int_range);        \
		munit_assert_int(rc, ==, 0);                                   \
		for (i = 0; i < N_SERVERS; i++) {                              \
			struct server_fixture *s = &f->servers[i];             \
			struct raft *raft = raft_fixture_get(&f->cluster, i);  \
			sprintf(s->name, "test%d", i);                         \
			SETUP_LOGGER_X(s);                                     \
			SETUP_VFS_X(s, s->name);                               \
			SETUP_OPTIONS_X(s, s->name);                           \
			SETUP_REGISTRY_X(s);                                   \
			rc = fsm__init(&f->fsms[i], &s->logger, &s->registry); \
			munit_assert_int(rc, ==, 0);                           \
			rc = replication__init(&s->replication, &s->logger,    \
					       raft);                          \
			s->replication.zName = s->name;                        \
			munit_assert_int(rc, ==, 0);                           \
			sqlite3_wal_replication_register(&s->replication, 0);  \
			SETUP_LEADER_X(s);                                     \
		}                                                              \
	}

#define TEAR_DOWN_CLUSTER                                                    \
	{                                                                    \
		int i;                                                       \
		for (i = 0; i < N_SERVERS; i++) {                            \
			struct server_fixture *s = &f->servers[i];           \
			TEAR_DOWN_LEADER_X(s);                               \
			sqlite3_wal_replication_unregister(&s->replication); \
			replication__close(&s->replication);                 \
			fsm__close(&f->fsms[i]);                             \
			TEAR_DOWN_REGISTRY_X(s);                             \
			TEAR_DOWN_OPTIONS_X(s);                              \
			TEAR_DOWN_VFS_X(s);                                  \
			TEAR_DOWN_LOGGER_X(s);                               \
		}                                                            \
		raft_fixture_tear_down(&f->cluster);                         \
	}

#define CLUSTER_LEADER(I) &f->servers[I].leader
#define CLUSTER_REGISTRY(I) &f->servers[I].registry

#define CLUSTER_ELECT(I) raft_fixture_elect(&f->cluster, I)
#define CLUSTER_APPLIED(N) raft_fixture_wait_applied(&f->cluster, N);

#define CLUSTER_STEP raft_fixture_step(&f->cluster)

#endif /* TEST_CLUSTER_H */
