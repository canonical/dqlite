/**
 * Helpers to setup a raft cluster in test fixtures.
 */

#ifndef TEST_CLUSTER_H
#define TEST_CLUSTER_H

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
	FIXTURE_LOGGER;
	FIXTURE_VFS;
	FIXTURE_OPTIONS;
	FIXTURE_REGISTRY;
	FIXTURE_RAFT;
	FIXTURE_REPLICATION;
	FIXTURE_LEADER;
	FIXTURE_STMT;
	char address[8];
	char name[8];
};

#define FIXTURE_CLUSTER                           \
	struct server_fixture servers[N_SERVERS]; \
	int leader_index;                         \
	struct leader *leader;                    \
	sqlite3_stmt *stmt;

#define SETUP_CLUSTER                                                       \
	{                                                                   \
		int i;                                                      \
		int j;                                                      \
		for (i = 0; i < N_SERVERS; i++) {                           \
			struct server_fixture *s = &f->servers[i];          \
			unsigned id = i + 1;                                \
			sprintf(s->address, "%d", id);                      \
			sprintf(s->name, "test%d", i);                      \
			SETUP_LOGGER_X(s);                                  \
			SETUP_VFS_X(s, s->name);                            \
			SETUP_OPTIONS_X(s, s->name);                        \
			SETUP_REGISTRY_X(s);                                \
			SETUP_RAFT_X(s, id, s->address);                    \
			SETUP_REPLICATION_X(s, s->name);                    \
			RAFT_BOOTSTRAP(s, N_SERVERS);                       \
			RAFT_START(s);                                      \
			SETUP_LEADER_X(s);                                  \
			SETUP_STMT_X(s);                                    \
		}                                                           \
		for (i = 0; i < N_SERVERS; i++) {                           \
			for (j = 0; j < N_SERVERS; j++) {                   \
				struct server_fixture *s1 = &f->servers[i]; \
				struct server_fixture *s2 = &f->servers[j]; \
				if (i == j) {                               \
					continue;                           \
				}                                           \
				RAFT_CONNECT(s1, s2);                       \
			}                                                   \
		}                                                           \
		f->leader = NULL;                                           \
		CLUSTER_ELECT;                                              \
	}

#define TEAR_DOWN_CLUSTER                                          \
	{                                                          \
		int i;                                             \
		for (i = 0; i < N_SERVERS; i++) {                  \
			struct server_fixture *s = &f->servers[i]; \
			TEAR_DOWN_STMT_X(s);                       \
			TEAR_DOWN_LEADER_X(s);                     \
			TEAR_DOWN_REPLICATION_X(s);                \
			TEAR_DOWN_RAFT_X(s);                       \
			TEAR_DOWN_REGISTRY_X(s);                   \
			TEAR_DOWN_OPTIONS_X(s);                    \
			TEAR_DOWN_VFS_X(s);                        \
			TEAR_DOWN_LOGGER_X(s);                     \
		}                                                  \
	}

#define CLUSTER_STEP                                                           \
	{                                                                      \
		int deliver_timeout = -1;                                      \
		int raft_timeout = -1;                                         \
		int timeout;                                                   \
		int i;                                                         \
		for (i = 0; i < N_SERVERS; i++) {                              \
			struct server_fixture *s = &f->servers[i];             \
			int timeout;                                           \
			timeout =                                              \
			    raft_io_stub_next_deliver_timeout(&s->raft_io);    \
			if (timeout != -1) {                                   \
				if (deliver_timeout == -1 ||                   \
				    timeout < deliver_timeout) {               \
					deliver_timeout = timeout;             \
				}                                              \
			}                                                      \
			timeout = raft_next_timeout(&s->raft);                 \
			if (raft_timeout == -1 || timeout < raft_timeout) {    \
				raft_timeout = timeout;                        \
			}                                                      \
		}                                                              \
		if (deliver_timeout != -1 && deliver_timeout < raft_timeout) { \
			timeout = deliver_timeout;                             \
		} else {                                                       \
			timeout = raft_timeout;                                \
		}                                                              \
		CLUSTER_ADVANCE(timeout + 1);                                  \
		CLUSTER_FLUSH;                                                 \
	}

/* Flush all pending I/O. Disk writes will complete and network messages
 * delivered. */
#define CLUSTER_FLUSH                                              \
	{                                                          \
		int i;                                             \
		for (i = 0; i < N_SERVERS; i++) {                  \
			struct server_fixture *s = &f->servers[i]; \
			RAFT_FLUSH(s);                             \
		}                                                  \
	}

#define CLUSTER_ADVANCE(MSECS)                                     \
	{                                                          \
		int i;                                             \
		for (i = 0; i < N_SERVERS; i++) {                  \
			struct server_fixture *s = &f->servers[i]; \
			raft_io_stub_advance(&s->raft_io, MSECS);  \
		}                                                  \
	}

#define CLUSTER_ELECT                                                      \
	{                                                                  \
		while (f->leader == NULL) {                                \
			CLUSTER_STEP;                                      \
			for (i = 0; i < N_SERVERS; i++) {                  \
				struct server_fixture *s = &f->servers[i]; \
				if (raft_state(&s->raft) == RAFT_LEADER) { \
					f->leader_index = i;               \
					f->leader = &s->leader;            \
					f->stmt = s->stmt;                 \
					break;                             \
				}                                          \
			}                                                  \
		}                                                          \
	}

#endif /* TEST_CLUSTER_H */
