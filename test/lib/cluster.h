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
};

#define FIXTURE_CLUSTER struct server_fixture servers[N_SERVERS];

#define SETUP_CLUSTER                                              \
	{                                                          \
		int i;                                             \
		for (i = 0; i < N_SERVERS; i++) {                  \
			struct server_fixture *s = &f->servers[i]; \
			unsigned id = i + 1;                       \
			char address[8];                           \
			char name[8];                              \
			sprintf(address, "%d", id);                \
			sprintf(name, "test%d", i);                \
			SETUP_LOGGER_X(s);                         \
			SETUP_VFS_X(s, name);                      \
			SETUP_OPTIONS_X(s, name);                  \
			SETUP_REGISTRY_X(s);                       \
			SETUP_RAFT_X(s, id, address);              \
			SETUP_REPLICATION_X(s, name);              \
			RAFT_BOOTSTRAP(s, N_SERVERS);              \
			RAFT_START(s);                             \
			SETUP_LEADER_X(s);                         \
			SETUP_STMT_X(s);                           \
		}                                                  \
		CLUSTER_ELECT(0);                                  \
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

/* Flush all pending I/O. Disk writes will complete and network messages
 * delivered. */
#define CLUSTER_FLUSH                                                  \
	{                                                              \
		int i;                                                 \
		for (i = 0; i < N_SERVERS; i++) {                      \
			struct server_fixture *s = &f->servers[i];     \
			struct raft_message *messages;                 \
			unsigned n;                                    \
			unsigned j;                                    \
			RAFT_FLUSH(s);                                 \
			raft_io_stub_sent(&s->raft_io, &messages, &n); \
			for (j = 0; j < n; j++) {                      \
				CLUSTER_DELIVER(s, &messages[j]);      \
			}                                              \
		}                                                      \
	}

/* Deliver a message */
#define CLUSTER_DELIVER(S, M)                                              \
	struct server_fixture *r = &f->servers[(M)->server_id - 1];        \
	struct raft_message message = *(M);                                \
	message.server_id = S->raft.id;                                    \
	message.server_address = S->raft.address;                          \
	switch ((M)->type) {                                               \
		case RAFT_IO_APPEND_ENTRIES:                               \
			/* Make a copy of the entries being sent */        \
			raft_copy_entries((M)->append_entries.entries,     \
					  &message.append_entries.entries, \
					  (M)->append_entries.n_entries);  \
			message.append_entries.n_entries =                 \
			    (M)->append_entries.n_entries;                 \
			break;                                             \
	}                                                                  \
	raft_io_stub_dispatch(r->raft.io, &message);\

#define CLUSTER_ELECT(I)                                                 \
	{                                                                \
		struct server_fixture *s = &f->servers[I];               \
		RAFT_BECOME_CANDIDATE(s);                                \
		CLUSTER_FLUSH;                                           \
		CLUSTER_FLUSH;                                           \
		munit_assert_int(raft_state(&s->raft), ==, RAFT_LEADER); \
	}

#endif /* TEST_CLUSTER_H */
