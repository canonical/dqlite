/**
 * Setup a test struct leader object.
 */

#ifndef TEST_LEADER_H
#define TEST_LEADER_H

#include "../../src/leader.h"
#include "../../src/registry.h"

#define FIXTURE_LEADER struct leader leader
#define SETUP_LEADER SETUP_LEADER_X(f)
#define TEAR_DOWN_LEADER TEAR_DOWN_LEADER_X(f)

#define SETUP_LEADER_X(F)                                            \
	{                                                            \
		struct db *db;                                       \
		int rc;                                              \
		rc = registry__db_get(&F->registry, "test.db", &db); \
		munit_assert_int(rc, ==, 0);                         \
		leader__init(&F->leader, db);                        \
	}
#define TEAR_DOWN_LEADER_X(F) leader__close(&F->leader)

#endif /* TEST_LEADER_H */
