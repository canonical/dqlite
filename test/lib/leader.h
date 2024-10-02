/**
 * Setup a test struct leader object.
 */

#ifndef TEST_LEADER_H
#define TEST_LEADER_H

#include "../../src/leader.h"
#include "../../src/registry.h"

#define FIXTURE_LEADER struct leader leader
#define SETUP_LEADER                                                 \
	{                                                            \
		struct db *db;                                       \
		int rv;                                              \
		rv = registry__db_get(&f->registry, "test.db", &db); \
		munit_assert_int(rv, ==, 0);                         \
		rv = leader_init(&f->leader, db, 0, &f->raft); \
		munit_assert_int(rv, ==, 0);                         \
	}
#define TEAR_DOWN_LEADER leader__close(&f->leader)

#endif /* TEST_LEADER_H */
