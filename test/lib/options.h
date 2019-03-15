/**
 * Options object for tests.
 */

#ifndef TEST_OPTIONS_H
#define TEST_OPTIONS_H

#include "../../src/options.h"

#define FIXTURE_OPTIONS struct options options;

#define SETUP_OPTIONS SETUP_OPTIONS_X(f, "test")
#define TEAR_DOWN_OPTIONS TEAR_DOWN_OPTIONS_X(f)

#define SETUP_OPTIONS_X(F, NAME)                                  \
	{                                                         \
		int rc;                                           \
		options__init(&F->options);                       \
		rc = options__set_vfs(&F->options, NAME);         \
		munit_assert_int(rc, ==, 0);                      \
		rc = options__set_replication(&F->options, NAME); \
		munit_assert_int(rc, ==, 0);                      \
	}

#define TEAR_DOWN_OPTIONS_X(F) options__close(&F->options);

#endif /* TEST_OPTIONS_H */
