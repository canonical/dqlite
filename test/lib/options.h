/**
 * Options object for tests.
 */

#ifndef TEST_OPTIONS_H
#define TEST_OPTIONS_H

#include "../../src/options.h"

#define FIXTURE_OPTIONS struct options options;

#define SETUP_OPTIONS                                               \
	{                                                           \
		int rc;                                             \
		options__init(&f->options);                         \
		rc = options__set_vfs(&f->options, "test");         \
		munit_assert_int(rc, ==, 0);                        \
		rc = options__set_replication(&f->options, "test"); \
		munit_assert_int(rc, ==, 0);                        \
	}

#define TEAR_DOWN_OPTIONS options__close(&f->options)

#endif /* TEST_OPTIONS_H */
