/**
 * Options object for tests.
 */

#ifndef TEST_OPTIONS_H
#define TEST_OPTIONS_H

#include "../../src/config.h"

#define FIXTURE_OPTIONS struct config options;

#define SETUP_OPTIONS                                              \
	{                                                          \
		int rc;                                            \
		config__init(&f->options, 1, "1");                 \
		rc = config__set_vfs(&f->options, "test");         \
		munit_assert_int(rc, ==, 0);                       \
		rc = config__set_replication(&f->options, "test"); \
		munit_assert_int(rc, ==, 0);                       \
	}

#define TEAR_DOWN_OPTIONS config__close(&f->options)

#endif /* TEST_OPTIONS_H */
