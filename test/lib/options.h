/**
 * Options object for tests.
 */

#ifndef TEST_OPTIONS_H
#define TEST_OPTIONS_H

#include "../../src/options.h"

#define OPTIONS_FIXTURE struct options options;
#define OPTIONS_SETUP                                               \
	{                                                           \
		int rc;                                             \
		options__init(&f->options);                         \
		rc = options__set_vfs(&f->options, "test");         \
		munit_assert_int(rc, ==, 0);                        \
		rc = options__set_replication(&f->options, "test"); \
		munit_assert_int(rc, ==, 0);                        \
	}
#define OPTIONS_TEAR_DOWN options__close(&f->options);

#endif /* TEST_OPTIONS_H */
