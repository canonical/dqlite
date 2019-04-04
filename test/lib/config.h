/**
 * Options object for tests.
 */

#ifndef TEST_OPTIONS_H
#define TEST_OPTIONS_H

#include "../../src/config.h"

#define FIXTURE_OPTIONS struct config options;

#define SETUP_OPTIONS                                   \
	{                                               \
		int rc;                                 \
		rc = config__init(&f->options, 1, "1"); \
		munit_assert_int(rc, ==, 0);            \
	}

#define TEAR_DOWN_OPTIONS config__close(&f->options)

#endif /* TEST_OPTIONS_H */
