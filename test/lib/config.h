/**
 * Options object for tests.
 */

#ifndef TEST_OPTIONS_H
#define TEST_OPTIONS_H

#include "../../src/config.h"

#define FIXTURE_CONFIG struct config config;

#define SETUP_CONFIG                                   \
	{                                              \
		int rc;                                \
		rc = config__init(&f->config, 1, "1"); \
		munit_assert_int(rc, ==, 0);           \
	}

#define TEAR_DOWN_CONFIG config__close(&f->config)

#endif /* TEST_OPTIONS_H */
