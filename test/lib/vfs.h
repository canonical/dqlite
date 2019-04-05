/**
 * Setup an in-memory VFS instance to use in tests.
 */

#ifndef TEST_VFS_H
#define TEST_VFS_H

#include "../../src/vfs.h"

#define FIXTURE_VFS struct sqlite3_vfs vfs;
#define SETUP_VFS                                     \
	{                                             \
		int rv2;                              \
		rv2 = vfs__init(&f->vfs, &f->config); \
		munit_assert_int(rv2, ==, 0);         \
	}

#define TEAR_DOWN_VFS vfs__close(&f->vfs);

#endif /* TEST_VFS_H */
