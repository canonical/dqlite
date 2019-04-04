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
		rv2 = vfs__init(&f->vfs, &f->logger); \
		munit_assert_int(rv2, ==, 0);         \
		f->vfs.zName = "test";                \
		sqlite3_vfs_register(&f->vfs, 0);     \
	}

#define TEAR_DOWN_VFS                    \
	sqlite3_vfs_unregister(&f->vfs); \
	vfs__close(&f->vfs);

#endif /* TEST_VFS_H */
