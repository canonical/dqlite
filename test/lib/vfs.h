/**
 * Setup an in-memory VFS instance to use in tests.
 */

#ifndef TEST_VFS_H
#define TEST_VFS_H

#include "../../include/dqlite.h"

#define VFS_FIXTURE sqlite3_vfs *vfs;

#define VFS_SETUP                                           \
	f->vfs = dqlite_vfs_create("volatile", &f->logger); \
	munit_assert_ptr_not_null(f->vfs);

#define VFS_TEAR_DOWN dqlite_vfs_destroy(f->vfs);

#endif /* TEST_VFS_H */
