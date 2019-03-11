/**
 * Setup an in-memory VFS instance to use in tests.
 */

#ifndef TEST_VFS_H
#define TEST_VFS_H

#include "../../include/dqlite.h"

#define VFS_FIXTURE sqlite3_vfs *vfs;

#define VFS_SETUP                                       \
	f->vfs = dqlite_vfs_create("test", &f->logger); \
	munit_assert_ptr_not_null(f->vfs);              \
	sqlite3_vfs_register(f->vfs, 0);

#define VFS_TEAR_DOWN                   \
	sqlite3_vfs_unregister(f->vfs); \
	dqlite_vfs_destroy(f->vfs);

#endif /* TEST_VFS_H */
