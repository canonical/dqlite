/**
 * Setup an in-memory VFS instance to use in tests.
 */

#ifndef TEST_VFS_H
#define TEST_VFS_H

#include "../../include/dqlite.h"

#define FIXTURE_VFS sqlite3_vfs *vfs;
#define SETUP_VFS SETUP_VFS_X(f, "test")
#define TEAR_DOWN_VFS TEAR_DOWN_VFS_X(f)

#define SETUP_VFS_X(F, NAME)                          \
	F->vfs = dqlite_vfs_create(NAME, &F->logger); \
	munit_assert_ptr_not_null(F->vfs);            \
	sqlite3_vfs_register(F->vfs, 0);

#define TEAR_DOWN_VFS_X(F)              \
	sqlite3_vfs_unregister(F->vfs); \
	dqlite_vfs_destroy(F->vfs);

#endif /* TEST_VFS_H */
