#ifndef DQLITE_VFS_H_
#define DQLITE_VFS_H_

#include "../include/dqlite.h"

/* Allocate and initialize an in-memory dqlite VFS object, configured with the
 * given registration name.
 *
 * A copy of the provided name will be made, so clients can free it after the
 * function returns. */
sqlite3_vfs *dqlite_vfs_create(const char *name, dqlite_logger *logger);

/* Destroy and deallocate an in-memory dqlite VFS object. */
void dqlite_vfs_destroy(sqlite3_vfs *vfs);


#endif /* DQLITE_VFS_H_ */
