#ifndef DQLITE_VFS_H_
#define DQLITE_VFS_H_

#include "../include/dqlite.h"

/**
 * Initialize the given SQLite VFS interface with dqlite's in-memory
 * implementation.
 */
int vfs__init(struct sqlite3_vfs *vfs, struct dqlite_logger *logger);

/**
 * Release all memory associated with the given dqlite in-memory VFS
 * implementation.
 */
void vfs__close(struct sqlite3_vfs *vfs);

#endif /* DQLITE_VFS_H_ */
