#ifndef DQLITE_VFS_H_
#define DQLITE_VFS_H_

#include "logger.h"

/**
 * Initialize the given SQLite VFS interface with dqlite's in-memory
 * implementation.
 *
 * This function also automatically register the implementation in the global
 * SQLite registry, using the given @name.
 */
int vfs__init(struct sqlite3_vfs *vfs, const char *name, struct logger *logger);

/**
 * Release all memory associated with the given dqlite in-memory VFS
 * implementation.
 *
 * This function also automatically unregister the implementation from the
 * SQLite global registry.
 */
void vfs__close(struct sqlite3_vfs *vfs);

#endif /* DQLITE_VFS_H_ */
