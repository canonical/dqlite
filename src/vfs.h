#ifndef VFS_H_
#define VFS_H_

#include <sqlite3.h>

#include "config.h"

/* Initialize the given SQLite VFS interface with dqlite's in-memory
 * implementation.
 *
 * This function also automatically register the implementation in the global
 * SQLite registry, using the given @name. */
int VfsInit(struct sqlite3_vfs *vfs, const char *name);

/* Release all memory associated with the given dqlite in-memory VFS
 * implementation.
 *
 * This function also automatically unregister the implementation from the
 * SQLite global registry. */
void VfsClose(struct sqlite3_vfs *vfs);

/* Read the content of a file, using the VFS implementation registered under the
 * given name. Used to take database snapshots using the dqlite in-memory
 * VFS. */
int VfsFileRead(const char *vfs_name,
		const char *filename,
		void **buf,
		size_t *len);

/* Write the content of a file, using the VFS implementation registered under
 * the given name. Used to restore database snapshots against the dqlite
 * in-memory VFS. If the file already exists, it's overwritten. */
int VfsFileWrite(const char *vfs_name,
		 const char *filename,
		 const void *buf,
		 size_t len);

#endif /* VFS_H_ */
