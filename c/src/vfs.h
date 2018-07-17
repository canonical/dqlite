#ifndef DQLITE_VFS_H
#define DQLITE_VFS_H

#include <stdint.h>
#include <stdlib.h>

#include <sqlite3.h>

int dqlite_vfs_register(const char *name, sqlite3_vfs **out);
void dqlite_vfs_unregister(sqlite3_vfs* vfs);

int dqlite_vfs_content(
	sqlite3_vfs* vfs,
	const char *filename,
	uint8_t **buf,
	size_t *len
	);

#endif /* DQLITE_VFS_H */
