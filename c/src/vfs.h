#ifndef DQLITE_VFS_H
#define DQLITE_VFS_H

#include <sqlite3.h>

int dqlite_vfs_register(const char *name, sqlite3_vfs **out);
void dqlite_vfs_unregister(sqlite3_vfs* vfs);

#endif /* DQLITE_VFS_H */
