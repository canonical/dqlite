#ifndef DQLITE_VFS2_H
#define DQLITE_VFS2_H

#include <sqlite3.h>

sqlite3_vfs *vfs2_make(sqlite3_vfs *orig);

int vfs2_apply(sqlite3_file *file);
#endif
