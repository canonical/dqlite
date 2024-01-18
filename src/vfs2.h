#ifndef DQLITE_VFS2_H
#define DQLITE_VFS2_H

#include <sqlite3.h>

sqlite3_vfs *vfs2_make(sqlite3_vfs *orig);

#endif
