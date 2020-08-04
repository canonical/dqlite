#include "../include/dqlite.h"

#include "vfs.h"

int dqlite_vfs_init(sqlite3_vfs *vfs, const char *name)
{
	return VfsInitV2(vfs, name);
}

void dqlite_vfs_close(sqlite3_vfs *vfs)
{
	VfsClose(vfs);
}

int dqlite_vfs_poll(sqlite3_vfs *vfs,
		    const char *filename,
		    dqlite_vfs_frame **frames,
		    unsigned *n)
{
	return VfsPoll(vfs, filename, frames, n);
}

int dqlite_vfs_commit(sqlite3_vfs *vfs,
		      const char *filename,
		      unsigned n,
		      unsigned long *page_numbers,
		      void *frames)
{
	return VfsCommit(vfs, filename, n, page_numbers, frames);
}

int dqlite_vfs_abort(sqlite3_vfs *vfs, const char *filename) {
	return VfsAbort(vfs, filename);
}
