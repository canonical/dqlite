#include "../include/dqlite.h"

#include "vfs.h"

int dqlite_version_number(void)
{
	return DQLITE_VERSION_NUMBER;
}

int dqlite_vfs_init(sqlite3_vfs *vfs, const char *name)
{
	return VfsInit(vfs, name);
}

int dqlite_vfs_enable_disk(sqlite3_vfs *vfs)
{
	return VfsEnableDisk(vfs);
}

void dqlite_vfs_close(sqlite3_vfs *vfs)
{
	VfsClose(vfs);
}

int dqlite_vfs_snapshot(sqlite3_vfs *vfs,
			const char *filename,
			void **data,
			size_t *n)
{
	return VfsSnapshot(vfs, filename, data, n);
}

int dqlite_vfs_snapshot_disk(sqlite3_vfs *vfs,
			     const char *filename,
			     struct dqlite_buffer bufs[],
			     unsigned n)
{
	return VfsSnapshotDisk(vfs, filename, bufs, n);
}

int dqlite_vfs_num_pages(sqlite3_vfs *vfs, const char *filename, unsigned *n)
{
	return VfsDatabaseNumPages(vfs, filename, false, n);
}

int dqlite_vfs_shallow_snapshot(sqlite3_vfs *vfs,
				const char *filename,
				struct dqlite_buffer bufs[],
				unsigned n)
{
	return VfsShallowSnapshot(vfs, filename, bufs, n);
}

int dqlite_vfs_restore(sqlite3_vfs *vfs,
		       const char *filename,
		       const void *data,
		       size_t n)
{
	return VfsRestore(vfs, filename, data, n);
}

int dqlite_vfs_restore_disk(sqlite3_vfs *vfs,
			    const char *filename,
			    const void *data,
			    size_t main_size,
			    size_t wal_size)
{
	return VfsDiskRestore(vfs, filename, data, main_size, wal_size);
}
