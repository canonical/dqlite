#ifndef DQLITE_FILE_H_
#define DQLITE_FILE_H_

#include <stdint.h>
#include <unistd.h>

/* Read the content of a file, using the VFS implementation registered under the
 * given name. Used to take database snapshots using the dqlite in-memory
 * VFS. */
int dqlite_file_read(const char *vfs_name,
		     const char *filename,
		     uint8_t **buf,
		     size_t *len);

/* Write the content of a file, using the VFS implementation registered under
 * the given name. Used to restore database snapshots against the dqlite
 * in-memory VFS. If the file already exists, it's overwritten. */
int dqlite_file_write(const char *vfs_name,
		      const char *filename,
		      uint8_t *buf,
		      size_t len);

#endif /* DQLITE_FILE_H_ */
