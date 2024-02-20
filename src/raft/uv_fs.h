/* File system related utilities. */

#ifndef UV_FS_H_
#define UV_FS_H_

#include <uv.h>

#include "../raft.h"
#include "err.h"

#define TMP_FILE_PREFIX "tmp-"
#define TMP_FILE_FMT TMP_FILE_PREFIX "%s"

/* Check that the given directory can be used. */
int UvFsCheckDir(const char *dir, char *errmsg);

/* Sync the given directory by calling fsync(). */
int UvFsSyncDir(const char *dir, char *errmsg);

/* Check whether a the given file exists. */
int UvFsFileExists(const char *dir,
		   const char *filename,
		   bool *exists,
		   char *errmsg);

/* Get the size of the given file. */
int UvFsFileSize(const char *dir,
		 const char *filename,
		 off_t *size,
		 char *errmsg);

/* Check whether the given file in the given directory is empty. */
int UvFsFileIsEmpty(const char *dir,
		    const char *filename,
		    bool *empty,
		    char *errmsg);

/* Create the given file in the given directory and allocate the given size to
 * it, returning its file descriptor. The file must not exist yet. */
int UvFsAllocateFile(const char *dir,
		     const char *filename,
		     size_t size,
		     uv_file *fd,
		     bool fallocate,
		     char *errmsg);

/* Create a file and write the given content into it. */
int UvFsMakeFile(const char *dir,
		 const char *filename,
		 struct raft_buffer *bufs,
		 unsigned n_bufs,
		 char *errmsg);

/* Create or overwrite a file.
 *
 * If the file does not exists yet, it gets created, the given content written
 * to it, and then fully persisted to disk by fsync()'ing the file and the
 * dir.
 *
 * If the file already exists, it gets overwritten. The assumption is that the
 * file size will stay the same and its content will change, so only fdatasync()
 * will be used */
int UvFsMakeOrOverwriteFile(const char *dir,
			    const char *filename,
			    const struct raft_buffer *buf,
			    char *errmsg);

/* Open a file for reading. */
int UvFsOpenFileForReading(const char *dir,
			   const char *filename,
			   uv_file *fd,
			   char *errmsg);

/* Read exactly buf->len bytes from the given file descriptor into
   buf->base. Fail if less than buf->len bytes are read. */
int UvFsReadInto(uv_file fd, struct raft_buffer *buf, char *errmsg);

/* Read all the content of the given file. */
int UvFsReadFile(const char *dir,
		 const char *filename,
		 struct raft_buffer *buf,
		 char *errmsg);

/* Read exactly buf->len bytes from the given file into buf->base. Fail if less
 * than buf->len bytes are read. */
int UvFsReadFileInto(const char *dir,
		     const char *filename,
		     struct raft_buffer *buf,
		     char *errmsg);

/* Synchronously remove a file, calling the unlink() system call. */
int UvFsRemoveFile(const char *dir, const char *filename, char *errmsg);

/* Synchronously truncate a file to the given size and then rename it. */
int UvFsTruncateAndRenameFile(const char *dir,
			      size_t size,
			      const char *filename1,
			      const char *filename2,
			      char *errmsg);

/* Synchronously rename a file. */
int UvFsRenameFile(const char *dir,
		   const char *filename1,
		   const char *filename2,
		   char *errmsg);

/* Return information about the I/O capabilities of the underlying file
 * system.
 *
 * The @direct parameter will be set to zero if direct I/O is not possible, or
 * to the block size to use for direct I/O otherwise.
 *
 * The @async parameter will be set to true if fully asynchronous I/O is
 * possible using the KAIO API. */
int UvFsProbeCapabilities(const char *dir,
			  size_t *direct,
			  bool *async,
			  bool *fallocate,
			  char *errmsg);

#endif /* UV_FS_H_ */
