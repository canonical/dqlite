#ifndef _GNU_SOURCE
# define _GNU_SOURCE
#endif

#include <fcntl.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <sys/vfs.h>
#include <time.h>
#include <unistd.h>

#include "assert.h"
#include "err.h"
#include "heap.h"
#include "uv_fs.h"
#include "uv_os.h"

#ifdef HAVE_XFS_XFS_H
# include <xfs/xfs.h> 
# include <linux/magic.h> 
#endif

#ifdef LZ4_AVAILABLE
# include <lz4.h>
# include <lz4frame.h>
#endif

#define UV__FS_PROBE_FILE ".probe"
#define UV__FS_PROBE_FILE_SIZE 4096

int UvFsCheckDir(const char *dir, char *errmsg)
{
	struct uv_fs_s req;
	int rv;

	/* Make sure we have a directory we can write into. */
	rv = uv_fs_stat(NULL, &req, dir, NULL);
	if (rv != 0) {
		switch (rv) {
			case UV_ENOENT:
				ErrMsgPrintf((char *)errmsg,
					     "directory '%s' does not exist",
					     dir);
				return RAFT_NOTFOUND;
			case UV_EACCES:
				ErrMsgPrintf((char *)errmsg,
					     "can't access directory '%s'",
					     dir);
				return RAFT_UNAUTHORIZED;
			case UV_ENOTDIR:
				ErrMsgPrintf((char *)errmsg,
					     "path '%s' is not a directory",
					     dir);
				return RAFT_INVALID;
		}
		ErrMsgPrintf((char *)errmsg, "can't stat '%s': %s", dir,
			     uv_strerror(rv));
		return RAFT_IOERR;
	}

	if (!(req.statbuf.st_mode & S_IFDIR)) {
		ErrMsgPrintf((char *)errmsg, "path '%s' is not a directory",
			     dir);
		return RAFT_INVALID;
	}

	if (!(req.statbuf.st_mode & S_IWRITE)) {
		ErrMsgPrintf((char *)errmsg, "directory '%s' is not writable",
			     dir);
		return RAFT_INVALID;
	}

	return 0;
}

int UvFsSyncDir(const char *dir, char *errmsg)
{
	uv_file fd;
	int rv;
	rv = UvOsOpen(dir, UV_FS_O_RDONLY | UV_FS_O_DIRECTORY, 0, &fd);
	if (rv != 0) {
		UvOsErrMsg(errmsg, "open directory", rv);
		return RAFT_IOERR;
	}
	rv = UvOsFsync(fd);
	UvOsClose(fd);
	if (rv != 0) {
		UvOsErrMsg(errmsg, "fsync directory", rv);
		return RAFT_IOERR;
	}
	return 0;
}

int UvFsFileExists(const char *dir,
		   const char *filename,
		   bool *exists,
		   char *errmsg)
{
	uv_stat_t sb;
	char path[UV__PATH_SZ];
	int rv;

	rv = UvOsJoin(dir, filename, path);
	if (rv != 0) {
		return RAFT_INVALID;
	}

	rv = UvOsStat(path, &sb);
	if (rv != 0) {
		if (rv == UV_ENOENT) {
			*exists = false;
			goto out;
		}
		UvOsErrMsg(errmsg, "stat", rv);
		return RAFT_IOERR;
	}

	*exists = true;

out:
	return 0;
}

/* Get the size of the given file. */
int UvFsFileSize(const char *dir,
		 const char *filename,
		 off_t *size,
		 char *errmsg)
{
	uv_stat_t sb;
	char path[UV__PATH_SZ];
	int rv;

	rv = UvOsJoin(dir, filename, path);
	if (rv != 0) {
		return RAFT_INVALID;
	}

	rv = UvOsStat(path, &sb);
	if (rv != 0) {
		UvOsErrMsg(errmsg, "stat", rv);
		return RAFT_IOERR;
	}
	*size = (off_t)sb.st_size;

	return 0;
}

int UvFsFileIsEmpty(const char *dir,
		    const char *filename,
		    bool *empty,
		    char *errmsg)
{
	off_t size;
	int rv;

	rv = UvFsFileSize(dir, filename, &size, errmsg);
	if (rv != 0) {
		return rv;
	}
	*empty = size == 0 ? true : false;
	return 0;
}

/* Reopens a file descriptor with FMODE_NOCMTIME flag on for XFS.
 *
 * This is needed because XFS does not work correctly with async I/O when using
 * O_DSYNC: unlike other filesystems, it also updates metadata (ctime/mtime)
 * synchronously, which breaks async behavior. Disabling metadata updates allows
 * dqlite to use fdatasync as intended.
 *
 * TODO: remove this hack if/when we get `O_CMTIME` flag.
 */
static int uvXfsMaybeReopen(int fd, const char *dir, int flags)
{
	/* Given that if this fails the worst thing happening is going back to
	 * the thread pool, errors here are ignored and the reopening just
	 * doesn't happen. This will likely be the case, given that (as of
	 * Linux 6.17) SYS_ADMIN capability is required for this flow to suceed.
	 *
	 * As of Linux 6.17, there is no userland way to properly control this
	 * behaviour, but there is a comment around FMODE_NOCMTIME that gives
	 * some insight (linux/fs.h):
	 *
	 *    Don't update ctime and mtime.
	 *
	 *    Currently a special hack for the XFS open_by_handle ioctl, but
	 *    we'll hopefully graduate it to a proper O_CMTIME flag supported
	 *    by open(2) soon.
	 *
	 * Which points to the idea that it is possible to opt-out this
	 * behaviour using the syscall `open_by_handle_at` that, as a hack, will
	 * set the internal FMODE_NOCMTIME flag to suppress this behaviour.
	 *
	 * See the linux kernel functions:
	 *  - xfs_file_write_checks
	 *  - kiocb_modified
	 *  - file_modified_flags
	 * for a rational on the checks and:
	 *  - xfs_open_by_handle
	 * for the mentioned hack.
	 */
#ifdef HAVE_XFS_XFS_H
	/* Only for synchronized I/O */
	if (!(flags & O_DSYNC)) {
		return fd;
	}

	/* Fix for xfs */
	struct statfs fs_info;
	if (fstatfs(fd, &fs_info) == -1) {
		return fd;
	}

	if (fs_info.f_type != XFS_SUPER_MAGIC) {
		return fd;
	}

	/* Open the containing directory. */
	int dirfd = open(dir, O_RDONLY);
	if (dirfd < 0) {
		return fd;
	}

    xfs_handle_t handle = {};
    __u32 handle_length = sizeof(handle);
	xfs_fsop_handlereq_t req = {
		.fd = (unsigned)fd,
	    .ohandle = &handle,
	    .ohandlen = &handle_length, 
	};

	int rv = ioctl(dirfd, XFS_IOC_FD_TO_HANDLE, &req);
	if (rv != 0) {
		close(dirfd);
		return fd;
	}
	
	req = (xfs_fsop_handlereq_t){
	    .ihandle = &handle,
	    .ihandlen = handle_length,
        .oflags = (__u32)(flags & ~(O_CREAT | O_EXCL)),
	};
	int new_fd = ioctl(dirfd, XFS_IOC_OPEN_BY_HANDLE, &req);
	close(dirfd);
	if (new_fd < 0) {
		return fd;
	}
	close(fd);
	return new_fd;
#else
	(void)dir;
	(void)flags;
	return fd;
#endif
}

/* Open a file in a directory. */
static int uvFsOpenFile(const char *dir,
			const char *filename,
			int flags,
			int mode,
			uv_file *fd,
			char *errmsg)
{
	char path[UV__PATH_SZ];
	int rv;
	rv = UvOsJoin(dir, filename, path);
	if (rv != 0) {
		return RAFT_INVALID;
	}
	rv = UvOsOpen(path, flags, mode, fd);
	if (rv != 0) {
		UvOsErrMsg(errmsg, "open", rv);
		return RAFT_IOERR;
	}

	*fd = uvXfsMaybeReopen(*fd, dir, flags);

	return 0;
}

int UvFsOpenFileForReading(const char *dir,
			   const char *filename,
			   uv_file *fd,
			   char *errmsg)
{
	char path[UV__PATH_SZ];
	int flags = O_RDONLY;
	int rv;

	rv = UvOsJoin(dir, filename, path);
	if (rv != 0) {
		return RAFT_INVALID;
	}

	return uvFsOpenFile(dir, filename, flags, 0, fd, errmsg);
}

int UvFsAllocateFile(const char *dir,
		     const char *filename,
		     size_t size,
		     uv_file *fd,
		     bool fallocate,
		     char *errmsg)
{
	char path[UV__PATH_SZ];
	/* TODO: use RWF_DSYNC instead, if available. */
	const int create_flags = O_CREAT | O_EXCL;
	const int open_flags = O_WRONLY | O_DSYNC; /* Common open flags */
	int rv = 0;

	rv = UvOsJoin(dir, filename, path);
	if (rv != 0) {
		return RAFT_INVALID;
	}

	/* Allocate the desired size. */
	if (fallocate) {
		rv = uvFsOpenFile(dir, filename, open_flags | create_flags,
				  S_IRUSR | S_IWUSR, fd, errmsg);
		if (rv != 0) {
			goto err;
		}
		rv = UvOsFallocate(*fd, 0, (off_t)size);
		if (rv == 0) {
			return 0;
		} else if (rv == UV_ENOSPC) {
			ErrMsgPrintf(errmsg,
				     "not enough space to allocate %zu bytes",
				     size);
			rv = RAFT_NOSPACE;
			goto err_after_open;
		} else {
			UvOsErrMsg(errmsg, "posix_allocate", rv);
			rv = RAFT_IOERR;
			goto err_after_open;
		}
	} else {
		/* Emulate fallocate, open without O_DSYNC, because we risk
		 * doing a lot of synced writes. */
		rv = uvFsOpenFile(dir, filename,
				  (open_flags | create_flags) & ~O_DSYNC,
				  S_IRUSR | S_IWUSR, fd, errmsg);
		if (rv != 0) {
			goto err;
		}
		rv = UvOsFallocateEmulation(*fd, 0, (off_t)size);
		if (rv == UV_ENOSPC) {
			ErrMsgPrintf(errmsg,
				     "not enough space to allocate %zu bytes",
				     size);
			rv = RAFT_NOSPACE;
			goto err_after_open;
		} else if (rv != 0) {
			ErrMsgPrintf(errmsg, "fallocate emulation %d", rv);
			rv = RAFT_IOERR;
			goto err_after_open;
		}
		rv = UvOsFsync(*fd);
		if (rv != 0) {
			ErrMsgPrintf(errmsg, "fsync %d", rv);
			rv = RAFT_IOERR;
			goto err_after_open;
		}
		/* Now close and reopen the file with O_DSYNC */
		rv = UvOsClose(*fd);
		if (rv != 0) {
			ErrMsgPrintf(errmsg, "close %d", rv);
			goto err_unlink;
		}
		/* TODO: use RWF_DSYNC instead, if available. */
		rv = uvFsOpenFile(dir, filename, open_flags, S_IRUSR | S_IWUSR,
				  fd, errmsg);
		if (rv != 0) {
			goto err_unlink;
		}
	}

	return RAFT_OK;

err_after_open:
	UvOsClose(*fd);
err_unlink:
	UvOsUnlink(path);
err:
	assert(rv != 0);
	return rv;
}


static int uvFsWriteFile(const char *dir,
			 const char *filename,
			 int flags,
			 struct raft_buffer *bufs,
			 unsigned n_bufs,
			 char *errmsg)
{
	uv_file fd;
	int rv;
	size_t size;
	unsigned i;
	size = 0;
	for (i = 0; i < n_bufs; i++) {
		size += bufs[i].len;
	}
	rv = uvFsOpenFile(dir, filename, flags, S_IRUSR | S_IWUSR, &fd, errmsg);
	if (rv != 0) {
		goto err;
	}
	rv = UvOsWrite(fd, (const uv_buf_t *)bufs, n_bufs, 0);
	if (rv != (int)(size)) {
		if (rv < 0) {
			UvOsErrMsg(errmsg, "write", rv);
		} else {
			ErrMsgPrintf(errmsg,
				     "short write: %d only bytes written", rv);
		}
		goto err_after_file_open;
	}
	rv = UvOsFsync(fd);
	if (rv != 0) {
		UvOsErrMsg(errmsg, "fsync", rv);
		goto err_after_file_open;
	}
	rv = UvOsClose(fd);
	if (rv != 0) {
		UvOsErrMsg(errmsg, "close", rv);
		goto err;
	}
	return 0;

err_after_file_open:
	UvOsClose(fd);
err:
	return rv;
}

int UvFsMakeFile(const char *dir,
		 const char *filename,
		 struct raft_buffer *bufs,
		 unsigned n_bufs,
		 char *errmsg)
{
	int rv;
	char tmp_filename[UV__FILENAME_LEN + 1] = {0};
	char path[UV__PATH_SZ] = {0};
	char tmp_path[UV__PATH_SZ] = {0};

	/* Create a temp file with the given content
	 * TODO as of libuv 1.34.0, use `uv_fs_mkstemp` */
	size_t sz = sizeof(tmp_filename);
	rv = snprintf(tmp_filename, sz, TMP_FILE_FMT, filename);
	if (rv < 0 || rv >= (int)sz) {
		return rv;
	}
	int flags = UV_FS_O_WRONLY | UV_FS_O_CREAT | UV_FS_O_EXCL;
	rv = uvFsWriteFile(dir, tmp_filename, flags, bufs, n_bufs, errmsg);
	if (rv != 0) {
		goto err_after_tmp_create;
	}

	/* Check if the file exists */
	bool exists = false;
	rv = UvFsFileExists(dir, filename, &exists, errmsg);
	if (rv != 0) {
		goto err_after_tmp_create;
	}
	if (exists) {
		rv = -1;
		goto err_after_tmp_create;
	}

	/* Rename the temp file. Remark that there is a race between the
	 * existence check and the rename, there is no `renameat2` equivalent in
	 * libuv. However, in the current implementation this should pose no
	 * problems.*/
	rv = UvOsJoin(dir, tmp_filename, tmp_path);
	if (rv != 0) {
		return RAFT_INVALID;
	}
	rv = UvOsJoin(dir, filename, path);
	if (rv != 0) {
		return RAFT_INVALID;
	}
	rv = UvOsRename(tmp_path, path);
	if (rv != 0) {
		UvOsErrMsg(errmsg, "rename", rv);
		goto err_after_tmp_create;
	}

	rv = UvFsSyncDir(dir, errmsg);
	if (rv != 0) {
		char ignored[RAFT_ERRMSG_BUF_SIZE];
		UvFsRemoveFile(dir, filename, ignored);
		return rv;
	}

	return 0;

err_after_tmp_create:
	UvFsRemoveFile(dir, tmp_filename, errmsg);
	return rv;
}

#ifdef LZ4_AVAILABLE
static inline int uvOsWriteOne(uv_os_fd_t fd, void *data, size_t length, int64_t offset, char *errmsg) {
	const uv_buf_t buffer = {
		.base = data,
		.len = length,
	};
	int rv = UvOsWrite(fd, &buffer, 1, offset);
	if (rv == (int)length) {
		return RAFT_OK;
	} else if (rv < 0) {
		UvOsErrMsg(errmsg, "write", rv);
	} else {
		ErrMsgPrintf(errmsg, "short write: %d only bytes written", rv);
	}
	return RAFT_IOERR;
}
#endif

int UvFsMakeCompressedFile(const char *dir,
			   const char *filename,
			   struct raft_buffer *bufs,
			   unsigned n_bufs,
			   char *errmsg)
{
#ifndef LZ4_AVAILABLE
	(void)dir;
	(void)filename;
	(void)bufs;
	(void)n_bufs;

	ErrMsgPrintf(errmsg, "LZ4 not available");
	return RAFT_INVALID;
#else
	char path[UV__PATH_SZ] = {};
	int rv = UvOsJoin(dir, TMP_FILE_PREFIX "XXXXXX", path);
	if (rv != 0) {
		return RAFT_INVALID;
	}

	uv_fs_t temp_file;
	rv = uv_fs_mkstemp(NULL, &temp_file, path, NULL);
	if (rv == -1) {
		UvOsErrMsg(errmsg, "mkstemp", rv);
		return RAFT_IOERR;
	}
	uv_file fd = (uv_file)temp_file.result;

	size_t chunk_size = 0;
	size_t content_size = 0;
	for (unsigned i = 0; i < n_bufs; i++) {
		if (bufs[i].len > chunk_size) {
			chunk_size = bufs[i].len;
		}
		content_size += bufs[i].len;
	}
	const size_t lz4_max_block_size = 4 * 1024 * 1024;
	assert(chunk_size <= lz4_max_block_size);

	LZ4F_preferences_t
	    lz4_pref = { .frameInfo = {
			     /* Detect data corruption when decompressing */
			     .contentChecksumFlag = 1,
			     /* For allocating a suitable buffer when
				decompressing */
			     .contentSize = content_size,
			 } };

	const size_t output_cap = LZ4F_compressBound(chunk_size, &lz4_pref);
	char *output_buffer = raft_malloc(output_cap);
	if (output_buffer == NULL) {
		rv = RAFT_NOMEM;
		goto err_after_open;
	}

	LZ4F_compressionContext_t ctx;
	LZ4F_errorCode_t err =
	    LZ4F_createCompressionContext(&ctx, LZ4F_VERSION);
	if (LZ4F_isError(err)) {
		ErrMsgPrintf(errmsg, "LZ4F_createDecompressionContext %s",
			     LZ4F_getErrorName(err));
		rv = RAFT_NOMEM;
		goto err_after_buf_alloc;
	}

	size_t output_len =
	    LZ4F_compressBegin(ctx, output_buffer, output_cap, &lz4_pref);
	if (LZ4F_isError(output_len)) {
		ErrMsgPrintf(errmsg, "LZ4F_compressBegin %s",
			     LZ4F_getErrorName(output_len));
		rv = RAFT_IOERR;
		goto err_after_ctx_alloc;
	}

	/* Write the header */
	rv = uvOsWriteOne(fd, output_buffer, output_len, -1, errmsg);
	if (rv != RAFT_OK) {
		goto err_after_ctx_alloc;
	}

	for (unsigned i = 0; i < n_bufs; i++) {
		output_len =
		    LZ4F_compressUpdate(ctx, output_buffer, output_cap,
					bufs[i].base, bufs[i].len, NULL);
		if (output_len == 0) {
			/* In this case the output is buffered internally by
			 * liblz4 */
			continue;
		} else if (LZ4F_isError(output_len)) {
			ErrMsgPrintf(errmsg, "LZ4F_compressUpdate %s",
				     LZ4F_getErrorName(output_len));
			rv = RAFT_IOERR;
			goto err_after_ctx_alloc;
		}

		rv = uvOsWriteOne(fd, output_buffer, output_len, -1, errmsg);
		if (rv != RAFT_OK) {
			goto err_after_ctx_alloc;
		}
	}

	output_len = LZ4F_compressEnd(ctx, output_buffer, output_cap, NULL);
	if (LZ4F_isError(output_len)) {
		ErrMsgPrintf(errmsg, "LZ4F_compressEnd %s",
			     LZ4F_getErrorName(output_len));
		rv = RAFT_IOERR;
		goto err_after_ctx_alloc;
	} else if (output_len > 0) {
		rv = uvOsWriteOne(fd, output_buffer, output_len, -1, errmsg);
		if (rv != RAFT_OK) {
			goto err_after_ctx_alloc;
		}
	}

	rv = UvOsFsync(fd);
	if (rv != 0) {
		UvOsErrMsg(errmsg, "fsync", rv);
		goto err_after_ctx_alloc;
	}

	rv = UvOsJoin(dir, filename, path);
	if (rv != 0) {
		rv = RAFT_INVALID;
		goto err_after_ctx_alloc;
	}

	rv = UvOsRename(temp_file.path, path);
	if (rv != 0) {
		rv = RAFT_IOERR;
		goto err_after_ctx_alloc;
	}

	rv = UvFsSyncDir(dir, errmsg);
	if (rv != 0) {
		/* Try to unlink the new file. */
		UvOsUnlink(path);
	}

err_after_ctx_alloc:
	LZ4F_freeCompressionContext(ctx);
err_after_buf_alloc:
	raft_free(output_buffer);
err_after_open:
	UvOsClose(fd);
	if (rv != RAFT_OK) {
		/* Try to unlink the temp file. */
		UvOsUnlink(temp_file.path);
	}
	uv_fs_req_cleanup(&temp_file);
	return rv;
#endif
}

int UvFsMakeOrOverwriteFile(const char *dir,
			    const char *filename,
			    const struct raft_buffer *buf,
			    char *errmsg)
{
	char path[UV__PATH_SZ];
	int flags = UV_FS_O_WRONLY;
	int mode = 0;
	bool exists = true;
	uv_file fd;
	int rv;

	rv = UvOsJoin(dir, filename, path);
	if (rv != 0) {
		return RAFT_INVALID;
	}

open:
	rv = UvOsOpen(path, flags, mode, &fd);
	if (rv != 0) {
		if (rv == UV_ENOENT && !(flags & UV_FS_O_CREAT)) {
			exists = false;
			flags |= UV_FS_O_CREAT;
			mode = S_IRUSR | S_IWUSR;
			goto open;
		}
		goto err;
	}

	rv = UvOsWrite(fd, (const uv_buf_t *)buf, 1, 0);
	if (rv != (int)(buf->len)) {
		if (rv < 0) {
			UvOsErrMsg(errmsg, "write", rv);
		} else {
			ErrMsgPrintf(errmsg,
				     "short write: %d only bytes written", rv);
		}
		goto err_after_file_open;
	}

	if (exists) {
		rv = UvOsFdatasync(fd);
		if (rv != 0) {
			UvOsErrMsg(errmsg, "fsync", rv);
			goto err_after_file_open;
		}
	} else {
		rv = UvOsFsync(fd);
		if (rv != 0) {
			UvOsErrMsg(errmsg, "fsync", rv);
			goto err_after_file_open;
		}
	}

	rv = UvOsClose(fd);
	if (rv != 0) {
		UvOsErrMsg(errmsg, "close", rv);
		goto err;
	}

	if (!exists) {
		rv = UvFsSyncDir(dir, errmsg);
		if (rv != 0) {
			goto err;
		}
	}

	return 0;

err_after_file_open:
	UvOsClose(fd);
err:
	return RAFT_IOERR;
}

int UvFsReadInto(uv_file fd, struct raft_buffer *buf, char *errmsg)
{
	ssize_t rv;
	size_t offset = 0;

	/* TODO: use uv_fs_read() */
	while (offset < buf->len) {
		rv = read(fd, (char *)buf->base + offset, buf->len - offset);
		if (rv == -1) {
			UvOsErrMsg(errmsg, "read", -errno);
			return RAFT_IOERR;
		}
		/* EOF. Don't think this is reachable, but just make very sure
		 * we don't loop forever. */
		if (rv == 0) {
			break;
		}
		assert(rv > 0);
		offset += (size_t)rv;
	}
	if (offset < buf->len) {
		ErrMsgPrintf(errmsg, "short read: %zu bytes instead of %zu",
			     offset, buf->len);
		return RAFT_IOERR;
	}
	return 0;
}

int UvFsReadFile(const char *dir,
		 const char *filename,
		 struct raft_buffer *buf,
		 char *errmsg)
{
	uv_stat_t sb;
	char path[UV__PATH_SZ];
	uv_file fd;
	int rv;

	rv = UvOsJoin(dir, filename, path);
	if (rv != 0) {
		return RAFT_INVALID;
	}

	rv = UvOsStat(path, &sb);
	if (rv != 0) {
		UvOsErrMsg(errmsg, "stat", rv);
		rv = RAFT_IOERR;
		goto err;
	}

	rv = uvFsOpenFile(dir, filename, O_RDONLY, 0, &fd, errmsg);
	if (rv != 0) {
		goto err;
	}

	rv = posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
	assert(rv == 0);

	buf->len = (size_t)sb.st_size;
	buf->base = RaftHeapMalloc(buf->len);
	if (buf->base == NULL) {
		ErrMsgOom(errmsg);
		rv = RAFT_NOMEM;
		goto err_after_open;
	}

	rv = UvFsReadInto(fd, buf, errmsg);
	if (rv != 0) {
		goto err_after_buf_alloc;
	}

	UvOsClose(fd);

	return 0;

err_after_buf_alloc:
	RaftHeapFree(buf->base);
err_after_open:
	UvOsClose(fd);
err:
	return rv;
}

int UvFsReadCompressedFile(const char *dir,
			   const char *filename,
			   struct raft_buffer *buf,
			   char *errmsg)
{
#ifndef LZ4_AVAILABLE
	(void)dir;
	(void)filename;
	(void)buf;

	ErrMsgPrintf(errmsg, "LZ4 not available");
	return RAFT_INVALID;
#else
	char path[UV__PATH_SZ] = {};
	int rv = UvOsJoin(dir, filename, path);
	if (rv != 0) {
		return RAFT_INVALID;
	}

	uv_file fd;
	rv = uvFsOpenFile(dir, filename, O_RDONLY, 0, &fd, errmsg);
	if (rv != 0) {
		return rv;
	}

	rv = posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
	assert(rv == 0);

	LZ4F_decompressionContext_t ctx;
	size_t lzrv = LZ4F_createDecompressionContext(&ctx, LZ4F_VERSION);
	if (LZ4F_isError(lzrv)) {
		ErrMsgPrintf(errmsg, "LZ4F_createDecompressionContext %s",
			LZ4F_getErrorName(lzrv));
		rv = RAFT_NOMEM;
		goto err_after_open;
	}

	/* The proper input size depends a lot on what the maximum block size is, but 
	 * in general it is possible to be a little bit pessimistic here and use the
	 * max value, which is 4MiB. */
	const size_t input_buffer_size =  LZ4_COMPRESSBOUND(4 * 1024 * 1024);
	void *input_buffer = raft_malloc(input_buffer_size);
	if (input_buffer == NULL) {
		rv = RAFT_NOMEM;
		goto err_after_ctx_alloc;
	}

	/* Read at least LZ4F_HEADER_SIZE_MAX bytes. */
	size_t input_size = 0;
	while (input_size < LZ4F_HEADER_SIZE_MAX) {
		ssize_t read_rv = read(fd, (char *)input_buffer + input_size, input_buffer_size - input_size);
		if (read_rv == -1) {
			rv = RAFT_IOERR;
			UvOsErrMsg(errmsg, "read", -errno);
			goto done;
		} else if (read_rv == 0) {
			break;
		}
		input_size += (size_t)read_rv;
	}
	if (input_size < LZ4F_HEADER_SIZE_MIN) {
		ErrMsgPrintf(errmsg, "compressed file is too short");
		rv = RAFT_INVALID;
		goto done;
	}

	LZ4F_frameInfo_t frameInfo = {};
	size_t input_offset = (size_t)input_size;
	lzrv = LZ4F_getFrameInfo(ctx, &frameInfo, input_buffer, &input_offset);
	if (LZ4F_isError(lzrv)) {
		ErrMsgPrintf(errmsg, "LZ4F_getFrameInfo %s",
			     LZ4F_getErrorName(lzrv));
		rv = RAFT_IOERR;
		goto done;
	}

	if (frameInfo.contentSize == 0) {
		rv = RAFT_OK;
		goto done; // Not really an error here...
	}

	const size_t output_buffer_size = (size_t)frameInfo.contentSize;
	void *output_buffer = raft_malloc(output_buffer_size);
	if (output_buffer == NULL) {
		rv = RAFT_NOMEM;
		goto done;
	}

	size_t output_offset = 0;
	while (output_offset < output_buffer_size) {
		if (input_offset == input_size) {
			/* try to read some */
			ssize_t read_rv = read(fd, input_buffer, input_buffer_size);
			if (read_rv < 0) {
				UvOsErrMsg(errmsg, "read", -errno);
				rv = RAFT_IOERR;
				goto err_after_output_alloc;
			} else if (read_rv == 0) {
				ErrMsgPrintf(errmsg, "short read: %zu bytes instead of %zu",
			     	output_offset, output_buffer_size);
				rv = RAFT_IOERR;
				goto err_after_output_alloc;
			}
			input_size = (size_t)read_rv;
			input_offset = 0;
		}

		size_t output_size = output_buffer_size - output_offset;
		size_t input_read = input_size - input_offset;
		lzrv = LZ4F_decompress(ctx, output_buffer + output_offset,
				&output_size, input_buffer + input_offset,
				&input_read, NULL);
		if (LZ4F_isError(lzrv)) {
			ErrMsgPrintf(errmsg, "LZ4F_decompress %s",
					LZ4F_getErrorName(lzrv));
			rv = RAFT_IOERR;
			goto err_after_output_alloc;
		}

		output_offset += output_size;
		input_offset += input_read;
	}
	rv = RAFT_OK;
	*buf = (struct raft_buffer) {
		.base = output_buffer,
		.len = output_buffer_size,
	};
	goto done;

err_after_output_alloc:
	raft_free(output_buffer);
done:
	raft_free(input_buffer);
err_after_ctx_alloc:
	LZ4F_freeDecompressionContext(ctx);
err_after_open:
	UvOsClose(fd);
	return rv;
#endif
}

int UvFsReadFileInto(const char *dir,
		     const char *filename,
		     struct raft_buffer *buf,
		     char *errmsg)
{
	char path[UV__PATH_SZ];
	uv_file fd;
	int rv;

	rv = UvOsJoin(dir, filename, path);
	if (rv != 0) {
		return RAFT_INVALID;
	}

	rv = uvFsOpenFile(dir, filename, O_RDONLY, 0, &fd, errmsg);
	if (rv != 0) {
		goto err;
	}

	rv = UvFsReadInto(fd, buf, errmsg);
	if (rv != 0) {
		goto err_after_open;
	}

	UvOsClose(fd);

	return 0;

err_after_open:
	UvOsClose(fd);
err:
	return rv;
}

int UvFsRemoveFile(const char *dir, const char *filename, char *errmsg)
{
	char path[UV__PATH_SZ];
	int rv;
	rv = UvOsJoin(dir, filename, path);
	if (rv != 0) {
		return RAFT_INVALID;
	}
	rv = UvOsUnlink(path);
	if (rv != 0) {
		UvOsErrMsg(errmsg, "unlink", rv);
		return RAFT_IOERR;
	}
	return 0;
}

int UvFsRenameFile(const char *dir,
		   const char *filename1,
		   const char *filename2,
		   char *errmsg)
{
	char path1[UV__PATH_SZ];
	char path2[UV__PATH_SZ];
	int rv;

	rv = UvOsJoin(dir, filename1, path1);
	if (rv != 0) {
		return RAFT_INVALID;
	}
	rv = UvOsJoin(dir, filename2, path2);
	if (rv != 0) {
		return RAFT_INVALID;
	}

	rv = UvOsRename(path1, path2);
	if (rv != 0) {
		UvOsErrMsg(errmsg, "rename", rv);
		return rv;
	}

	return 0;
}

int UvFsTruncateAndRenameFile(const char *dir,
			      size_t size,
			      const char *filename1,
			      const char *filename2,
			      char *errmsg)
{
	char path1[UV__PATH_SZ];
	char path2[UV__PATH_SZ];
	uv_file fd;
	int rv;

	rv = UvOsJoin(dir, filename1, path1);
	if (rv != 0) {
		return RAFT_INVALID;
	}
	rv = UvOsJoin(dir, filename2, path2);
	if (rv != 0) {
		return RAFT_INVALID;
	}

	/* Truncate and rename. */
	rv = UvOsOpen(path1, UV_FS_O_RDWR, 0, &fd);
	if (rv != 0) {
		UvOsErrMsg(errmsg, "open", rv);
		goto err;
	}
	rv = UvOsTruncate(fd, (off_t)size);
	if (rv != 0) {
		UvOsErrMsg(errmsg, "truncate", rv);
		goto err_after_open;
	}
	rv = UvOsFsync(fd);
	if (rv != 0) {
		UvOsErrMsg(errmsg, "fsync", rv);
		goto err_after_open;
	}
	UvOsClose(fd);

	rv = UvOsRename(path1, path2);
	if (rv != 0) {
		UvOsErrMsg(errmsg, "rename", rv);
		goto err;
	}

	return 0;

err_after_open:
	UvOsClose(fd);
err:
	return RAFT_IOERR;
}

/* Check if direct I/O is possible on the given fd. */
static int probeDirectIO(int fd, size_t *size, char *errmsg)
{
	struct statfs fs_info; /* To check the file system type. */
	void *buf;             /* Buffer to use for the probe write. */
	int rv;

	rv = UvOsSetDirectIo(fd);
	if (rv != 0) {
		if (rv != UV_EINVAL) {
			/* UNTESTED: the parameters are ok, so this should never
			 * happen. */
			UvOsErrMsg(errmsg, "fnctl", rv);
			return RAFT_IOERR;
		}
		rv = fstatfs(fd, &fs_info);
		if (rv == -1) {
			/* UNTESTED: in practice ENOMEM should be the only
			 * failure mode */
			UvOsErrMsg(errmsg, "fstatfs", -errno);
			return RAFT_IOERR;
		}
		switch (fs_info.f_type) {
			case 0x01021994: /* TMPFS_MAGIC */
			case 0x2fc12fc1: /* ZFS magic */
			case 0x24051905: /* UBIFS Support magic */
				*size = 0;
				return 0;
			default:
				/* UNTESTED: this is an unsupported file system.
				 */
				ErrMsgPrintf(errmsg,
					     "unsupported file system: %llx",
					     (unsigned long long)fs_info.f_type);
				return RAFT_IOERR;
		}
	}

	/* Try to perform direct I/O, using various buffer size. */
	*size = UV__FS_PROBE_FILE_SIZE;
	while (*size >= 512) {
		buf = raft_aligned_alloc(*size, *size);
		if (buf == NULL) {
			ErrMsgOom(errmsg);
			return RAFT_NOMEM;
		}
		memset(buf, 0, *size);
		rv = (int)write(fd, buf, *size);
		raft_aligned_free(*size, buf);
		if (rv > 0) {
			/* Since we fallocate'ed the file, we should never fail
			 * because of lack of disk space, and all bytes should
			 * have been written. */
			assert(rv == (int)(*size));
			return 0;
		}
		assert(rv == -1);
		if (errno != EIO && errno != EOPNOTSUPP) {
			/* UNTESTED: this should basically fail only because of
			 * disk errors, since we allocated the file with
			 * posix_fallocate. */

			/* FIXME: this is a workaround because shiftfs doesn't
			 * return EINVAL in the fnctl call above, for example
			 * when the underlying fs is ZFS. */
			if (errno == EINVAL && *size == 4096) {
				*size = 0;
				return 0;
			}

			UvOsErrMsg(errmsg, "write", -errno);
			return RAFT_IOERR;
		}
		*size = *size / 2;
	}

	*size = 0;
	return 0;
}

/* Check if fully non-blocking async I/O is possible on the given fd. */
static int probeAsyncIO(int fd, size_t size, bool *ok, char *errmsg)
{
	void *buf;                  /* Buffer to use for the probe write */
	aio_context_t ctx = 0;      /* KAIO context handle */
	int n_events;
	int rv;

	/* Setup the KAIO context handle */
	rv = UvOsIoSetup(1, &ctx);
	if (rv != 0) {
		UvOsErrMsg(errmsg, "io_setup", rv);
		/* UNTESTED: in practice this should fail only with ENOMEM */
		return RAFT_IOERR;
	}

	/* Allocate the write buffer */
	buf = raft_aligned_alloc(size, size);
	if (buf == NULL) {
		ErrMsgOom(errmsg);
		return RAFT_NOMEM;
	}
	memset(buf, 0, size);

	/* Prepare the KAIO request object */
	struct iocb iocb = {
		.aio_lio_opcode = IOCB_CMD_PWRITE,
		.aio_fildes = (__u32)fd,
		.aio_buf = (__u64)(uintptr_t)buf,
		.aio_nbytes = (__u64)size,
		.aio_rw_flags = RWF_NOWAIT,
	};

	/* Submit the KAIO request */
	struct iocb *iocbs = &iocb; /* Because the io_submit() API sucks */
	rv = UvOsIoSubmit(ctx, 1, &iocbs);
	if (rv != 0) {
		/* UNTESTED: in practice this should fail only with ENOMEM */
		raft_aligned_free(size, buf);
		UvOsIoDestroy(ctx);
		/* On ZFS 0.8 this is not properly supported yet. Also, when
		 * running on older kernels a binary compiled on a kernel with
		 * RWF_NOWAIT support, we might get EINVAL. */
		if (errno == EOPNOTSUPP || errno == EINVAL) {
			*ok = false;
			return 0;
		}
		UvOsErrMsg(errmsg, "io_submit", rv);
		return RAFT_IOERR;
	}

	/* Fetch the response: will block until done. */
	struct io_event event = {}; /* KAIO response object */
	n_events = UvOsIoGetevents(ctx, 1, 1, &event, NULL);
	assert(n_events == 1);
	if (n_events != 1) {
		/* UNTESTED */
		UvOsErrMsg(errmsg, "UvOsIoGetevents", n_events);
		return RAFT_IOERR;
	}

	/* Release the write buffer. */
	raft_aligned_free(size, buf);

	/* Release the KAIO context handle. */
	rv = UvOsIoDestroy(ctx);
	if (rv != 0) {
		UvOsErrMsg(errmsg, "io_destroy", rv);
		return RAFT_IOERR;
	}

	if (event.res > 0) {
		assert(event.res == (int)size);
		*ok = true;
	} else {
		/* UNTESTED: this should basically fail only because of disk
		 * errors, since we allocated the file with posix_fallocate and
		 * the block size is supposed to be correct. */
		*ok = false;
	}

	return 0;
}

#define UV__FS_PROBE_FALLOCATE_FILE ".probe_fallocate"
/* Leave detection of other error conditions to other probe* functions, only
 * bother checking if posix_fallocate returns success. */
static void probeFallocate(const char *dir, bool *fallocate)
{
	int flags = O_WRONLY | O_CREAT | O_EXCL; /* Common open flags */
	char ignored[RAFT_ERRMSG_BUF_SIZE];
	int rv = 0;
	int fd = -1;

	*fallocate = false;
	UvFsRemoveFile(dir, UV__FS_PROBE_FALLOCATE_FILE, ignored);
	rv = uvFsOpenFile(dir, UV__FS_PROBE_FALLOCATE_FILE, flags,
			  S_IRUSR | S_IWUSR, &fd, ignored);
	if (rv != 0) {
		goto out;
	}
	rv = UvOsFallocate(fd, 0, (off_t)4096);
	if (rv == 0) {
		*fallocate = true;
	}
	close(fd);

out:
	UvFsRemoveFile(dir, UV__FS_PROBE_FALLOCATE_FILE, ignored);
}

int UvFsProbeCapabilities(const char *dir,
			  size_t *direct,
			  bool *async,
			  bool *fallocate,
			  char *errmsg)
{
	int fd; /* File descriptor of the probe file */
	int rv;
	char ignored[RAFT_ERRMSG_BUF_SIZE];

	probeFallocate(dir, fallocate);

	/* Create a temporary probe file. */
	UvFsRemoveFile(dir, UV__FS_PROBE_FILE, ignored);
	rv = UvFsAllocateFile(dir, UV__FS_PROBE_FILE, UV__FS_PROBE_FILE_SIZE,
			      &fd, *fallocate, errmsg);
	if (rv != 0) {
		ErrMsgWrapf(errmsg, "create I/O capabilities probe file");
		goto err;
	}
	UvFsRemoveFile(dir, UV__FS_PROBE_FILE, ignored);

	/* Check if we can use direct I/O. */
	rv = probeDirectIO(fd, direct, errmsg);
	if (rv != 0) {
		ErrMsgWrapf(errmsg, "probe Direct I/O");
		goto err_after_file_open;
	}

	/* If direct I/O is not possible, we can't perform fully asynchronous
	 * I/O, because io_submit might potentially block. */
	if (*direct == 0) {
		*async = false;
		goto out;
	}
	rv = probeAsyncIO(fd, *direct, async, errmsg);
	if (rv != 0) {
		ErrMsgWrapf(errmsg, "probe Async I/O");
		goto err_after_file_open;
	}

out:
	close(fd);
	return 0;

err_after_file_open:
	close(fd);
err:
	return rv;
}
