#include "uv_os.h"

#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/types.h>
#include <sys/vfs.h>
#include <unistd.h>
#include <uv.h>

#include "assert.h"
#include "err.h"
#include "syscall.h"

/* Default permissions when creating a directory. */
#define DEFAULT_DIR_PERM 0700

int UvOsOpen(const char *path, int flags, int mode, uv_file *fd)
{
	struct uv_fs_s req;
	int rv;
	rv = uv_fs_open(NULL, &req, path, flags, mode, NULL);
	if (rv < 0) {
		return rv;
	}
	*fd = rv;
	return 0;
}

int UvOsClose(uv_file fd)
{
	struct uv_fs_s req;
	return uv_fs_close(NULL, &req, fd, NULL);
}

/* Emulate fallocate(). Mostly taken from glibc's implementation. */
int UvOsFallocateEmulation(int fd, off_t offset, off_t len)
{
	ssize_t increment;
	struct statfs f;
	int rv;

	rv = fstatfs(fd, &f);
	if (rv != 0) {
		return -errno;
	}

	if (f.f_bsize == 0) {
		increment = 512;
	} else if (f.f_bsize < 4096) {
		increment = (ssize_t)f.f_bsize;
	} else {
		increment = 4096;
	}

	for (offset += (len - 1) % increment; len > 0; offset += increment) {
		len -= increment;
		rv = (int)pwrite(fd, "", 1, offset);
		if (rv != 1) {
			return -errno;
		}
	}

	return 0;
}

int UvOsFallocate(uv_file fd, off_t offset, off_t len)
{
	int rv;
	rv = posix_fallocate(fd, offset, len);
	if (rv != 0) {
		/* From the manual page:
		 *
		 *   posix_fallocate() returns zero on success, or an error
		 * number on failure.  Note that errno is not set.
		 */
		return -rv;
	}
	return 0;
}

int UvOsTruncate(uv_file fd, off_t offset)
{
	struct uv_fs_s req;
	return uv_fs_ftruncate(NULL, &req, fd, offset, NULL);
}

int UvOsFsync(uv_file fd)
{
	struct uv_fs_s req;
	return uv_fs_fsync(NULL, &req, fd, NULL);
}

int UvOsFdatasync(uv_file fd)
{
	struct uv_fs_s req;
	return uv_fs_fdatasync(NULL, &req, fd, NULL);
}

int UvOsStat(const char *path, uv_stat_t *sb)
{
	struct uv_fs_s req;
	int rv;
	rv = uv_fs_stat(NULL, &req, path, NULL);
	if (rv != 0) {
		return rv;
	}
	memcpy(sb, &req.statbuf, sizeof *sb);
	return 0;
}

int UvOsWrite(uv_file fd,
	      const uv_buf_t bufs[],
	      unsigned int nbufs,
	      int64_t offset)
{
	struct uv_fs_s req;
	return uv_fs_write(NULL, &req, fd, bufs, nbufs, offset, NULL);
}

int UvOsUnlink(const char *path)
{
	struct uv_fs_s req;
	return uv_fs_unlink(NULL, &req, path, NULL);
}

int UvOsRename(const char *path1, const char *path2)
{
	struct uv_fs_s req;
	return uv_fs_rename(NULL, &req, path1, path2, NULL);
}

int UvOsJoin(const char *dir, const char *filename, char *path)
{
	if (!UV__DIR_HAS_VALID_LEN(dir) ||
	    !UV__FILENAME_HAS_VALID_LEN(filename)) {
		return -1;
	}
	strcpy(path, dir);
	strcat(path, "/");
	strcat(path, filename);
	return 0;
}

int UvOsIoSetup(unsigned nr, aio_context_t *ctxp)
{
	int rv;
	rv = io_setup(nr, ctxp);
	if (rv == -1) {
		return -errno;
	}
	return 0;
}

int UvOsIoDestroy(aio_context_t ctx)
{
	int rv;
	rv = io_destroy(ctx);
	if (rv == -1) {
		return -errno;
	}
	return 0;
}

int UvOsIoSubmit(aio_context_t ctx, long nr, struct iocb **iocbpp)
{
	int rv;
	rv = io_submit(ctx, nr, iocbpp);
	if (rv == -1) {
		return -errno;
	}
	assert(rv == nr); /* TODO: can something else be returned? */
	return 0;
}

int UvOsIoGetevents(aio_context_t ctx,
		    long min_nr,
		    long max_nr,
		    struct io_event *events,
		    struct timespec *timeout)
{
	int rv;
	do {
		rv = io_getevents(ctx, min_nr, max_nr, events, timeout);
	} while (rv == -1 && errno == EINTR);

	if (rv == -1) {
		return -errno;
	}
	assert(rv >= min_nr);
	assert(rv <= max_nr);
	return rv;
}

int UvOsEventfd(unsigned int initval, int flags)
{
	int rv;
	/* At the moment only UV_FS_O_NONBLOCK is supported */
	assert(flags == UV_FS_O_NONBLOCK);
	flags = EFD_NONBLOCK | EFD_CLOEXEC;
	rv = eventfd(initval, flags);
	if (rv == -1) {
		return -errno;
	}
	return rv;
}

int UvOsSetDirectIo(uv_file fd)
{
	int flags; /* Current fcntl flags */
	int rv;
	flags = fcntl(fd, F_GETFL);
	rv = fcntl(fd, F_SETFL, flags | UV_FS_O_DIRECT);
	if (rv == -1) {
		return -errno;
	}
	return 0;
}
