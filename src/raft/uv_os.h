/* Operating system related utilities. */

#ifndef UV_OS_H_
#define UV_OS_H_

#include <fcntl.h>
#include <linux/aio_abi.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <uv.h>

/* Maximum size of a full file system path string. */
#define UV__PATH_SZ 1024

/* Maximum length of a filename string. */
#define UV__FILENAME_LEN 128

/* Length of path separator. */
#define UV__SEP_LEN 1 /* strlen("/") */

/* True if STR's length is at most LEN. */
#define LEN_AT_MOST_(STR, LEN) (strnlen(STR, LEN + 1) <= LEN)

/* Maximum length of a directory path string. */
#define UV__DIR_LEN (UV__PATH_SZ - UV__SEP_LEN - UV__FILENAME_LEN - 1)

/* True if the given DIR string has at most UV__DIR_LEN chars. */
#define UV__DIR_HAS_VALID_LEN(DIR) LEN_AT_MOST_(DIR, UV__DIR_LEN)

/* True if the given FILENAME string has at most UV__FILENAME_LEN chars. */
#define UV__FILENAME_HAS_VALID_LEN(FILENAME) \
	LEN_AT_MOST_(FILENAME, UV__FILENAME_LEN)

/* Portable open() */
int UvOsOpen(const char *path, int flags, int mode, uv_file *fd);

/* Portable close() */
int UvOsClose(uv_file fd);

/* TODO: figure a portable abstraction. */
int UvOsFallocate(uv_file fd, off_t offset, off_t len);

/* Emulation to use in case UvOsFallocate fails with -EONOTSUPP.
 * This might happen with a libc implementation (e.g. musl) that
 * doesn't implement a transparent fallback if fallocate() is
 * not supported by the underlying file system. */
int UvOsFallocateEmulation(int fd, off_t offset, off_t len);

/* Portable truncate() */
int UvOsTruncate(uv_file fd, off_t offset);

/* Portable fsync() */
int UvOsFsync(uv_file fd);

/* Portable fdatasync() */
int UvOsFdatasync(uv_file fd);

/* Portable stat() */
int UvOsStat(const char *path, uv_stat_t *sb);

/* Portable write() */
int UvOsWrite(uv_file fd,
	      const uv_buf_t bufs[],
	      unsigned int nbufs,
	      int64_t offset);

/* Portable unlink() */
int UvOsUnlink(const char *path);

/* Portable rename() */
int UvOsRename(const char *path1, const char *path2);

/* Join dir and filename into a full OS path. */
int UvOsJoin(const char *dir, const char *filename, char *path);

/* TODO: figure a portable abstraction. */
int UvOsIoSetup(unsigned nr, aio_context_t *ctxp);
int UvOsIoDestroy(aio_context_t ctx);
int UvOsIoSubmit(aio_context_t ctx, long nr, struct iocb **iocbpp);
int UvOsIoGetevents(aio_context_t ctx,
		    long min_nr,
		    long max_nr,
		    struct io_event *events,
		    struct timespec *timeout);
int UvOsEventfd(unsigned int initval, int flags);
int UvOsSetDirectIo(uv_file fd);

/* Format an error message caused by a failed system call or stdlib function. */
#define UvOsErrMsg(ERRMSG, SYSCALL, ERRNUM)                      \
	{                                                        \
		ErrMsgPrintf(ERRMSG, "%s", uv_strerror(ERRNUM)); \
		ErrMsgWrapf(ERRMSG, SYSCALL);                    \
	}

#endif /* UV_OS_H_ */
