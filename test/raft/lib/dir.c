#include "dir.h"

#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <unistd.h>

#define SEP "/"
#define TEMPLATE "raft-test-XXXXXX"

#define TEST_DIR_TEMPLATE "./tmp/%s/raft-test-XXXXXX"

static char *dirAll[] = {"tmpfs", "ext4", "btrfs", "xfs", "zfs", NULL};

static char *dirTmpfs[] = {"tmpfs", NULL};

static char *dirAio[] = {"btrfs", "ext4", "xfs", NULL};

static char *dirNoAio[] = {"tmpfs", "zfs", NULL};

MunitParameterEnum DirTmpfsParams[] = {
    {DIR_FS_PARAM, dirTmpfs},
    {NULL, NULL},
};

MunitParameterEnum DirAllParams[] = {
    {DIR_FS_PARAM, dirAll},
    {NULL, NULL},
};

MunitParameterEnum DirAioParams[] = {
    {DIR_FS_PARAM, dirAio},
    {NULL, NULL},
};

MunitParameterEnum DirNoAioParams[] = {
    {DIR_FS_PARAM, dirNoAio},
    {NULL, NULL},
};

/* Create a temporary directory in the given parent directory. */
static char *dirMakeTemp(const char *parent)
{
    char *dir;
    if (parent == NULL) {
        return NULL;
    }
    dir = munit_malloc(strlen(parent) + strlen(SEP) + strlen(TEMPLATE) + 1);
    sprintf(dir, "%s%s%s", parent, SEP, TEMPLATE);
    if (mkdtemp(dir) == NULL) {
        munit_error(strerror(errno));
    }
    return dir;
}

void *DirSetUp(MUNIT_UNUSED const MunitParameter params[],
               MUNIT_UNUSED void *user_data)
{
    const char *fs = munit_parameters_get(params, DIR_FS_PARAM);
    if (fs == NULL) {
        return dirMakeTemp("/tmp");
    } else if (strcmp(fs, "tmpfs") == 0) {
        return DirTmpfsSetUp(params, user_data);
    } else if (strcmp(fs, "ext4") == 0) {
        return DirExt4SetUp(params, user_data);
    } else if (strcmp(fs, "btrfs") == 0) {
        return DirBtrfsSetUp(params, user_data);
    } else if (strcmp(fs, "zfs") == 0) {
        return DirZfsSetUp(params, user_data);
    } else if (strcmp(fs, "xfs") == 0) {
        return DirXfsSetUp(params, user_data);
    }
    munit_errorf("Unsupported file system %s", fs);
    return NULL;
}

void *DirTmpfsSetUp(MUNIT_UNUSED const MunitParameter params[],
                    MUNIT_UNUSED void *user_data)
{
    return dirMakeTemp(getenv("RAFT_TMP_TMPFS"));
}

void *DirExt4SetUp(MUNIT_UNUSED const MunitParameter params[],
                   MUNIT_UNUSED void *user_data)
{
    return dirMakeTemp(getenv("RAFT_TMP_EXT4"));
}

void *DirBtrfsSetUp(MUNIT_UNUSED const MunitParameter params[],
                    MUNIT_UNUSED void *user_data)
{
    return dirMakeTemp(getenv("RAFT_TMP_BTRFS"));
}

void *DirZfsSetUp(MUNIT_UNUSED const MunitParameter params[],
                  MUNIT_UNUSED void *user_data)
{
    return dirMakeTemp(getenv("RAFT_TMP_ZFS"));
}

void *DirXfsSetUp(MUNIT_UNUSED const MunitParameter params[],
                  MUNIT_UNUSED void *user_data)
{
    return dirMakeTemp(getenv("RAFT_TMP_XFS"));
}

/* Wrapper around remove(), compatible with ntfw. */
static int dirRemoveFn(const char *path,
                       MUNIT_UNUSED const struct stat *sbuf,
                       MUNIT_UNUSED int type,
                       MUNIT_UNUSED struct FTW *ftwb)
{
    return remove(path);
}

static void dirRemove(char *dir)
{
    int rv;
    rv = chmod(dir, 0755);
    munit_assert_int(rv, ==, 0);

    rv = nftw(dir, dirRemoveFn, 10, FTW_DEPTH | FTW_MOUNT | FTW_PHYS);
    munit_assert_int(rv, ==, 0);
}

static bool dirExists(const char *dir)
{
    struct stat sb;
    int rv;

    rv = stat(dir, &sb);
    if (rv == -1) {
        munit_assert_int(errno, ==, ENOENT);
        return false;
    }

    return true;
}

void DirTearDown(void *data)
{
    char *dir = data;
    if (dir == NULL) {
        return;
    }
    if (dirExists(dir)) {
        dirRemove(dir);
    }
    free(dir);
}

/* Join the given @dir and @filename into @path. */
static void joinPath(const char *dir, const char *filename, char *path)
{
    strcpy(path, dir);
    strcat(path, "/");
    strcat(path, filename);
}

void DirWriteFile(const char *dir,
                  const char *filename,
                  const void *buf,
                  const size_t n)
{
    char path[256];
    int fd;
    int rv;

    joinPath(dir, filename, path);

    fd = open(path, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    munit_assert_int(fd, !=, -1);

    rv = write(fd, buf, n);
    munit_assert_int(rv, ==, n);

    close(fd);
}

void DirWriteFileWithZeros(const char *dir,
                           const char *filename,
                           const size_t n)
{
    void *buf = munit_malloc(n);

    DirWriteFile(dir, filename, buf, n);

    free(buf);
}

void DirOverwriteFile(const char *dir,
                      const char *filename,
                      const void *buf,
                      const size_t n,
                      const off_t whence)
{
    char path[256];
    int fd;
    int rv;
    off_t size;

    joinPath(dir, filename, path);

    fd = open(path, O_RDWR, S_IRUSR | S_IWUSR);

    munit_assert_int(fd, !=, -1);

    /* Get the size of the file */
    size = lseek(fd, 0, SEEK_END);

    if (whence == 0) {
        munit_assert_int(size, >=, n);
        lseek(fd, 0, SEEK_SET);
    } else if (whence > 0) {
        munit_assert_int(whence, <=, size);
        munit_assert_int(size - whence, >=, n);
        lseek(fd, whence, SEEK_SET);
    } else {
        munit_assert_int(-whence, <=, size);
        munit_assert_int(-whence, >=, n);
        lseek(fd, whence, SEEK_END);
    }

    rv = write(fd, buf, n);
    munit_assert_int(rv, ==, n);

    close(fd);
}

void DirTruncateFile(const char *dir, const char *filename, const size_t n)
{
    char path[256];
    int fd;
    int rv;

    joinPath(dir, filename, path);

    fd = open(path, O_RDWR, S_IRUSR | S_IWUSR);
    munit_assert_int(fd, !=, -1);

    rv = ftruncate(fd, n);
    munit_assert_int(rv, ==, 0);

    rv = close(fd);
    munit_assert_int(rv, ==, 0);
}

void DirGrowFile(const char *dir, const char *filename, const size_t n)
{
    char path[256];
    int fd;
    struct stat sb;
    void *buf;
    size_t size;
    int rv;

    joinPath(dir, filename, path);

    fd = open(path, O_RDWR, S_IRUSR | S_IWUSR);
    munit_assert_int(fd, !=, -1);

    rv = fstat(fd, &sb);
    munit_assert_int(rv, ==, 0);
    munit_assert_int(sb.st_size, <=, n);

    /* Fill with zeros. */
    lseek(fd, sb.st_size, SEEK_SET);
    size = n - sb.st_size;
    buf = munit_malloc(size);
    rv = write(fd, buf, size);
    munit_assert_int(rv, ==, size);
    free(buf);

    rv = close(fd);
    munit_assert_int(rv, ==, 0);
}

void DirRenameFile(const char *dir,
                   const char *filename1,
                   const char *filename2)
{
    char path1[256];
    char path2[256];
    int rv;

    joinPath(dir, filename1, path1);
    joinPath(dir, filename2, path2);

    rv = rename(path1, path2);
    munit_assert_int(rv, ==, 0);
}

void DirRemoveFile(const char *dir, const char *filename)
{
    char path[256];
    int rv;

    joinPath(dir, filename, path);
    rv = unlink(path);
    munit_assert_int(rv, ==, 0);
}

void DirReadFile(const char *dir,
                 const char *filename,
                 void *buf,
                 const size_t n)
{
    char path[256];
    int fd;
    int rv;

    joinPath(dir, filename, path);

    fd = open(path, O_RDONLY);
    if (fd == -1) {
        munit_logf(MUNIT_LOG_ERROR, "read file '%s': %s", path,
                   strerror(errno));
    }

    rv = read(fd, buf, n);
    munit_assert_int(rv, ==, n);

    close(fd);
}

void DirMakeUnexecutable(const char *dir)
{
    int rv;

    rv = chmod(dir, 0);
    munit_assert_int(rv, ==, 0);
}

void DirMakeUnwritable(const char *dir)
{
    int rv;

    rv = chmod(dir, 0500);
    munit_assert_int(rv, ==, 0);
}

void DirMakeFileUnreadable(const char *dir, const char *filename)
{
    char path[256];
    int rv;

    joinPath(dir, filename, path);

    rv = chmod(path, 0);
    munit_assert_int(rv, ==, 0);
}

bool DirHasFile(const char *dir, const char *filename)
{
    char path[256];
    int fd;

    joinPath(dir, filename, path);

    fd = open(path, O_RDONLY);
    if (fd == -1) {
        munit_assert_true(errno == ENOENT || errno == EACCES);
        return false;
    }

    close(fd);

    return true;
}

void DirFill(const char *dir, const size_t n)
{
    char path[256];
    const char *filename = ".fill";
    struct statvfs fs;
    size_t size;
    int fd;
    int rv;

    rv = statvfs(dir, &fs);
    munit_assert_int(rv, ==, 0);

    size = fs.f_bsize * fs.f_bavail;

    if (n > 0) {
        munit_assert_int(size, >=, n);
    }

    joinPath(dir, filename, path);

    fd = open(path, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    munit_assert_int(fd, !=, -1);

    rv = posix_fallocate(fd, 0, size - n);
    munit_assert_int(rv, ==, 0);

    /* If n is zero, make sure any further write fails with ENOSPC */
    if (n == 0) {
        char buf[4096];
        int i;

        rv = lseek(fd, 0, SEEK_END);
        munit_assert_int(rv, !=, -1);

        for (i = 0; i < 40; i++) {
            rv = write(fd, buf, sizeof buf);
            if (rv < 0) {
                break;
            }
        }

        munit_assert_int(rv, ==, -1);
        munit_assert_int(errno, ==, ENOSPC);
    }

    close(fd);
}
