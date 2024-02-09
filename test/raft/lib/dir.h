/* Test directory utilities.
 *
 * This module sports helpers to create temporary directories backed by various
 * file systems, read/write files in them, check for the presence of files
 * etc. */

#ifndef TEST_DIR_H
#define TEST_DIR_H

#include <sys/types.h>

#include "munit.h"

/* Munit parameter defining the file system type backing the temporary directory
 * created by test_dir_setup().
 *
 * The various file systems must have been previously setup with the fs.sh
 * script. */
#define DIR_FS_PARAM "dir-fs"

#define FIXTURE_DIR char *dir
#define SET_UP_DIR                                                      \
    f->dir = DirSetUp(params, user_data);                               \
    if (f->dir == NULL) { /* Fs not available, test must be skipped. */ \
        free(f);                                                        \
        return NULL;                                                    \
    }
#define TEAR_DOWN_DIR DirTearDown(f->dir)

/* Contain a single DIR_FS_PARAM parameter set to all supported file system
 * types. */
extern MunitParameterEnum DirAllParams[];

/* Contain a single DIR_FS_PARAM parameter set to tmpfs. */
extern MunitParameterEnum DirTmpfsParams[];

/* Contain a single DIR_FS_PARAM parameter set to all file systems with
 * proper AIO support (i.e. NOWAIT works). */
extern MunitParameterEnum DirAioParams[];

/* Contain a single DIR_FS_PARAM parameter set to all file systems without
 * proper AIO support (i.e. NOWAIT does not work). */
extern MunitParameterEnum DirNoAioParams[];

/* Create a temporary test directory.
 *
 * Return a pointer the path of the created directory. */
void *DirSetUp(const MunitParameter params[], void *user_data);

/* Create a temporary test directory backed by tmpfs.
 *
 * Return a pointer the path of the created directory, or NULL if no tmpfs file
 * system is available. */
void *DirTmpfsSetUp(const MunitParameter params[], void *user_data);

/* Create a temporary test directory backed by ext4.
 *
 * Return a pointer the path of the created directory, or NULL if no ext4 file
 * system is available. */
void *DirExt4SetUp(const MunitParameter params[], void *user_data);

/* Create a temporary test directory backed by btrfs.
 *
 * Return a pointer the path of the created directory, or NULL if no btrfs file
 * system is available. */
void *DirBtrfsSetUp(const MunitParameter params[], void *user_data);

/* Create a temporary test directory backed by zfs.
 *
 * Return a pointer the path of the created directory, or NULL if no zfs file
 * system is available. */
void *DirZfsSetUp(const MunitParameter params[], void *user_data);

/* Create a temporary test directory backed by xfs.
 *
 * Return a pointer the path of the created directory, or NULL if no xfs file
 * system is available. */
void *DirXfsSetUp(const MunitParameter params[], void *user_data);

/* Recursively remove a temporary directory. */
void DirTearDown(void *data);

/* Write the given @buf to the given @filename in the given @dir. */
void DirWriteFile(const char *dir,
                  const char *filename,
                  const void *buf,
                  const size_t n);

/* Write the given @filename and fill it with zeros. */
void DirWriteFileWithZeros(const char *dir,
                           const char *filename,
                           const size_t n);

/* Overwrite @n bytes of the given file with the given @buf data.
 *
 * If @whence is zero, overwrite the first @n bytes of the file. If @whence is
 * positive overwrite the @n bytes starting at offset @whence. If @whence is
 * negative overwrite @n bytes starting at @whence bytes from the end of the
 * file. */
void DirOverwriteFile(const char *dir,
                      const char *filename,
                      const void *buf,
                      const size_t n,
                      const off_t whence);

/* Truncate the given file, leaving only the first @n bytes. */
void DirTruncateFile(const char *dir, const char *filename, const size_t n);

/* Grow the given file to the given size, filling the new bytes with zeros. */
void DirGrowFile(const char *dir, const char *filename, const size_t n);

/* Rename a file in the given directory from filename1 to filename2. */
void DirRenameFile(const char *dir,
                   const char *filename1,
                   const char *filename2);

/* Remove a file. */
void DirRemoveFile(const char *dir, const char *filename);

/* Read into @buf the content of the given @filename in the given @dir. */
void DirReadFile(const char *dir,
                 const char *filename,
                 void *buf,
                 const size_t n);

/* Make the given directory not executable, so files can't be open. */
void DirMakeUnexecutable(const char *dir);

/* Make the given directory not writable. */
void DirMakeUnwritable(const char *dir);

/* Make the given file not readable. */
void DirMakeFileUnreadable(const char *dir, const char *filename);

/* Check if the given directory has the given file. */
bool DirHasFile(const char *dir, const char *filename);

/* Fill the underlying file system of the given dir, leaving only n bytes free.
 */
void DirFill(const char *dir, const size_t n);

#endif /* TEST_DIR_H */
