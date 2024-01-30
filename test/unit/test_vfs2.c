#include <errno.h>
#include <stdio.h>
#include <unistd.h>

#include <raft.h>
#include <sqlite3.h>

#include "../../include/dqlite.h"

#include "../lib/config.h"
#include "../lib/fs.h"
#include "../lib/heap.h"
#include "../lib/runner.h"
#include "../lib/sqlite.h"

#include "../../src/format.h"
#include "../../src/vfs2.h"

/******************************************************************************
 *
 * Fixture
 *
 ******************************************************************************/

#define VFS_PATH_SZ 512
struct fixture
{
	sqlite3_vfs *vfs;
	char *dir;
	char path[VFS_PATH_SZ];
};

static void vfsFillPath(struct fixture *f, char *filename)
{
	int rv;
	const char *dir = f->dir;
	if (dir != NULL) {
		rv = snprintf(f->path, VFS_PATH_SZ, "%s/%s", dir, filename);
	} else {
		rv = snprintf(f->path, VFS_PATH_SZ, "%s", filename);
	}
	munit_assert_int(rv, >, 0);
	munit_assert_int(rv, <, VFS_PATH_SZ);
}

static void setPageSize(sqlite3_file *f, unsigned page_size, int rv)
{
	int rc;
	char page_sz[32];
	rc = snprintf(page_sz, sizeof(page_sz), "%u", page_size);
	munit_assert_int(rc, >, 0);
	munit_assert_int(rc, <, sizeof(page_sz));

	char *fnctl[] = {
	    "",
	    "page_size",
	    "512",
	    "",
	};

	rc = f->pMethods->xFileControl(f, SQLITE_FCNTL_PRAGMA, fnctl);
	munit_assert_int(rc, ==, rv);
}

static void *setUp(const MunitParameter params[], void *user_data)
{
	struct fixture *f = munit_malloc(sizeof *f);
	int rv;

	SETUP_HEAP;
	SETUP_SQLITE;
	f->vfs = vfs2_make(sqlite3_vfs_find("unix"), "dqlite-vfs2", 0);
	munit_assert_ptr(f->vfs, !=, NULL);
	f->dir = test_dir_setup();
	rv = sqlite3_vfs_register(f->vfs, 0);
	munit_assert_int(rv, ==, SQLITE_OK);
	return f;
}

static void tearDown(void *data)
{
	struct fixture *f = data;
	test_dir_tear_down(f->dir);
	vfs2_destroy(f->vfs);
	TEAR_DOWN_SQLITE;
	TEAR_DOWN_HEAP;
	free(f);
}

/******************************************************************************
 *
 * Helpers
 *
 ******************************************************************************/

/* Helper for creating a new file */
static sqlite3_file *__file_create(sqlite3_vfs *vfs,
				   const char *name,
				   int type_flag)
{
	sqlite3_file *file = munit_malloc(vfs->szOsFile);

	int flags;
	int rc;

	flags = SQLITE_OPEN_EXCLUSIVE | SQLITE_OPEN_CREATE |
		SQLITE_OPEN_READWRITE | type_flag;

	rc = vfs->xOpen(vfs, name, file, flags, &flags);
	munit_assert_int(rc, ==, 0);

	return file;
}

/* Helper for creating a new database file */
static sqlite3_file *__file_create_main_db(struct fixture *f)
{
	vfsFillPath(f, "test.db");
	return __file_create(f->vfs, f->path, SQLITE_OPEN_MAIN_DB);
}

/* Helper for allocating a buffer of 100 bytes containing a database header with
 * a page size field set to 512 bytes. */
static void *__buf_header_main_db(void)
{
	char *buf = munit_malloc(100 * sizeof *buf);

	/* Set page size to 512. */
	buf[16] = 2;
	buf[17] = 0;

	return buf;
}

/* Helper for allocating a buffer with the content of the first page, i.e. the
 * the header and some other bytes. */
static void *__buf_page_1(void)
{
	char *buf = munit_malloc(512 * sizeof *buf);

	/* Set page size to 512. */
	buf[16] = 2;
	buf[17] = 0;

	/* Set some other bytes */
	buf[101] = 1;
	buf[256] = 2;
	buf[511] = 3;

	return buf;
}

/* Helper for allocating a buffer with the content of the second page. */
static void *__buf_page_2(void)
{
	char *buf = munit_malloc(512 * sizeof *buf);

	buf[0] = 4;
	buf[256] = 5;
	buf[511] = 6;

	return buf;
}

/******************************************************************************
 *
 * xOpen
 *
 ******************************************************************************/

SUITE(vfs2_open)

/* If the EXCLUSIVE and CREATE flag are given, and the file already exists, an
 * error is returned. */
TEST(vfs2_open, exclusive, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	sqlite3_file *file1 = munit_malloc(f->vfs->szOsFile);
	sqlite3_file *file2 = munit_malloc(f->vfs->szOsFile);

	int flags;
	int rc;

	(void)params;
	vfsFillPath(f, "test.db");

	flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB;
	rc = f->vfs->xOpen(f->vfs, f->path, file1, flags, &flags);

	munit_assert_int(rc, ==, SQLITE_OK);

	flags |= SQLITE_OPEN_EXCLUSIVE;
	rc = f->vfs->xOpen(f->vfs, f->path, file2, flags, &flags);

	munit_assert_int(rc, ==, SQLITE_CANTOPEN);
	munit_assert_int(EEXIST, ==, f->vfs->xGetLastError(f->vfs, 0, 0));

	rc = file2->pMethods->xClose(file2);
	munit_assert_int(rc, ==, SQLITE_OK);

	rc = file1->pMethods->xClose(file1);
	munit_assert_int(rc, ==, SQLITE_OK);

	free(file2);
	free(file1);

	return MUNIT_OK;
}

/* It's possible to open again a previously created file. In that case passing
 * SQLITE_OPEN_CREATE is not necessary. */
TEST(vfs2_open, again, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	sqlite3_file *file = munit_malloc(f->vfs->szOsFile);

	int flags;
	int rc;

	(void)params;
	vfsFillPath(f, "test.db");

	flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB;
	rc = f->vfs->xOpen(f->vfs, f->path, file, flags, &flags);

	munit_assert_int(rc, ==, SQLITE_OK);

	rc = file->pMethods->xClose(file);
	munit_assert_int(rc, ==, SQLITE_OK);

	flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB;
	rc = f->vfs->xOpen(f->vfs, f->path, file, flags, &flags);

	munit_assert_int(rc, ==, 0);

	rc = file->pMethods->xClose(file);
	munit_assert_int(rc, ==, SQLITE_OK);

	free(file);

	return MUNIT_OK;
}

/* If the file does not exist and the SQLITE_OPEN_CREATE flag is not passed, an
 * error is returned. */
TEST(vfs2_open, noent, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	sqlite3_file *file = munit_malloc(f->vfs->szOsFile);

	int flags;
	int rc;

	(void)params;
	vfsFillPath(f, "test.db");

	rc = f->vfs->xOpen(f->vfs, f->path, file, 0, &flags);

	munit_assert_int(rc, ==, SQLITE_CANTOPEN);
	munit_assert_int(ENOENT, ==, f->vfs->xGetLastError(f->vfs, 0, 0));

	rc = file->pMethods->xClose(file);
	munit_assert_int(rc, ==, SQLITE_OK);

	free(file);

	return MUNIT_OK;
}

/* Trying to open a WAL file before its main database file results in an
 * error. */
TEST(vfs2_open, walBeforeDb, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	sqlite3_file *file = munit_malloc(f->vfs->szOsFile);

	int flags;
	int rc;

	(void)params;
	vfsFillPath(f, "test.db-wal");

	flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_WAL;
	rc = f->vfs->xOpen(f->vfs, f->path, file, flags, &flags);

	munit_assert_int(rc, ==, SQLITE_IOERR_FSTAT);

	free(file);

	return MUNIT_OK;
}

/* Out of memory when creating the content structure for a new file. */
TEST(vfs2_open, oom, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	sqlite3_file *file = munit_malloc(f->vfs->szOsFile);
	int flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB;
	int rc;

	(void)params;
	vfsFillPath(f, "test.db");

	test_heap_fault_config(0, 1);
	test_heap_fault_enable();

	rc = f->vfs->xOpen(f->vfs, f->path, file, flags, &flags);
	munit_assert_int(rc, ==, SQLITE_NOMEM);

	free(file);

	return MUNIT_OK;
}

/* Out of memory when internally copying the filename. */
TEST(vfs2_open, oomFilename, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	sqlite3_file *file = munit_malloc(f->vfs->szOsFile);
	int flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB;
	int rc;

	(void)params;
	vfsFillPath(f, "test.db");

	test_heap_fault_config(1, 1);
	test_heap_fault_enable();

	rc = f->vfs->xOpen(f->vfs, f->path, file, flags, &flags);
	munit_assert_int(rc, ==, SQLITE_NOMEM);

	rc = file->pMethods->xClose(file);
	munit_assert_int(rc, ==, SQLITE_OK);

	free(file);

	return MUNIT_OK;
}

/* Open a temporary file. */
TEST(vfs2_open, tmp, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	sqlite3_file *file = munit_malloc(f->vfs->szOsFile);
	int flags = 0;
	char buf[16];
	int rc;

	(void)params;

	flags |= SQLITE_OPEN_CREATE;
	flags |= SQLITE_OPEN_READWRITE;
	flags |= SQLITE_OPEN_TEMP_JOURNAL;
	flags |= SQLITE_OPEN_DELETEONCLOSE;

	rc = f->vfs->xOpen(f->vfs, NULL, file, flags, &flags);
	munit_assert_int(rc, ==, SQLITE_OK);

	rc = file->pMethods->xWrite(file, "hello", 5, 0);
	munit_assert_int(rc, ==, SQLITE_OK);

	memset(buf, 0, sizeof buf);
	rc = file->pMethods->xRead(file, buf, 5, 0);
	munit_assert_int(rc, ==, SQLITE_OK);

	munit_assert_string_equal(buf, "hello");

	rc = file->pMethods->xClose(file);
	munit_assert_int(rc, ==, SQLITE_OK);

	free(file);

	return MUNIT_OK;
}

/******************************************************************************
 *
 * xDelete
 *
 ******************************************************************************/

SUITE(vfs2_delete)

/* Delete a file. */
TEST(vfs2_delete, success, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	sqlite3_file *file = munit_malloc(f->vfs->szOsFile);

	int flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB;
	int rc;

	(void)params;

	vfsFillPath(f, "test.db");
	rc = f->vfs->xOpen(f->vfs, f->path, file, flags, &flags);
	munit_assert_int(rc, ==, 0);

	rc = file->pMethods->xClose(file);
	munit_assert_int(rc, ==, 0);

	rc = f->vfs->xDelete(f->vfs, f->path, 0);
	munit_assert_int(rc, ==, 0);

	/* Trying to open the file again without the SQLITE_OPEN_CREATE flag
	 * results in an error. */
	rc = f->vfs->xOpen(f->vfs, f->path, file, 0, &flags);
	munit_assert_int(rc, ==, SQLITE_CANTOPEN);

	rc = file->pMethods->xClose(file);
	munit_assert_int(rc, ==, SQLITE_OK);

	free(file);

	return MUNIT_OK;
}

/* Trying to delete a non-existing file results in an error. */
TEST(vfs2_delete, enoent, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;

	int rc;

	(void)params;

	vfsFillPath(f, "test.db");
	rc = f->vfs->xDelete(f->vfs, f->path, 0);
	munit_assert_int(rc, ==, SQLITE_IOERR_DELETE_NOENT);
	munit_assert_int(ENOENT, ==, f->vfs->xGetLastError(f->vfs, 0, 0));

	return MUNIT_OK;
}

/******************************************************************************
 *
 * xAccess
 *
 ******************************************************************************/

SUITE(vfs2_access)

/* Accessing an existing file returns true. */
TEST(vfs2_access, success, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	sqlite3_file *file = munit_malloc(f->vfs->szOsFile);

	int flags =
	    SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB | SQLITE_OPEN_READWRITE;
	int rc;
	int exists;

	vfsFillPath(f, "test.db");
	rc = f->vfs->xOpen(f->vfs, f->path, file, flags, &flags);
	munit_assert_int(rc, ==, 0);

	setPageSize(file, 512, SQLITE_NOTFOUND);
	/* Write the first page, containing the header and some content. */
	void *buf_page_1 = __buf_page_1();
	rc = file->pMethods->xWrite(file, buf_page_1, 512, 0);
	munit_assert_int(rc, ==, 0);

	rc = file->pMethods->xClose(file);
	munit_assert_int(rc, ==, 0);

	rc = f->vfs->xAccess(f->vfs, f->path, SQLITE_ACCESS_EXISTS, &exists);
	munit_assert_int(rc, ==, 0);
	munit_assert_true(exists);

	free(file);
	free(buf_page_1);

	return MUNIT_OK;
}

/* Trying to access a non existing file returns false. */
TEST(vfs2_access, noent, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;

	int rc;
	int exists;

	(void)params;
	vfsFillPath(f, "test.db");

	rc = f->vfs->xAccess(f->vfs, f->path, SQLITE_ACCESS_EXISTS, &exists);
	munit_assert_int(rc, ==, 0);

	munit_assert_false(exists);

	return MUNIT_OK;
}

/******************************************************************************
 *
 * xClose
 *
 ******************************************************************************/

SUITE(vfs2_close)

/* Closing a file decreases its refcount so it's possible to delete it. */
TEST(vfs2_close, thenDelete, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	sqlite3_file *file = munit_malloc(f->vfs->szOsFile);

	int flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB;
	int rc;

	(void)params;
	vfsFillPath(f, "test.db");

	rc = f->vfs->xOpen(f->vfs, f->path, file, flags, &flags);
	munit_assert_int(rc, ==, 0);

	rc = file->pMethods->xClose(file);
	munit_assert_int(rc, ==, 0);

	rc = f->vfs->xDelete(f->vfs, f->path, 0);
	munit_assert_int(rc, ==, 0);

	free(file);

	return MUNIT_OK;
}

/******************************************************************************
 *
 * xRead
 *
 ******************************************************************************/

SUITE(vfs2_read)

/* Trying to read a file that was not written yet, results in an error. */
TEST(vfs2_read, neverWritten, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	sqlite3_file *file = __file_create_main_db(f);

	int rc;
	char buf[1] = {123};

	(void)params;

	rc = file->pMethods->xRead(file, (void *)buf, 1, 0);
	munit_assert_int(rc, ==, SQLITE_IOERR_SHORT_READ);

	/* The buffer gets filled with zero */
	munit_assert_int(buf[0], ==, 0);

	rc = file->pMethods->xClose(file);
	munit_assert_int(rc, ==, 0);

	free(file);

	return MUNIT_OK;
}

/******************************************************************************
 *
 * xWrite
 *
 ******************************************************************************/

SUITE(vfs2_write)

/* Write the header of the database file. */
TEST(vfs2_write, dbHeader, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	sqlite3_file *file = __file_create_main_db(f);

	void *buf = __buf_header_main_db();

	int rc;

	(void)params;

	rc = file->pMethods->xWrite(file, buf, 100, 0);
	munit_assert_int(rc, ==, 0);

	rc = file->pMethods->xClose(file);
	munit_assert_int(rc, ==, 0);

	free(file);
	free(buf);

	return MUNIT_OK;
}

/* Write the header of the database file, then the full first page and a second
 * page. */
TEST(vfs2_write, andReadPages, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	sqlite3_file *file = __file_create_main_db(f);

	int rc;
	char buf[512];
	void *buf_header_main = __buf_header_main_db();
	void *buf_page_1 = __buf_page_1();
	void *buf_page_2 = __buf_page_2();

	(void)params;

	memset(buf, 0, 512);

	/* Write the header. */
	rc = file->pMethods->xWrite(file, buf_header_main, 100, 0);
	munit_assert_int(rc, ==, 0);

	/* Write the first page, containing the header and some content. */
	rc = file->pMethods->xWrite(file, buf_page_1, 512, 0);
	munit_assert_int(rc, ==, 0);

	/* Set the page_size in disk_mode */
	setPageSize(file, 512, SQLITE_NOTFOUND);

	/* Write a second page. */
	rc = file->pMethods->xWrite(file, buf_page_2, 512, 512);
	munit_assert_int(rc, ==, 0);

	/* Read the page header. */
	rc = file->pMethods->xRead(file, (void *)buf, 512, 0);
	munit_assert_int(rc, ==, 0);

	munit_assert_int(buf[16], ==, 2);
	munit_assert_int(buf[17], ==, 0);
	munit_assert_int(buf[101], ==, 1);
	munit_assert_int(buf[256], ==, 2);
	munit_assert_int(buf[511], ==, 3);

	/* Read the second page. */
	memset(buf, 0, 512);
	rc = file->pMethods->xRead(file, (void *)buf, 512, 512);
	munit_assert_int(rc, ==, 0);

	munit_assert_int(buf[0], ==, 4);
	munit_assert_int(buf[256], ==, 5);
	munit_assert_int(buf[511], ==, 6);

	free(buf_header_main);
	free(buf_page_1);
	free(buf_page_2);
	rc = file->pMethods->xClose(file);
	munit_assert_int(rc, ==, 0);
	free(file);

	return MUNIT_OK;
}

/******************************************************************************
 *
 * xTruncate
 *
 ******************************************************************************/

SUITE(vfs2_truncate);

/* Truncate the main database file. */
TEST(vfs2_truncate, database, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	sqlite3_file *file = __file_create_main_db(f);
	void *buf_page_1 = __buf_page_1();
	void *buf_page_2 = __buf_page_2();

	int rc;

	sqlite_int64 size;

	(void)params;

	/* Initial size is 0. */
	rc = file->pMethods->xFileSize(file, &size);
	munit_assert_int(rc, ==, 0);
	munit_assert_int(size, ==, 0);

	/* Truncating an empty file is a no-op. */
	rc = file->pMethods->xTruncate(file, 0);
	munit_assert_int(rc, ==, 0);

	/* The size is still 0. */
	rc = file->pMethods->xFileSize(file, &size);
	munit_assert_int(rc, ==, 0);
	munit_assert_int(size, ==, 0);

	/* Set the page_size in disk_mode */
	setPageSize(file, 512, SQLITE_NOTFOUND);

	/* Write the first page, containing the header. */
	rc = file->pMethods->xWrite(file, buf_page_1, 512, 0);
	munit_assert_int(rc, ==, 0);

	/* Write a second page. */
	rc = file->pMethods->xWrite(file, buf_page_2, 512, 512);
	munit_assert_int(rc, ==, 0);

	/* The size is 1024. */
	rc = file->pMethods->xFileSize(file, &size);
	munit_assert_int(rc, ==, 0);
	munit_assert_int(size, ==, 1024);

	/* Truncate the second page. */
	rc = file->pMethods->xTruncate(file, 512);
	munit_assert_int(rc, ==, 0);

	/* The size is 512. */
	rc = file->pMethods->xFileSize(file, &size);
	munit_assert_int(rc, ==, 0);
	munit_assert_int(size, ==, 512);

	/* Truncate also the first. */
	rc = file->pMethods->xTruncate(file, 0);
	munit_assert_int(rc, ==, 0);

	/* The size is 0. */
	rc = file->pMethods->xFileSize(file, &size);
	munit_assert_int(rc, ==, 0);
	munit_assert_int(size, ==, 0);

	free(buf_page_1);
	free(buf_page_2);
	rc = file->pMethods->xClose(file);
	munit_assert_int(rc, ==, 0);
	free(file);

	return MUNIT_OK;
}

/* Truncating an empty file is a no-op. */
TEST(vfs2_truncate, empty, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	sqlite3_file *file = __file_create_main_db(f);
	sqlite_int64 size;
	int rc;

	(void)params;

	/* Truncating an empty file is a no-op. */
	rc = file->pMethods->xTruncate(file, 0);
	munit_assert_int(rc, ==, SQLITE_OK);

	/* Size is 0. */
	rc = file->pMethods->xFileSize(file, &size);
	munit_assert_int(rc, ==, 0);
	munit_assert_int(size, ==, 0);

	rc = file->pMethods->xClose(file);
	munit_assert_int(rc, ==, 0);
	free(file);

	return MUNIT_OK;
}

/******************************************************************************
 *
 * xShmMap
 *
 ******************************************************************************/

SUITE(vfs2_shm_map);

static char *test_shm_map_oom_delay[] = {"0", "1", NULL};
static char *test_shm_map_oom_repeat[] = {"1", NULL};

static MunitParameterEnum test_shm_map_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, test_shm_map_oom_delay},
    {TEST_HEAP_FAULT_REPEAT, test_shm_map_oom_repeat},
    {NULL, NULL},
};

/* Out of memory when trying to initialize the internal VFS shm data struct. */
TEST(vfs2_shm_map, oom, setUp, tearDown, 0, test_shm_map_oom_params)
{
	struct fixture *f = data;
	sqlite3_file *file = __file_create_main_db(f);
	volatile void *region;
	int rc;

	(void)params;
	(void)data;

	test_heap_fault_enable();

	rc = file->pMethods->xShmMap(file, 0, 32768, 1, &region);
	munit_assert_int(rc, ==, SQLITE_NOMEM);

	rc = file->pMethods->xClose(file);
	munit_assert_int(rc, ==, SQLITE_OK);

	free(file);

	return MUNIT_OK;
}

/******************************************************************************
 *
 * xShmLock
 *
 ******************************************************************************/

SUITE(vfs2_shm_lock)

/* If an exclusive lock is in place, getting a shared lock on any index of its
 * range fails. */
TEST(vfs2_shm_lock, sharedBusy, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	sqlite3_file *file = munit_malloc(f->vfs->szOsFile);
	int flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB;
	volatile void *region;
	int rc;

	(void)params;
	(void)data;

	vfsFillPath(f, "test.db");
	rc = f->vfs->xOpen(f->vfs, f->path, file, flags, &flags);
	munit_assert_int(rc, ==, 0);

	rc = file->pMethods->xShmMap(file, 0, 32768, 1, &region);
	munit_assert_int(rc, ==, 0);

	/* Take an exclusive lock on a range. */
	flags = SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE;
	rc = file->pMethods->xShmLock(file, 2, 3, flags);
	munit_assert_int(rc, ==, 0);

	/* Attempting to get a shared lock on an index in that range fails. */
	flags = SQLITE_SHM_LOCK | SQLITE_SHM_SHARED;
	rc = file->pMethods->xShmLock(file, 3, 1, flags);
	munit_assert_int(rc, ==, SQLITE_BUSY);

	rc = file->pMethods->xClose(file);
	munit_assert_int(rc, ==, 0);
	free(file);

	return MUNIT_OK;
}

/* If a shared lock is in place on any of the indexes of the requested range,
 * getting an exclusive lock fails. */
TEST(vfs2_shm_lock, exclBusy, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	sqlite3_file *file = munit_malloc(f->vfs->szOsFile);
	int flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB;
	volatile void *region;
	int rc;

	(void)params;
	(void)data;

	vfsFillPath(f, "test.db");

	rc = f->vfs->xOpen(f->vfs, f->path, file, flags, &flags);
	munit_assert_int(rc, ==, 0);

	rc = file->pMethods->xShmMap(file, 0, 32768, 1, &region);
	munit_assert_int(rc, ==, 0);

	/* Take a shared lock on index 3. */
	flags = SQLITE_SHM_LOCK | SQLITE_SHM_SHARED;
	rc = file->pMethods->xShmLock(file, 3, 1, flags);
	munit_assert_int(rc, ==, 0);

	/* Attempting to get an exclusive lock on a range that contains index 3
	 * fails. */
	flags = SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE;
	rc = file->pMethods->xShmLock(file, 2, 3, flags);
	munit_assert_int(rc, ==, SQLITE_BUSY);

	rc = file->pMethods->xClose(file);
	munit_assert_int(rc, ==, 0);
	free(file);

	return MUNIT_OK;
}

/* The native unix VFS implementation from SQLite allows to release a shared
 * memory lock without acquiring it first. */
TEST(vfs2_shm_lock, releaseUnix, setUp, tearDown, 0, NULL)
{
	(void)data;
	struct sqlite3_vfs *vfs = sqlite3_vfs_find("unix");
	sqlite3_file *file = munit_malloc(vfs->szOsFile);
	int flags =
	    SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB;
	char *dir = test_dir_setup();
	char buf[1024];
	char *path;
	volatile void *region;
	int rc;

	(void)params;
	(void)data;

	/* The SQLite pager stores the Database filename, Journal filename, and
	 * WAL filename consecutively in memory, in that order. The database
	 * filename is prefixed by four zero bytes. Emulate that behavior here,
	 * since the internal SQLite code triggered by the xShmMap unix
	 * implementation relies on that.*/
	memset(buf, 0, sizeof buf);
	path = buf + 4;
	sprintf(path, "%s/test.db", dir);

	rc = vfs->xOpen(vfs, path, file, flags, &flags);
	munit_assert_int(rc, ==, 0);

	rc = file->pMethods->xShmMap(file, 0, 32768, 1, &region);
	munit_assert_int(rc, ==, 0);

	flags = SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE;
	rc = file->pMethods->xShmLock(file, 3, 1, flags);
	munit_assert_int(rc, ==, 0);

	flags = SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED;
	rc = file->pMethods->xShmLock(file, 2, 1, flags);
	munit_assert_int(rc, ==, 0);

	rc = file->pMethods->xShmUnmap(file, 1);
	munit_assert_int(rc, ==, 0);

	rc = file->pMethods->xClose(file);
	munit_assert_int(rc, ==, 0);

	test_dir_tear_down(dir);

	free(file);

	return MUNIT_OK;
}

/* The dqlite VFS implementation allows to release a shared memory lock without
 * acquiring it first. This is important because at open time sometimes SQLite
 * will do just that (release before acquire). */
TEST(vfs2_shm_lock, release, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	sqlite3_file *file = munit_malloc(f->vfs->szOsFile);
	int flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB;
	volatile void *region;
	int rc;

	(void)params;
	(void)data;

	vfsFillPath(f, "test.db");
	rc = f->vfs->xOpen(f->vfs, f->path, file, flags, &flags);
	munit_assert_int(rc, ==, 0);

	rc = file->pMethods->xShmMap(file, 0, 32768, 1, &region);
	munit_assert_int(rc, ==, 0);

	flags = SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED;
	rc = file->pMethods->xShmLock(file, 3, 1, flags);
	munit_assert_int(rc, ==, 0);

	flags = SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED;
	rc = file->pMethods->xShmLock(file, 2, 1, flags);
	munit_assert_int(rc, ==, 0);

	rc = file->pMethods->xShmUnmap(file, 1);
	munit_assert_int(rc, ==, 0);

	rc = file->pMethods->xClose(file);
	munit_assert_int(rc, ==, 0);

	free(file);

	return MUNIT_OK;
}

/******************************************************************************
 *
 * xFileControl
 *
 ******************************************************************************/

SUITE(vfs2_file_control)

/* Trying to set the journal mode to anything other than "wal" produces an
 * error. */
TEST(vfs2_file_control, journal, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	sqlite3_file *file = __file_create_main_db(f);
	char *fnctl[] = {
	    "",
	    "journal_mode",
	    "memory",
	    "",
	};
	int rc;

	(void)params;
	(void)data;

	rc = file->pMethods->xFileControl(file, SQLITE_FCNTL_PRAGMA, fnctl);
	munit_assert_int(rc, ==, SQLITE_ERROR);

	rc = file->pMethods->xClose(file);
	munit_assert_int(rc, ==, 0);
	free(file);

	/* Free allocated memory from call to sqlite3_mprintf */
	sqlite3_free(fnctl[0]);

	return MUNIT_OK;
}
