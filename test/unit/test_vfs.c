#include <errno.h>
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
#include "../../src/vfs.h"

static char *bools[] = {"0", "1", NULL};

static MunitParameterEnum vfs_params[] = {
    {"disk_mode", bools},
    {NULL, NULL},
};

/******************************************************************************
 *
 * Fixture
 *
 ******************************************************************************/

#define VFS_PATH_SZ 512
struct fixture
{
	struct sqlite3_vfs vfs;
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

/* Sets the page_size in disk_mode. */
static void setPageSizeDisk(const MunitParameter params[],
			    sqlite3_file *f,
			    unsigned page_size,
			    int rv)
{
	int rc;
	bool disk_mode = false;
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

	const char *disk_mode_param = munit_parameters_get(params, "disk_mode");
	if (disk_mode_param != NULL) {
		disk_mode = (bool)atoi(disk_mode_param);
	}
	if (disk_mode) {
		rc = f->pMethods->xFileControl(f, SQLITE_FCNTL_PRAGMA, fnctl);
		munit_assert_int(rc, ==, rv);
	}
}

static void *setUp(const MunitParameter params[], void *user_data)
{
	struct fixture *f = munit_malloc(sizeof *f);
	int rv;
	bool disk_mode = false;

	SETUP_HEAP;
	SETUP_SQLITE;
	rv = VfsInit(&f->vfs, "dqlite");
	munit_assert_int(rv, ==, 0);
	f->dir = NULL;
	const char *disk_mode_param = munit_parameters_get(params, "disk_mode");
	if (disk_mode_param != NULL) {
		disk_mode = (bool)atoi(disk_mode_param);
	}
	if (disk_mode) {
		rv = VfsEnableDisk(&f->vfs);
		munit_assert_int(rv, ==, 0);
		f->dir = test_dir_setup();
	}
	rv = sqlite3_vfs_register(&f->vfs, 0);
	munit_assert_int(rv, ==, 0);
	return f;
}

static void tearDown(void *data)
{
	struct fixture *f = data;
	test_dir_tear_down(f->dir);
	sqlite3_vfs_unregister(&f->vfs);
	VfsClose(&f->vfs);
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
	return __file_create(&f->vfs, f->path, SQLITE_OPEN_MAIN_DB);
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

/* Helper to execute a SQL statement. */
static void __db_exec(sqlite3 *db, const char *sql)
{
	int rc;

	rc = sqlite3_exec(db, sql, NULL, NULL, NULL);
	munit_assert_int(rc, ==, SQLITE_OK);
}

/* Helper to open and initialize a database, setting the page size and
 * WAL mode. */
static sqlite3 *__db_open(void)
{
	sqlite3 *db;
	int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	int rc;

	rc = sqlite3_open_v2("test.db", &db, flags, "dqlite");
	munit_assert_int(rc, ==, SQLITE_OK);

	__db_exec(db, "PRAGMA page_size=512");
	__db_exec(db, "PRAGMA synchronous=OFF");
	__db_exec(db, "PRAGMA journal_mode=WAL");

	return db;
}

/* Helper to close a database. */
static void __db_close(sqlite3 *db)
{
	int rv;
	rv = sqlite3_close(db);
	munit_assert_int(rv, ==, SQLITE_OK);
}

/* Helper get the mxFrame value of the WAL index object associated with the
 * given database. */
static uint32_t __wal_idx_mx_frame(sqlite3 *db)
{
	sqlite3_file *file;
	volatile void *region;
	uint32_t mx_frame;
	int rc;

	rc = sqlite3_file_control(db, "main", SQLITE_FCNTL_FILE_POINTER, &file);
	munit_assert_int(rc, ==, SQLITE_OK);

	rc = file->pMethods->xShmMap(file, 0, 0, 0, &region);
	munit_assert_int(rc, ==, SQLITE_OK);

	/* The mxFrame number is 16th byte of the WAL index header. See also
	 * https://sqlite.org/walformat.html. */
	mx_frame = ((uint32_t *)region)[4];

	return mx_frame;
}

/* Helper get the read mark array of the WAL index object associated with the
 * given database. */
static uint32_t *__wal_idx_read_marks(sqlite3 *db)
{
	sqlite3_file *file;
	volatile void *region;
	uint32_t *idx;
	uint32_t *marks;
	int rc;

	marks = munit_malloc(FORMAT__WAL_NREADER * sizeof *marks);

	rc = sqlite3_file_control(db, "main", SQLITE_FCNTL_FILE_POINTER, &file);
	munit_assert_int(rc, ==, SQLITE_OK);

	rc = file->pMethods->xShmMap(file, 0, 0, 0, &region);
	munit_assert_int(rc, ==, SQLITE_OK);

	/* The read-mark array starts at the 100th byte of the WAL index
	 * header. See also https://sqlite.org/walformat.html. */
	idx = (uint32_t *)region;
	memcpy(marks, &idx[25], (sizeof *idx) * FORMAT__WAL_NREADER);

	return marks;
}

/* Helper that returns true if the i'th lock of the shared memmory reagion
 * associated with the given database is currently held. */
static int __shm_shared_lock_held(sqlite3 *db, int i)
{
	sqlite3_file *file;
	int flags;
	int locked;
	int rc;

	rc = sqlite3_file_control(db, "main", SQLITE_FCNTL_FILE_POINTER, &file);
	munit_assert_int(rc, ==, SQLITE_OK);

	/* Try to acquire an exclusive lock, which will fail if the shared lock
	 * is held. */
	flags = SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE;
	rc = file->pMethods->xShmLock(file, i, 1, flags);

	locked = rc == SQLITE_BUSY;

	if (rc == SQLITE_OK) {
		flags = SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE;
		rc = file->pMethods->xShmLock(file, i, 1, flags);
		munit_assert_int(rc, ==, SQLITE_OK);
	}

	return locked;
}

/******************************************************************************
 *
 * xOpen
 *
 ******************************************************************************/

SUITE(VfsOpen)

/* If the EXCLUSIVE and CREATE flag are given, and the file already exists, an
 * error is returned. */
TEST(VfsOpen, exclusive, setUp, tearDown, 0, vfs_params)
{
	struct fixture *f = data;
	sqlite3_file *file1 = munit_malloc(f->vfs.szOsFile);
	sqlite3_file *file2 = munit_malloc(f->vfs.szOsFile);

	int flags;
	int rc;

	(void)params;
	vfsFillPath(f, "test.db");

	flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB;
	rc = f->vfs.xOpen(&f->vfs, f->path, file1, flags, &flags);

	munit_assert_int(rc, ==, SQLITE_OK);

	flags |= SQLITE_OPEN_EXCLUSIVE;
	rc = f->vfs.xOpen(&f->vfs, f->path, file2, flags, &flags);

	munit_assert_int(rc, ==, SQLITE_CANTOPEN);
	munit_assert_int(EEXIST, ==, f->vfs.xGetLastError(&f->vfs, 0, 0));

	rc = file1->pMethods->xClose(file1);
	munit_assert_int(rc, ==, SQLITE_OK);

	free(file2);
	free(file1);

	return MUNIT_OK;
}

/* It's possible to open again a previously created file. In that case passing
 * SQLITE_OPEN_CREATE is not necessary. */
TEST(VfsOpen, again, setUp, tearDown, 0, vfs_params)
{
	struct fixture *f = data;
	sqlite3_file *file = munit_malloc(f->vfs.szOsFile);

	int flags;
	int rc;

	(void)params;
	vfsFillPath(f, "test.db");

	flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB;
	rc = f->vfs.xOpen(&f->vfs, f->path, file, flags, &flags);

	munit_assert_int(rc, ==, SQLITE_OK);

	rc = file->pMethods->xClose(file);
	munit_assert_int(rc, ==, SQLITE_OK);

	flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB;
	rc = f->vfs.xOpen(&f->vfs, f->path, file, flags, &flags);

	munit_assert_int(rc, ==, 0);

	rc = file->pMethods->xClose(file);
	munit_assert_int(rc, ==, SQLITE_OK);

	free(file);

	return MUNIT_OK;
}

/* If the file does not exist and the SQLITE_OPEN_CREATE flag is not passed, an
 * error is returned. */
TEST(VfsOpen, noent, setUp, tearDown, 0, vfs_params)
{
	struct fixture *f = data;
	sqlite3_file *file = munit_malloc(f->vfs.szOsFile);

	int flags;
	int rc;

	(void)params;
	vfsFillPath(f, "test.db");

	rc = f->vfs.xOpen(&f->vfs, f->path, file, 0, &flags);

	munit_assert_int(rc, ==, SQLITE_CANTOPEN);
	munit_assert_int(ENOENT, ==, f->vfs.xGetLastError(&f->vfs, 0, 0));

	free(file);

	return MUNIT_OK;
}

/* Trying to open a WAL file before its main database file results in an
 * error. */
TEST(VfsOpen, walBeforeDb, setUp, tearDown, 0, vfs_params)
{
	struct fixture *f = data;
	sqlite3_file *file = munit_malloc(f->vfs.szOsFile);

	int flags;
	int rc;

	(void)params;
	vfsFillPath(f, "test.db");

	flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_WAL;
	rc = f->vfs.xOpen(&f->vfs, f->path, file, flags, &flags);

	munit_assert_int(rc, ==, SQLITE_CANTOPEN);

	free(file);

	return MUNIT_OK;
}

/* Trying to run queries against a database that hasn't turned off the
 * synchronous flag results in an error. */
TEST(VfsOpen, synchronous, setUp, tearDown, 0, vfs_params)
{
	struct fixture *f = data;
	sqlite3 *db;
	int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	int rc;

	(void)params;
	vfsFillPath(f, "test.db");

	rc = sqlite3_vfs_register(&f->vfs, 0);
	munit_assert_int(rc, ==, SQLITE_OK);

	rc = sqlite3_open_v2(f->path, &db, flags, f->vfs.zName);
	munit_assert_int(rc, ==, SQLITE_OK);

	__db_exec(db, "PRAGMA page_size=4092");

	rc = sqlite3_exec(db, "PRAGMA journal_mode=WAL", NULL, NULL, NULL);
	munit_assert_int(rc, ==, SQLITE_IOERR);

	munit_assert_string_equal(sqlite3_errmsg(db), "disk I/O error");

	__db_close(db);

	rc = sqlite3_vfs_unregister(&f->vfs);
	munit_assert_int(rc, ==, SQLITE_OK);

	return MUNIT_OK;
}

/* Out of memory when creating the content structure for a new file. */
TEST(VfsOpen, oom, setUp, tearDown, 0, vfs_params)
{
	struct fixture *f = data;
	sqlite3_file *file = munit_malloc(f->vfs.szOsFile);
	int flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB;
	int rc;

	(void)params;
	vfsFillPath(f, "test.db");

	test_heap_fault_config(0, 1);
	test_heap_fault_enable();

	rc = f->vfs.xOpen(&f->vfs, f->path, file, flags, &flags);
	munit_assert_int(rc, ==, SQLITE_CANTOPEN);

	free(file);

	return MUNIT_OK;
}

/* Out of memory when internally copying the filename. */
TEST(VfsOpen, oomFilename, setUp, tearDown, 0, vfs_params)
{
	struct fixture *f = data;
	sqlite3_file *file = munit_malloc(f->vfs.szOsFile);
	int flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB;
	int rc;

	(void)params;
	vfsFillPath(f, "test.db");

	test_heap_fault_config(1, 1);
	test_heap_fault_enable();

	rc = f->vfs.xOpen(&f->vfs, f->path, file, flags, &flags);
	munit_assert_int(rc, ==, SQLITE_CANTOPEN);

	free(file);

	return MUNIT_OK;
}

/* Open a temporary file. */
TEST(VfsOpen, tmp, setUp, tearDown, 0, vfs_params)
{
	struct fixture *f = data;
	sqlite3_file *file = munit_malloc(f->vfs.szOsFile);
	int flags = 0;
	char buf[16];
	int rc;

	(void)params;

	flags |= SQLITE_OPEN_CREATE;
	flags |= SQLITE_OPEN_READWRITE;
	flags |= SQLITE_OPEN_TEMP_JOURNAL;
	flags |= SQLITE_OPEN_DELETEONCLOSE;

	rc = f->vfs.xOpen(&f->vfs, NULL, file, flags, &flags);
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

SUITE(VfsDelete)

/* Delete a file. */
TEST(VfsDelete, success, setUp, tearDown, 0, vfs_params)
{
	struct fixture *f = data;
	sqlite3_file *file = munit_malloc(f->vfs.szOsFile);

	int flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB;
	int rc;

	(void)params;

	vfsFillPath(f, "test.db");
	rc = f->vfs.xOpen(&f->vfs, f->path, file, flags, &flags);
	munit_assert_int(rc, ==, 0);

	rc = file->pMethods->xClose(file);
	munit_assert_int(rc, ==, 0);

	rc = f->vfs.xDelete(&f->vfs, f->path, 0);
	munit_assert_int(rc, ==, 0);

	/* Trying to open the file again without the SQLITE_OPEN_CREATE flag
	 * results in an error. */
	rc = f->vfs.xOpen(&f->vfs, f->path, file, 0, &flags);
	munit_assert_int(rc, ==, SQLITE_CANTOPEN);

	free(file);

	return MUNIT_OK;
}

/* Trying to delete a non-existing file results in an error. */
TEST(VfsDelete, enoent, setUp, tearDown, 0, vfs_params)
{
	struct fixture *f = data;

	int rc;

	(void)params;

	vfsFillPath(f, "test.db");
	rc = f->vfs.xDelete(&f->vfs, f->path, 0);
	munit_assert_int(rc, ==, SQLITE_IOERR_DELETE_NOENT);
	munit_assert_int(ENOENT, ==, f->vfs.xGetLastError(&f->vfs, 0, 0));

	return MUNIT_OK;
}

/******************************************************************************
 *
 * xAccess
 *
 ******************************************************************************/

SUITE(VfsAccess)

/* Accessing an existing file returns true. */
TEST(VfsAccess, success, setUp, tearDown, 0, vfs_params)
{
	struct fixture *f = data;
	sqlite3_file *file = munit_malloc(f->vfs.szOsFile);

	int flags =
	    SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB | SQLITE_OPEN_READWRITE;
	int rc;
	int exists;

	vfsFillPath(f, "test.db");
	rc = f->vfs.xOpen(&f->vfs, f->path, file, flags, &flags);
	munit_assert_int(rc, ==, 0);

	setPageSizeDisk(params, file, 512, SQLITE_NOTFOUND);
	/* Write the first page, containing the header and some content. */
	void *buf_page_1 = __buf_page_1();
	rc = file->pMethods->xWrite(file, buf_page_1, 512, 0);
	munit_assert_int(rc, ==, 0);

	rc = file->pMethods->xClose(file);
	munit_assert_int(rc, ==, 0);

	rc = f->vfs.xAccess(&f->vfs, f->path, SQLITE_ACCESS_EXISTS, &exists);
	munit_assert_int(rc, ==, 0);
	munit_assert_true(exists);

	free(file);
	free(buf_page_1);

	return MUNIT_OK;
}

/* Trying to access a non existing file returns false. */
TEST(VfsAccess, noent, setUp, tearDown, 0, vfs_params)
{
	struct fixture *f = data;

	int rc;
	int exists;

	(void)params;
	vfsFillPath(f, "test.db");

	rc = f->vfs.xAccess(&f->vfs, f->path, SQLITE_ACCESS_EXISTS, &exists);
	munit_assert_int(rc, ==, 0);

	munit_assert_false(exists);

	return MUNIT_OK;
}

/******************************************************************************
 *
 * xFullPathname
 *
 ******************************************************************************/

SUITE(VfsFullPathname);

/* The xFullPathname API returns the filename unchanged. */
TEST(VfsFullPathname, success, setUp, tearDown, 0, vfs_params)
{
	struct fixture *f = data;

	int rc;
	char pathname[10];

	(void)params;

	rc = f->vfs.xFullPathname(&f->vfs, "test.db", 10, pathname);
	munit_assert_int(rc, ==, 0);

	munit_assert_string_equal(pathname, "test.db");

	return MUNIT_OK;
}

/******************************************************************************
 *
 * xClose
 *
 ******************************************************************************/

SUITE(VfsClose)

/* Closing a file decreases its refcount so it's possible to delete it. */
TEST(VfsClose, thenDelete, setUp, tearDown, 0, vfs_params)
{
	struct fixture *f = data;
	sqlite3_file *file = munit_malloc(f->vfs.szOsFile);

	int flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB;
	int rc;

	(void)params;
	vfsFillPath(f, "test.db");

	rc = f->vfs.xOpen(&f->vfs, f->path, file, flags, &flags);
	munit_assert_int(rc, ==, 0);

	rc = file->pMethods->xClose(file);
	munit_assert_int(rc, ==, 0);

	rc = f->vfs.xDelete(&f->vfs, f->path, 0);
	munit_assert_int(rc, ==, 0);

	free(file);

	return MUNIT_OK;
}

/******************************************************************************
 *
 * xRead
 *
 ******************************************************************************/

SUITE(VfsRead)

/* Trying to read a file that was not written yet, results in an error. */
TEST(VfsRead, neverWritten, setUp, tearDown, 0, vfs_params)
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

SUITE(VfsWrite)

/* Write the header of the database file. */
TEST(VfsWrite, dbHeader, setUp, tearDown, 0, vfs_params)
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
TEST(VfsWrite, andReadPages, setUp, tearDown, 0, vfs_params)
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
	setPageSizeDisk(params, file, 512, SQLITE_NOTFOUND);

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

/* Out of memory when trying to create a new page. */
TEST(VfsWrite, oomPage, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	sqlite3_file *file = __file_create_main_db(f);
	void *buf_header_main = __buf_header_main_db();
	char buf[512];
	int rc;

	test_heap_fault_config(0, 1);
	test_heap_fault_enable();

	(void)params;

	memset(buf, 0, 512);

	/* Write the database header, which triggers creating the first page. */
	rc = file->pMethods->xWrite(file, buf_header_main, 100, 0);
	munit_assert_int(rc, ==, SQLITE_NOMEM);

	free(buf_header_main);
	free(file);

	return MUNIT_OK;
}

/* Out of memory when trying to append a new page to the internal page array of
 * the content object. */
TEST(VfsWrite, oomPageArray, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	sqlite3_file *file = __file_create_main_db(f);
	void *buf_header_main = __buf_header_main_db();
	char buf[512];
	int rc;

	test_heap_fault_config(1, 1);
	test_heap_fault_enable();

	(void)params;

	memset(buf, 0, 512);

	/* Write the database header, which triggers creating the first page. */
	rc = file->pMethods->xWrite(file, buf_header_main, 100, 0);
	munit_assert_int(rc, ==, SQLITE_NOMEM);

	free(buf_header_main);
	free(file);

	return MUNIT_OK;
}

/* Out of memory when trying to create the content buffer of a new page. */
TEST(VfsWrite, oomPageBuf, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	sqlite3_file *file = __file_create_main_db(f);
	void *buf_header_main = __buf_header_main_db();
	char buf[512];
	int rc;

	test_heap_fault_config(1, 1);
	test_heap_fault_enable();

	(void)params;

	memset(buf, 0, 512);

	/* Write the database header, which triggers creating the first page. */
	rc = file->pMethods->xWrite(file, buf_header_main, 100, 0);
	munit_assert_int(rc, ==, SQLITE_NOMEM);

	free(buf_header_main);
	free(file);

	return MUNIT_OK;
}

/* Trying to write two pages beyond the last one results in an error. */
TEST(VfsWrite, beyondLast, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	sqlite3_file *file = __file_create_main_db(f);
	void *buf_page_1 = __buf_page_1();
	void *buf_page_2 = __buf_page_2();
	char buf[512];
	int rc;

	(void)params;

	memset(buf, 0, 512);

	/* Write the first page. */
	rc = file->pMethods->xWrite(file, buf_page_1, 512, 0);
	munit_assert_int(rc, ==, 0);

	/* Write the third page, without writing the second. */
	rc = file->pMethods->xWrite(file, buf_page_2, 512, 1024);
	munit_assert_int(rc, ==, SQLITE_IOERR_WRITE);

	free(buf_page_1);
	free(buf_page_2);

	free(file);

	return MUNIT_OK;
}

/******************************************************************************
 *
 * xTruncate
 *
 ******************************************************************************/

SUITE(VfsTruncate);

/* Truncate the main database file. */
TEST(VfsTruncate, database, setUp, tearDown, 0, vfs_params)
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
	setPageSizeDisk(params, file, 512, SQLITE_NOTFOUND);

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

/* Truncating a file which is not the main db file or the WAL file produces an
 * error. */
TEST(VfsTruncate, unexpected, setUp, tearDown, 0, vfs_params)
{
	struct fixture *f = data;
	sqlite3_file *main_db = __file_create_main_db(f);
	sqlite3_file *file = munit_malloc(f->vfs.szOsFile);
	int flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_JOURNAL;
	char buf[32];
	char journal[512];
	int rc;

	(void)params;

	/* Open a journal file. */
	rc = snprintf(journal, sizeof(journal), "%s-journal", f->path);
	munit_assert_int(rc, >, 0);
	munit_assert_int(rc, <, sizeof(journal));
	rc = f->vfs.xOpen(&f->vfs, journal, file, flags, &flags);
	munit_assert_int(rc, ==, 0);

	/* Write some content. */
	rc = file->pMethods->xWrite(file, buf, 32, 0);
	munit_assert_int(rc, ==, 0);

	/* Truncating produces an error. */
	rc = file->pMethods->xTruncate(file, 0);
	munit_assert_int(rc, ==, SQLITE_IOERR_TRUNCATE);

	rc = file->pMethods->xClose(main_db);
	munit_assert_int(rc, ==, 0);
	free(main_db);

	rc = file->pMethods->xClose(file);
	munit_assert_int(rc, ==, 0);
	free(file);

	return MUNIT_OK;
}

/* Truncating an empty file is a no-op. */
TEST(VfsTruncate, empty, setUp, tearDown, 0, vfs_params)
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

/* Trying to grow an empty file produces an error. */
TEST(VfsTruncate, emptyGrow, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	sqlite3_file *file = __file_create_main_db(f);
	int rc;

	(void)params;

	/* Truncating an empty file is a no-op. */
	rc = file->pMethods->xTruncate(file, 512);
	munit_assert_int(rc, ==, SQLITE_IOERR_TRUNCATE);

	rc = file->pMethods->xClose(file);
	munit_assert_int(rc, ==, 0);
	free(file);

	return MUNIT_OK;
}

/* Trying to truncate a main database file to a size which is not a multiple of
 * the page size produces an error. */
TEST(VfsTruncate, misaligned, setUp, tearDown, 0, NULL)
{
	struct fixture *f = data;
	sqlite3_file *file = __file_create_main_db(f);
	void *buf_page_1 = __buf_page_1();

	int rc;

	(void)params;

	/* Write the first page, containing the header. */
	rc = file->pMethods->xWrite(file, buf_page_1, 512, 0);
	munit_assert_int(rc, ==, 0);

	/* Truncating to an invalid size. */
	rc = file->pMethods->xTruncate(file, 400);
	munit_assert_int(rc, ==, SQLITE_IOERR_TRUNCATE);

	free(buf_page_1);
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

SUITE(VfsShmMap);

static char *test_shm_map_oom_delay[] = {"0", "1", NULL};
static char *test_shm_map_oom_repeat[] = {"1", NULL};

static MunitParameterEnum test_shm_map_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, test_shm_map_oom_delay},
    {TEST_HEAP_FAULT_REPEAT, test_shm_map_oom_repeat},
    {NULL, NULL},
};

/* Out of memory when trying to initialize the internal VFS shm data struct. */
TEST(VfsShmMap, oom, setUp, tearDown, 0, test_shm_map_oom_params)
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

	free(file);

	return MUNIT_OK;
}

/******************************************************************************
 *
 * xShmLock
 *
 ******************************************************************************/

SUITE(VfsShmLock)

/* If an exclusive lock is in place, getting a shared lock on any index of its
 * range fails. */
TEST(VfsShmLock, sharedBusy, setUp, tearDown, 0, vfs_params)
{
	struct fixture *f = data;
	sqlite3_file *file = munit_malloc(f->vfs.szOsFile);
	int flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB;
	volatile void *region;
	int rc;

	(void)params;
	(void)data;

	vfsFillPath(f, "test.db");
	rc = f->vfs.xOpen(&f->vfs, f->path, file, flags, &flags);
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
TEST(VfsShmLock, exclBusy, setUp, tearDown, 0, vfs_params)
{
	struct fixture *f = data;
	sqlite3_file *file = munit_malloc(f->vfs.szOsFile);
	int flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB;
	volatile void *region;
	int rc;

	(void)params;
	(void)data;

	vfsFillPath(f, "test.db");

	rc = f->vfs.xOpen(&f->vfs, f->path, file, flags, &flags);
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
TEST(VfsShmLock, releaseUnix, setUp, tearDown, 0, vfs_params)
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
TEST(VfsShmLock, release, setUp, tearDown, 0, vfs_params)
{
	struct fixture *f = data;
	sqlite3_file *file = munit_malloc(f->vfs.szOsFile);
	int flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB;
	volatile void *region;
	int rc;

	(void)params;
	(void)data;

	vfsFillPath(f, "test.db");
	rc = f->vfs.xOpen(&f->vfs, f->path, file, flags, &flags);
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

SUITE(VfsFileControl)

/* Trying to set the journal mode to anything other than "wal" produces an
 * error. */
TEST(VfsFileControl, journal, setUp, tearDown, 0, vfs_params)
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

	/* Setting the page size a first time returns NOTFOUND, which is what
	 * SQLite effectively expects. */
	rc = file->pMethods->xFileControl(file, SQLITE_FCNTL_PRAGMA, fnctl);
	munit_assert_int(rc, ==, SQLITE_IOERR);

	rc = file->pMethods->xClose(file);
	munit_assert_int(rc, ==, 0);
	free(file);

	/* Free allocated memory from call to sqlite3_mprintf */
	sqlite3_free(fnctl[0]);

	return MUNIT_OK;
}

/******************************************************************************
 *
 * xCurrentTime
 *
 ******************************************************************************/

SUITE(VfsCurrentTime)

TEST(VfsCurrentTime, success, setUp, tearDown, 0, vfs_params)
{
	struct fixture *f = data;
	double now;
	int rc;

	(void)params;

	rc = f->vfs.xCurrentTime(&f->vfs, &now);
	munit_assert_int(rc, ==, SQLITE_OK);

	munit_assert_double(now, >, 0);

	return MUNIT_OK;
}

/******************************************************************************
 *
 * xSleep
 *
 ******************************************************************************/

SUITE(VfsSleep)

/* The xSleep implementation is a no-op. */
TEST(VfsSleep, success, setUp, tearDown, 0, vfs_params)
{
	struct fixture *f = data;
	int microseconds;

	(void)params;

	microseconds = f->vfs.xSleep(&f->vfs, 123);

	munit_assert_int(microseconds, ==, 123);

	return MUNIT_OK;
}

/******************************************************************************
 *
 * VfsInit
 *
 ******************************************************************************/

SUITE(VfsInit);

static char *test_create_oom_delay[] = {"0", NULL};
static char *test_create_oom_repeat[] = {"1", NULL};

static MunitParameterEnum test_create_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, test_create_oom_delay},
    {TEST_HEAP_FAULT_REPEAT, test_create_oom_repeat},
    {NULL, NULL},
};

TEST(VfsInit, oom, setUp, tearDown, 0, test_create_oom_params)
{
	struct sqlite3_vfs vfs;
	int rv;

	(void)params;
	(void)data;

	test_heap_fault_enable();

	rv = VfsInit(&vfs, "dqlite");
	munit_assert_int(rv, ==, DQLITE_NOMEM);

	return MUNIT_OK;
}

/******************************************************************************
 *
 * Integration
 *
 ******************************************************************************/

SUITE(VfsIntegration)

/* Test our expections on the memory-mapped WAl index format. */
TEST(VfsIntegration, wal, setUp, tearDown, 0, vfs_params)
{
	sqlite3 *db1;
	sqlite3 *db2;
	uint32_t *read_marks;
	int i;

	(void)data;
	(void)params;

	return MUNIT_SKIP;

	db1 = __db_open();
	db2 = __db_open();

	__db_exec(db1, "CREATE TABLE test (n INT)");

	munit_assert_int(__wal_idx_mx_frame(db1), ==, 2);

	read_marks = __wal_idx_read_marks(db1);
	munit_assert_uint32(read_marks[0], ==, 0);
	munit_assert_uint32(read_marks[1], ==, 0);
	munit_assert_uint32(read_marks[2], ==, 0xffffffff);
	munit_assert_uint32(read_marks[3], ==, 0xffffffff);
	munit_assert_uint32(read_marks[4], ==, 0xffffffff);
	free(read_marks);

	/* Start a read transaction on db2 */
	__db_exec(db2, "BEGIN");
	__db_exec(db2, "SELECT * FROM test");

	/* The max frame is set to 2, which is the current size of the WAL. */
	munit_assert_int(__wal_idx_mx_frame(db2), ==, 2);

	/* The starting mx frame value has been saved in the read marks */
	read_marks = __wal_idx_read_marks(db2);
	munit_assert_uint32(read_marks[0], ==, 0);
	munit_assert_uint32(read_marks[1], ==, 2);
	munit_assert_uint32(read_marks[2], ==, 0xffffffff);
	munit_assert_uint32(read_marks[3], ==, 0xffffffff);
	munit_assert_uint32(read_marks[4], ==, 0xffffffff);
	free(read_marks);

	/* A shared lock is held on the second read mark (read locks start at
	 * 3). */
	munit_assert_true(__shm_shared_lock_held(db2, 3 + 1));

	/* Start a write transaction on db1 */
	__db_exec(db1, "BEGIN");

	for (i = 0; i < 100; i++) {
		__db_exec(db1, "INSERT INTO test(n) VALUES(1)");
	}

	/* The mx frame is still 2 since the transaction is not committed. */
	munit_assert_int(__wal_idx_mx_frame(db1), ==, 2);

	/* No extra read mark wal taken. */
	read_marks = __wal_idx_read_marks(db1);
	munit_assert_uint32(read_marks[0], ==, 0);
	munit_assert_uint32(read_marks[1], ==, 2);
	munit_assert_uint32(read_marks[2], ==, 0xffffffff);
	munit_assert_uint32(read_marks[3], ==, 0xffffffff);
	munit_assert_uint32(read_marks[4], ==, 0xffffffff);
	free(read_marks);

	__db_exec(db1, "COMMIT");

	/* The mx frame is now 6. */
	munit_assert_int(__wal_idx_mx_frame(db1), ==, 6);

	/* The old read lock is still in place. */
	munit_assert_true(__shm_shared_lock_held(db2, 3 + 1));

	/* Start a read transaction on db1 */
	__db_exec(db1, "BEGIN");
	__db_exec(db1, "SELECT * FROM test");

	/* The mx frame is still unchanged. */
	munit_assert_int(__wal_idx_mx_frame(db1), ==, 6);

	/* A new read mark was taken. */
	read_marks = __wal_idx_read_marks(db1);
	munit_assert_uint32(read_marks[0], ==, 0);
	munit_assert_uint32(read_marks[1], ==, 2);
	munit_assert_uint32(read_marks[2], ==, 6);
	munit_assert_uint32(read_marks[3], ==, 0xffffffff);
	munit_assert_uint32(read_marks[4], ==, 0xffffffff);
	free(read_marks);

	/* The old read lock is still in place. */
	munit_assert_true(__shm_shared_lock_held(db2, 3 + 1));

	/* The new read lock is in place as well. */
	munit_assert_true(__shm_shared_lock_held(db2, 3 + 2));

	__db_close(db1);
	__db_close(db2);

	return SQLITE_OK;
}

/* Full checkpoints are possible only when no read mark is set. */
TEST(VfsIntegration, checkpoint, setUp, tearDown, 0, vfs_params)
{
	sqlite3 *db1;
	sqlite3 *db2;
	sqlite3_file *file1; /* main DB file */
	sqlite3_file *file2; /* WAL file */
	sqlite_int64 size;
	uint32_t *read_marks;
	unsigned mx_frame;
	char stmt[128];
	int log, ckpt;
	int i;
	int rv;

	(void)data;
	(void)params;

	return MUNIT_SKIP;

	db1 = __db_open();

	__db_exec(db1, "CREATE TABLE test (n INT)");

	/* Insert a few rows so we grow the size of the WAL. */
	__db_exec(db1, "BEGIN");

	for (i = 0; i < 500; i++) {
		sprintf(stmt, "INSERT INTO test(n) VALUES(%d)", i);
		__db_exec(db1, stmt);
	}

	__db_exec(db1, "COMMIT");

	/* Get the file objects for the main database and the WAL. */
	rv = sqlite3_file_control(db1, "main", SQLITE_FCNTL_FILE_POINTER,
				  &file1);
	munit_assert_int(rv, ==, 0);

	rv = sqlite3_file_control(db1, "main", SQLITE_FCNTL_JOURNAL_POINTER,
				  &file2);
	munit_assert_int(rv, ==, 0);

	/* The WAL file has now 13 pages */
	rv = file2->pMethods->xFileSize(file2, &size);
	munit_assert_int(formatWalCalcFramesNumber(512, size), ==, 13);

	mx_frame = __wal_idx_mx_frame(db1);
	munit_assert_int(mx_frame, ==, 13);

	/* Start a read transaction on a different connection, acquiring a
	 * shared lock on all WAL pages. */
	db2 = __db_open();
	__db_exec(db2, "BEGIN");
	__db_exec(db2, "SELECT * FROM test");

	read_marks = __wal_idx_read_marks(db1);
	munit_assert_int(read_marks[1], ==, 13);
	free(read_marks);

	rv = file1->pMethods->xShmLock(file1, 3 + 1, 1,
				       SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE);
	munit_assert_int(rv, ==, SQLITE_BUSY);

	munit_assert_true(__shm_shared_lock_held(db1, 3 + 1));

	/* Execute a new write transaction, deleting some of the pages we
	 * inserted and creating new ones. */
	__db_exec(db1, "BEGIN");
	__db_exec(db1, "DELETE FROM test WHERE n > 200");

	for (i = 0; i < 1000; i++) {
		sprintf(stmt, "INSERT INTO test(n) VALUES(%d)", i);
		__db_exec(db1, stmt);
	}

	__db_exec(db1, "COMMIT");

	/* Since there's a shared read lock, a full checkpoint will fail. */
	rv = sqlite3_wal_checkpoint_v2(db1, "main", SQLITE_CHECKPOINT_TRUNCATE,
				       &log, &ckpt);
	munit_assert_int(rv, !=, 0);

	/* If we complete the read transaction the shared lock is realeased and
	 * the checkpoint succeeds. */
	__db_exec(db2, "COMMIT");

	rv = sqlite3_wal_checkpoint_v2(db1, "main", SQLITE_CHECKPOINT_TRUNCATE,
				       &log, &ckpt);
	munit_assert_int(rv, ==, 0);

	__db_close(db1);
	__db_close(db2);

	return SQLITE_OK;
}
