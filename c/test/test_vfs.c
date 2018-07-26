#include <errno.h>

#include <sqlite3.h>

#include "../include/dqlite.h"
#include "../src/vfs.h"

#include "leak.h"
#include "munit.h"

/******************************************************************************
 *
 * Helpers
 *
 ******************************************************************************/

/* Helper for creating a new database file */
static sqlite3_file *__file_create_main_db(sqlite3_vfs *vfs) {
	sqlite3_file *file = munit_malloc(vfs->szOsFile);

	int flags;
	int rc;

	flags = SQLITE_OPEN_EXCLUSIVE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB;

	rc = vfs->xOpen(vfs, "test.db", file, flags, &flags);
	munit_assert_int(rc, ==, 0);

	return file;
}

/* Helper for creating a new WAL file */
static sqlite3_file *__file_create_wal(sqlite3_vfs *vfs) {
	sqlite3_file *file = munit_malloc(vfs->szOsFile);

	int flags;
	int rc;

	flags = SQLITE_OPEN_EXCLUSIVE | SQLITE_OPEN_CREATE | SQLITE_OPEN_WAL;

	rc = vfs->xOpen(vfs, "test.db-wal", file, flags, &flags);
	munit_assert_int(rc, ==, 0);

	return file;
}

/* Helper for allocating a buffer of 100 bytes containing a database header with
 * a page size field set to 512 bytes. */
static const void *__buf_header_main_db() {
	char *buf = munit_malloc(100 * sizeof *buf);

	/* Set page size to 512. */
	buf[16] = 2;
	buf[17] = 0;

	return buf;
}

/* Helper for allocating a buffer of 32 bytes containing a WAL header with
 * a page size field set to 512 bytes. */
static const void *__buf_header_wal() {
	char *buf = munit_malloc(32 * sizeof *buf);

	/* Set page size to 512. */
	buf[10] = 2;
	buf[11] = 0;

	return buf;
}

/* Helper for allocating a buffer of 24 bytes containing a WAL frame header. */
static const void *__buf_header_wal_frame() {
	char *buf = munit_malloc(24 * sizeof *buf);

	return buf;
}

/* Helper for allocating a buffer with the content of the first page, i.e. the
 * the header and some other bytes. */
static const void *__buf_page_1() {
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
static const void *__buf_page_2() {
	char *buf = munit_malloc(512 * sizeof *buf);

	buf[0]   = 4;
	buf[256] = 5;
	buf[511] = 6;

	return buf;
}

/* Helper to execute a SQL statement. */
static void __db_exec(sqlite3 *db, const char *sql) {
	int   rc;
	char *errmsg;

	rc = sqlite3_exec(db, sql, NULL, NULL, &errmsg);
	munit_assert_int(rc, ==, SQLITE_OK);
}

/* Helper to open and initialize a database, setting the page size and
 * WAL mode. */
static sqlite3 *__db_open() {
	int      rc;
	sqlite3 *db;

	rc = sqlite3_open_v2(
	    "test.db", &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "volatile");
	munit_assert_int(rc, ==, SQLITE_OK);

	__db_exec(db, "PRAGMA page_size=512");
	__db_exec(db, "PRAGMA synchronous=OFF");
	__db_exec(db, "PRAGMA journal_mode=WAL");

	return db;
}

/* Helper get the mxFrame value of the WAL index object associated with the given
 * database. */
static uint32_t __wal_idx_mx_frame(sqlite3 *db) {
	uint32_t mx_frame;
	int      rc;

	rc = sqlite3_file_control(
	    db, "main", DQLITE__VFS_FCNTL_WAL_IDX_MX_FRAME, &mx_frame);
	munit_assert_int(rc, ==, SQLITE_OK);

	return mx_frame;
}

/* Helper get the read mark array of the WAL index object associated with the given
 * database. */
static uint32_t *__wal_idx_read_marks(sqlite3 *db) {
	uint32_t *read_marks = munit_malloc(5 * (sizeof *read_marks));
	int       rc;

	rc = sqlite3_file_control(
	    db, "main", DQLITE__VFS_FCNTL_WAL_IDX_READ_MARKS, read_marks);
	munit_assert_int(rc, ==, SQLITE_OK);

	return read_marks;
}

/* Helper that returns true if the i'th lock of the shared memmory reagion
 * associated with the given database is currently held. */
static int __shm_shared_lock_held(sqlite3 *db, int i) {
	sqlite3_file *file;
	int           flags;
	int           locked;
	int           rc;

	rc = sqlite3_file_control(db, "main", SQLITE_FCNTL_FILE_POINTER, &file);
	munit_assert_int(rc, ==, SQLITE_OK);

	/* Try to acquire an exclusive lock, which will fail if the shared lock is
	 * held. */
	flags = SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE;
	rc    = file->pMethods->xShmLock(file, i, 1, flags);

	locked = rc == SQLITE_BUSY;

	if (rc == SQLITE_OK) {
		flags = SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE;
		rc    = file->pMethods->xShmLock(file, i, 1, flags);
		munit_assert_int(rc, ==, SQLITE_OK);
	}

	return locked;
}

/******************************************************************************
 *
 * Setup and tear down
 *
 ******************************************************************************/

static void *setup(const MunitParameter params[], void *user_data) {
	sqlite3_vfs *vfs;

	int rc;

	(void)params;
	(void)user_data;

	rc = dqlite_vfs_register("volatile", &vfs);
	if (rc != 0) {
		munit_errorf(
		    "failed to register vfs: %s - %d", sqlite3_errstr(rc), rc);
	}

	return vfs;
}

static void tear_down(void *data) {
	sqlite3_vfs *vfs = data;

	dqlite_vfs_unregister(vfs);

	test_assert_no_leaks();
}

/******************************************************************************
 *
 * dqlite__vfs_open
 *
 ******************************************************************************/

/* If the EXCLUSIVE and CREATE flag are given, and the file already exists, an
 * error is returned. */
static MunitResult test_open_exclusive(const MunitParameter params[], void *data) {
	sqlite3_vfs * vfs  = data;
	sqlite3_file *file = munit_malloc(vfs->szOsFile);

	int flags;
	int rc;

	(void)params;

	flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB;
	rc    = vfs->xOpen(vfs, "test.db", file, flags, &flags);

	munit_assert_int(rc, ==, SQLITE_OK);

	flags = SQLITE_OPEN_EXCLUSIVE | SQLITE_OPEN_CREATE;
	rc    = vfs->xOpen(vfs, "test.db", file, flags, &flags);

	munit_assert_int(rc, ==, SQLITE_CANTOPEN);
	munit_assert_int(EEXIST, ==, vfs->xGetLastError(vfs, 0, 0));

	return MUNIT_OK;
}

/* It's possible to open again a previously created file. In that case passing
 * SQLITE_OPEN_CREATE is not necessary. */
static MunitResult test_open_again(const MunitParameter params[], void *data) {
	sqlite3_vfs * vfs  = data;
	sqlite3_file *file = munit_malloc(vfs->szOsFile);

	int flags;
	int rc;

	(void)params;

	flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB;
	rc    = vfs->xOpen(vfs, "test.db", file, flags, &flags);

	munit_assert_int(rc, ==, SQLITE_OK);

	rc = file->pMethods->xClose(file);
	munit_assert_int(rc, ==, SQLITE_OK);

	flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_MAIN_DB;
	rc    = vfs->xOpen(vfs, "test.db", file, flags, &flags);

	munit_assert_int(rc, ==, 0);

	return MUNIT_OK;
}

/* If the file does not exist and the SQLITE_OPEN_CREATE flag is not passed, an
 * error is returned. */
static MunitResult test_open_noent(const MunitParameter params[], void *data) {
	sqlite3_vfs * vfs  = data;
	sqlite3_file *file = munit_malloc(vfs->szOsFile);

	int flags;
	int rc;

	(void)params;

	rc = vfs->xOpen(vfs, "test.db", file, 0, &flags);

	munit_assert_int(rc, ==, SQLITE_CANTOPEN);
	munit_assert_int(ENOENT, ==, vfs->xGetLastError(vfs, 0, 0));

	return MUNIT_OK;
}

/* There's an hard-coded limit for the number of files that can be opened. */
static MunitResult test_open_enfile(const MunitParameter params[], void *data) {
	sqlite3_vfs * vfs  = data;
	sqlite3_file *file = munit_malloc(vfs->szOsFile);

	int  flags;
	int  rc;
	int  i;
	char name[20];

	(void)params;

	flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB;

	for (i = 0; i < 64; i++) {
		sprintf(name, "test-%d.db", i);
		rc = vfs->xOpen(vfs, name, file, flags, &flags);
		munit_assert_int(rc, ==, 0);
	}

	rc = vfs->xOpen(vfs, "test-64.db", file, flags, &flags);

	munit_assert_int(rc, ==, SQLITE_CANTOPEN);
	munit_assert_int(ENFILE, ==, vfs->xGetLastError(vfs, 0, 0));

	return MUNIT_OK;
}

/* Trying to open a WAL file before its main database file results in an
 * error.. */
static MunitResult test_open_wal_before_db(const MunitParameter params[],
                                           void *               data) {
	sqlite3_vfs * vfs  = data;
	sqlite3_file *file = munit_malloc(vfs->szOsFile);

	int flags;
	int rc;

	(void)params;

	flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_WAL;
	rc    = vfs->xOpen(vfs, "test.db", file, flags, &flags);

	munit_assert_int(rc, ==, SQLITE_CORRUPT);

	return MUNIT_OK;
}

static MunitTest dqlite__vfs_open_tests[] = {
    {"/exclusive", test_open_exclusive, setup, tear_down, 0, NULL},
    {"/again", test_open_again, setup, tear_down, 0, NULL},
    {"/noent", test_open_noent, setup, tear_down, 0, NULL},
    {"/enfile", test_open_enfile, setup, tear_down, 0, NULL},
    {"/wal-before-db", test_open_wal_before_db, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * dqlite__vfs_delete
 *
 ******************************************************************************/

/* Delete a file. */
static MunitResult test_delete(const MunitParameter params[], void *data) {
	sqlite3_vfs * vfs  = data;
	sqlite3_file *file = munit_malloc(vfs->szOsFile);

	int flags;
	int rc;

	(void)params;

	rc = vfs->xOpen(vfs, "test.db", file, SQLITE_OPEN_CREATE, &flags);
	munit_assert_int(rc, ==, 0);

	rc = file->pMethods->xClose(file);
	munit_assert_int(rc, ==, 0);

	rc = vfs->xDelete(vfs, "test.db", 0);
	munit_assert_int(rc, ==, 0);

	/* Trying to open the file again without the SQLITE_OPEN_CREATE flag
	 * results in an error. */
	rc = vfs->xOpen(vfs, "test.db", file, 0, &flags);
	munit_assert_int(rc, ==, SQLITE_CANTOPEN);

	return MUNIT_OK;
}

/* Attempt to delete a file with open file descriptors. */
static MunitResult test_delete_busy(const MunitParameter params[], void *data) {
	sqlite3_vfs * vfs  = data;
	sqlite3_file *file = munit_malloc(vfs->szOsFile);

	int flags;
	int rc;

	(void)params;

	rc = vfs->xOpen(vfs, "test.db", file, SQLITE_OPEN_CREATE, &flags);
	munit_assert_int(rc, ==, 0);

	rc = vfs->xDelete(vfs, "test.db", 0);
	munit_assert_int(rc, ==, SQLITE_IOERR_DELETE);
	munit_assert_int(EBUSY, ==, vfs->xGetLastError(vfs, 0, 0));

	rc = file->pMethods->xClose(file);
	munit_assert_int(rc, ==, 0);

	return MUNIT_OK;
}

/* Trying to delete a non-existing file results in an error. */
static MunitResult test_delete_enoent(const MunitParameter params[], void *data) {
	sqlite3_vfs *vfs = data;

	int rc;

	(void)params;

	rc = vfs->xDelete(vfs, "test.db", 0);
	munit_assert_int(rc, ==, SQLITE_IOERR_DELETE_NOENT);
	munit_assert_int(ENOENT, ==, vfs->xGetLastError(vfs, 0, 0));

	return MUNIT_OK;
}

static MunitTest dqlite__vfs_delete_tests[] = {
    {"", test_delete, setup, tear_down, 0, NULL},
    {"/busy", test_delete_busy, setup, tear_down, 0, NULL},
    {"/enoent", test_delete_enoent, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * dqlite__vfs_access
 *
 ******************************************************************************/

/* Accessing an existing file returns true. */
static MunitResult test_access(const MunitParameter params[], void *data) {
	sqlite3_vfs * vfs  = data;
	sqlite3_file *file = munit_malloc(vfs->szOsFile);

	int flags;
	int rc;
	int exists;

	(void)params;

	rc = vfs->xOpen(vfs, "test.db", file, SQLITE_OPEN_CREATE, &flags);

	munit_assert_int(rc, ==, 0);

	rc = file->pMethods->xClose(file);
	munit_assert_int(rc, ==, 0);

	rc = vfs->xAccess(vfs, "test.db", 0, &exists);
	munit_assert_int(rc, ==, 0);

	munit_assert_true(exists);

	return MUNIT_OK;
}

/* Trying to access a non existing file returns false. */
static MunitResult test_access_noent(const MunitParameter params[], void *data) {
	sqlite3_vfs *vfs = data;

	int rc;
	int exists;

	(void)params;

	rc = vfs->xAccess(vfs, "test.db", 0, &exists);
	munit_assert_int(rc, ==, 0);

	munit_assert_false(exists);

	return MUNIT_OK;
}

static MunitTest dqlite__vfs_access_tests[] = {
    {"", test_access, setup, tear_down, 0, NULL},
    {"/noent", test_access_noent, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * dqlite__vfs_full_pathname
 *
 ******************************************************************************/

/* The xFullPathname API returns the filename unchanged. */
static MunitResult test_full_pathname(const MunitParameter params[], void *data) {
	sqlite3_vfs *vfs = data;

	int  rc;
	char pathname[10];

	(void)params;

	rc = vfs->xFullPathname(vfs, "test.db", 10, pathname);
	munit_assert_int(rc, ==, 0);

	munit_assert_string_equal(pathname, "test.db");

	return MUNIT_OK;
}

static MunitTest dqlite__vfs_full_pathname_tests[] = {
    {"", test_full_pathname, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * dqlite__vfs_close
 *
 ******************************************************************************/

/* Closing a file decreases its refcount so it's possible to delete it. */
static MunitResult test_close_then_delete(const MunitParameter params[],
                                          void *               data) {
	sqlite3_vfs * vfs  = data;
	sqlite3_file *file = munit_malloc(vfs->szOsFile);

	int flags;
	int rc;

	(void)params;

	rc = vfs->xOpen(vfs, "test.db", file, SQLITE_OPEN_CREATE, &flags);
	munit_assert_int(rc, ==, 0);

	rc = file->pMethods->xClose(file);
	munit_assert_int(rc, ==, 0);

	rc = vfs->xDelete(vfs, "test.db", 0);
	munit_assert_int(rc, ==, 0);

	return MUNIT_OK;
}

static MunitTest dqlite__vfs_close_tests[] = {
    {"/then-delete", test_close_then_delete, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * dqlite__vfs_read
 *
 ******************************************************************************/

/* Trying to read a file that was not written yet, results in an error. */
static MunitResult test_read_never_written(const MunitParameter params[],
                                           void *               data) {
	sqlite3_vfs * vfs  = data;
	sqlite3_file *file = __file_create_main_db(vfs);

	int  rc;
	char buf[1] = {123};

	(void)params;

	rc = file->pMethods->xRead(file, (void *)buf, 1, 0);
	munit_assert_int(rc, ==, SQLITE_IOERR_SHORT_READ);

	/* The buffer gets filled with zero */
	munit_assert_int(buf[0], ==, 0);

	return MUNIT_OK;
}

static MunitTest dqlite__vfs_read_tests[] = {
    {"/never-written", test_read_never_written, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * dqlite__vfs_write
 *
 ******************************************************************************/

/* Write the header of the database file. */
static MunitResult test_write_database_header(const MunitParameter params[],
                                              void *               data) {
	sqlite3_vfs * vfs  = data;
	sqlite3_file *file = __file_create_main_db(vfs);

	const void *buf = __buf_header_main_db();

	int rc;

	(void)params;

	rc = file->pMethods->xWrite(file, buf, 100, 0);
	munit_assert_int(rc, ==, 0);

	return MUNIT_OK;
}

/* Write the header of the database file, then the full first page and a second
 * page. */
static MunitResult test_write_and_read_database_pages(const MunitParameter params[],
                                                      void *               data) {
	sqlite3_vfs * vfs  = data;
	sqlite3_file *file = __file_create_main_db(vfs);

	int  rc;
	char buf[512];

	(void)params;

	memset(buf, 0, 512);

	/* Write the header. */
	rc = file->pMethods->xWrite(file, __buf_header_main_db(), 100, 0);
	munit_assert_int(rc, ==, 0);

	/* Write the first page, containing the header and some other content. */
	rc = file->pMethods->xWrite(file, __buf_page_1(), 512, 0);
	munit_assert_int(rc, ==, 0);

	/* Write a second page. */
	rc = file->pMethods->xWrite(file, __buf_page_2(), 512, 512);
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

	return MUNIT_OK;
}

/* Write the header of a WAL file, then two frames. */
static MunitResult test_write_and_read_wal_frames(const MunitParameter params[],
                                                  void *               data) {
	sqlite3_vfs * vfs   = data;
	sqlite3_file *file1 = __file_create_main_db(vfs);
	sqlite3_file *file2 = __file_create_wal(vfs);

	int  rc;
	char buf[512];

	(void)params;

	memset(buf, 0, 512);

	/* First write the main database header, which sets the page size. */
	rc = file1->pMethods->xWrite(file1, __buf_header_main_db(), 100, 0);
	munit_assert_int(rc, ==, 0);

	/* Open the associated WAL file and write the WAL header. */
	rc = file2->pMethods->xWrite(file2, __buf_header_wal(), 32, 0);
	munit_assert_int(rc, ==, 0);

	/* Write the header of the first frame. */
	rc = file2->pMethods->xWrite(file2, __buf_header_wal_frame(), 24, 32);
	munit_assert_int(rc, ==, 0);

	/* Write the page of the first frame. */
	rc = file2->pMethods->xWrite(file2, __buf_page_1(), 512, 32 + 24);
	munit_assert_int(rc, ==, 0);

	/* Write the header of the second frame. */
	rc = file2->pMethods->xWrite(
	    file2, __buf_header_wal_frame(), 24, 32 + 24 + 512);
	munit_assert_int(rc, ==, 0);

	/* Write the page of the second frame. */
	rc = file2->pMethods->xWrite(file2, __buf_page_2(), 512, 32 + 24 + 512 + 24);
	munit_assert_int(rc, ==, 0);

	/* Read the WAL header. */
	rc = file2->pMethods->xRead(file2, (void *)buf, 32, 0);
	munit_assert_int(rc, ==, 0);

	/* Read the header of the first frame. */
	rc = file2->pMethods->xRead(file2, (void *)buf, 24, 32);
	munit_assert_int(rc, ==, 0);

	/* Read the page of the first frame. */
	rc = file2->pMethods->xRead(file2, (void *)buf, 512, 32 + 24);
	munit_assert_int(rc, ==, 0);

	/* Read the header of the second frame. */
	rc = file2->pMethods->xRead(file2, (void *)buf, 24, 32 + 24 + 512);
	munit_assert_int(rc, ==, 0);

	/* Read the page of the second frame. */
	rc = file2->pMethods->xRead(file2, (void *)buf, 512, 32 + 24 + 512 + 24);
	munit_assert_int(rc, ==, 0);

	return MUNIT_OK;
}

static MunitTest dqlite__vfs_write_tests[] = {
    {"/db-header", test_write_database_header, setup, tear_down, 0, NULL},
    {"/db-pages", test_write_and_read_database_pages, setup, tear_down, 0, NULL},
    {"/wal-frames", test_write_and_read_wal_frames, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * dqlite__vfs_truncate
 *
 ******************************************************************************/

/* Truncate the main database file. */
static MunitResult test_truncate_database(const MunitParameter params[],
                                          void *               data) {
	sqlite3_vfs * vfs  = data;
	sqlite3_file *file = __file_create_main_db(vfs);

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

	/* Write the first page, containing the header. */
	rc = file->pMethods->xWrite(file, __buf_page_1(), 512, 0);
	munit_assert_int(rc, ==, 0);

	/* Write a second page. */
	rc = file->pMethods->xWrite(file, __buf_page_2(), 512, 512);
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

	return MUNIT_OK;
}

/* Truncate the WAL file. */
static MunitResult test_truncate_wal(const MunitParameter params[], void *data) {
	sqlite3_vfs * vfs   = data;
	sqlite3_file *file1 = __file_create_main_db(vfs);
	sqlite3_file *file2 = __file_create_wal(vfs);

	int rc;

	sqlite3_int64 size;

	(void)params;

	/* First write the main database header, which sets the page size. */
	rc = file1->pMethods->xWrite(file1, __buf_header_main_db(), 100, 0);
	munit_assert_int(rc, ==, 0);

	/* Initial size of the WAL file is 0. */
	rc = file2->pMethods->xFileSize(file2, &size);
	munit_assert_int(rc, ==, 0);
	munit_assert_int(size, ==, 0);

	/* Truncating an empty WAL file is a no-op. */
	rc = file2->pMethods->xTruncate(file2, 0);
	munit_assert_int(rc, ==, 0);

	/* The size is still 0. */
	rc = file2->pMethods->xFileSize(file2, &size);
	munit_assert_int(rc, ==, 0);
	munit_assert_int(size, ==, 0);

	/* Write the WAL header. */
	rc = file2->pMethods->xWrite(file2, __buf_header_wal(), 32, 0);
	munit_assert_int(rc, ==, 0);

	/* Write the header of the first frame. */
	rc = file2->pMethods->xWrite(file2, __buf_header_wal_frame(), 24, 32);
	munit_assert_int(rc, ==, 0);

	/* Write the page of the first frame. */
	rc = file2->pMethods->xWrite(file2, __buf_page_1(), 512, 32 + 24);
	munit_assert_int(rc, ==, 0);

	/* Write the header of the second frame. */
	rc = file2->pMethods->xWrite(
	    file2, __buf_header_wal_frame(), 24, 32 + 24 + 512);
	munit_assert_int(rc, ==, 0);

	/* Write the page of the second frame. */
	rc = file2->pMethods->xWrite(file2, __buf_page_2(), 512, 32 + 24 + 512 + 24);
	munit_assert_int(rc, ==, 0);

	/* The size is 1104. */
	rc = file2->pMethods->xFileSize(file2, &size);
	munit_assert_int(rc, ==, 0);
	munit_assert_int(size, ==, 1104);

	/* Truncate the WAL file. */
	rc = file2->pMethods->xTruncate(file2, 0);
	munit_assert_int(rc, ==, 0);

	/* The size is 0. */
	rc = file2->pMethods->xFileSize(file2, &size);
	munit_assert_int(rc, ==, 0);
	munit_assert_int(size, ==, 0);

	return MUNIT_OK;
}

static MunitTest dqlite__vfs_truncate_tests[] = {
    {"/database", test_truncate_database, setup, tear_down, 0, NULL},
    {"/wal", test_truncate_wal, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * dqlite__vfs_register
 *
 ******************************************************************************/

/* If an exclusive lock is in place, getting a shared lock on any index of its
 * range fails. */
static MunitResult test_shm_lock_shared_busy(const MunitParameter params[],
                                             void *               data) {
	sqlite3_vfs *  vfs   = data;
	sqlite3_file * file  = munit_malloc(vfs->szOsFile);
	int            flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB;
	volatile void *region;
	int            rc;

	(void)params;
	(void)data;

	rc = vfs->xOpen(vfs, "test.db", file, flags, &flags);
	munit_assert_int(rc, ==, 0);

	rc = file->pMethods->xShmMap(file, 0, 512, 1, &region);
	munit_assert_int(rc, ==, 0);

	/* Take an exclusive lock on a range. */
	flags = SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE;
	rc    = file->pMethods->xShmLock(file, 2, 3, flags);
	munit_assert_int(rc, ==, 0);

	/* Attempting to get a shared lock on an index in that range fails. */
	flags = SQLITE_SHM_LOCK | SQLITE_SHM_SHARED;
	rc    = file->pMethods->xShmLock(file, 3, 1, flags);
	munit_assert_int(rc, ==, SQLITE_BUSY);

	return MUNIT_OK;
}

/* If a shared lock is in place on any of the indexes of the requested range,
 * getting an exclusive lock fails. */
static MunitResult test_shm_lock_excl_busy(const MunitParameter params[],
                                           void *               data) {
	sqlite3_vfs *  vfs   = data;
	sqlite3_file * file  = munit_malloc(vfs->szOsFile);
	int            flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB;
	volatile void *region;
	int            rc;

	(void)params;
	(void)data;

	rc = vfs->xOpen(vfs, "test.db", file, flags, &flags);
	munit_assert_int(rc, ==, 0);

	rc = file->pMethods->xShmMap(file, 0, 512, 1, &region);
	munit_assert_int(rc, ==, 0);

	/* Take a shared lock on index 3. */
	flags = SQLITE_SHM_LOCK | SQLITE_SHM_SHARED;
	rc    = file->pMethods->xShmLock(file, 3, 1, flags);
	munit_assert_int(rc, ==, 0);

	/* Attempting to get an exclusive lock on a range that contains index 3
	 * fails. */
	flags = SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE;
	rc    = file->pMethods->xShmLock(file, 2, 3, flags);
	munit_assert_int(rc, ==, SQLITE_BUSY);

	return MUNIT_OK;
}

static MunitTest dqlite__vfs_shm_lock_tests[] = {
    {"/shared-busy", test_shm_lock_excl_busy, setup, tear_down, 0, NULL},
    {"/excl-busy", test_shm_lock_shared_busy, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * dqlite__vfs_register
 *
 ******************************************************************************/

/* Integration test, registering an in-memory VFS and performing various
 * database operations. */
static MunitResult test_register(const MunitParameter params[], void *data) {
	int           rc;
	sqlite3 *     db;
	sqlite3_stmt *stmt;
	const char *  tail;
	int           i;
	int           size;
	int           ckpt;

	(void)data;
	(void)params;

	db = __db_open();

	/* Create a test table and insert a few rows into it. */
	__db_exec(db, "CREATE TABLE test (n INT)");

	rc = sqlite3_prepare(db, "INSERT INTO test(n) VALUES(?)", -1, &stmt, &tail);
	munit_assert_int(rc, ==, SQLITE_OK);

	for (i = 0; i < 100; i++) {
		rc = sqlite3_bind_int(stmt, 1, i);
		munit_assert_int(rc, ==, SQLITE_OK);

		rc = sqlite3_step(stmt);
		munit_assert_int(rc, ==, SQLITE_DONE);

		rc = sqlite3_reset(stmt);
		munit_assert_int(rc, ==, SQLITE_OK);
	}

	rc = sqlite3_finalize(stmt);
	munit_assert_int(rc, ==, SQLITE_OK);

	rc = sqlite3_wal_checkpoint_v2(
	    db, "main", SQLITE_CHECKPOINT_TRUNCATE, &size, &ckpt);
	munit_assert_int(rc, ==, SQLITE_OK);

	rc = sqlite3_close(db);
	munit_assert_int(rc, ==, SQLITE_OK);

	return MUNIT_OK;
}

/* Trying to register a new VFS with the same name produces and error. */
static MunitResult test_register_twice(const MunitParameter params[], void *data) {
	sqlite3_vfs *vfs;
	int          rc;

	(void)params;
	(void)data;

	rc = dqlite_vfs_register("volatile", &vfs);

	munit_assert_int(rc, ==, DQLITE_ERROR);

	return MUNIT_OK;
}

/* Test our expections on the memory-mapped WAl index format. */
static MunitResult test_register_wal_index(const MunitParameter params[],
                                           void *               data) {
	sqlite3 * db1;
	sqlite3 * db2;
	uint32_t *read_marks;
	int       rc;
	int       i;

	(void)data;
	(void)params;

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

	/* Start a read transaction on db2 */
	munit_log(MUNIT_LOG_INFO, "BEGIN");
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

	/* A shared lock is held on the second read mark (read locks start at
	 * 3). */
	munit_assert_true(__shm_shared_lock_held(db2, 4));

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

	__db_exec(db1, "COMMIT");

	/* The mx frame is now 6. */
	munit_assert_int(__wal_idx_mx_frame(db1), ==, 6);

	/* The old read lock is still in place. */
	munit_assert_true(__shm_shared_lock_held(db2, 4));

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

	/* The old read lock is still in place. */
	munit_assert_true(__shm_shared_lock_held(db2, 4));

	/* The new read lock is in place as well. */
	munit_assert_true(__shm_shared_lock_held(db2, 5));

	rc = sqlite3_close(db1);
	munit_assert_int(rc, ==, SQLITE_OK);

	rc = sqlite3_close(db2);
	munit_assert_int(rc, ==, SQLITE_OK);

	return SQLITE_OK;
}

static MunitTest dqlite__vfs_register_tests[] = {
    {"", test_register, setup, tear_down, 0, NULL},
    {"/twice", test_register_twice, setup, tear_down, 0, NULL},
    {"/wal-index", test_register_wal_index, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * dqlite__vfs_snapshot
 *
 ******************************************************************************/

/* Test taking and restoring file snapshots. */
static MunitResult test_snapshot(const MunitParameter params[], void *data) {
	sqlite3_vfs * vfs = data;
	int           rc;
	sqlite3 *     db;
	uint8_t *     database;
	uint8_t *     wal;
	size_t        len;
	sqlite3_stmt *stmt;
	const char *  tail;

	(void)params;

	db = __db_open();

	__db_exec(db, "CREATE TABLE test (n INT)");

	rc = dqlite_vfs_snapshot(vfs, "test.db", &database, &len);
	munit_assert_int(rc, ==, SQLITE_OK);

	munit_assert_ptr_not_equal(database, NULL);
	munit_assert_int(len, ==, 512);

	rc = dqlite_vfs_snapshot(vfs, "test.db-wal", &wal, &len);
	munit_assert_int(rc, ==, SQLITE_OK);

	munit_assert_ptr_not_equal(wal, NULL);
	munit_assert_int(len, ==, 1104);

	rc = sqlite3_close(db);
	munit_assert_int(rc, ==, SQLITE_OK);

	rc = dqlite_vfs_restore(vfs, "test.db", database, 512);
	munit_assert_int(rc, ==, SQLITE_OK);

	rc = dqlite_vfs_restore(vfs, "test.db-wal", wal, 1104);
	munit_assert_int(rc, ==, SQLITE_OK);

	sqlite3_free(database);
	sqlite3_free(wal);

	rc = sqlite3_open_v2("test.db", &db, SQLITE_OPEN_READWRITE, "volatile");
	munit_assert_int(rc, ==, SQLITE_OK);

	rc = sqlite3_prepare(db, "INSERT INTO test(n) VALUES(?)", -1, &stmt, &tail);
	munit_assert_int(rc, ==, SQLITE_OK);

	rc = sqlite3_finalize(stmt);
	munit_assert_int(rc, ==, SQLITE_OK);

	rc = sqlite3_close(db);
	munit_assert_int(rc, ==, SQLITE_OK);

	return MUNIT_OK;
}

static MunitTest dqlite__vfs_snapshot_tests[] = {
    {"", test_snapshot, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/******************************************************************************
 *
 * Test suite
 *
 ******************************************************************************/

MunitSuite dqlite__vfs_suites[] = {
    {"_open", dqlite__vfs_open_tests, NULL, 1, 0},
    {"_delete", dqlite__vfs_delete_tests, NULL, 1, 0},
    {"_access", dqlite__vfs_access_tests, NULL, 1, 0},
    {"_full_pathname", dqlite__vfs_full_pathname_tests, NULL, 1, 0},
    {"_close", dqlite__vfs_close_tests, NULL, 1, 0},
    {"_read", dqlite__vfs_read_tests, NULL, 1, 0},
    {"_write", dqlite__vfs_write_tests, NULL, 1, 0},
    {"_truncate", dqlite__vfs_truncate_tests, NULL, 1, 0},
    {"_shm_lock", dqlite__vfs_shm_lock_tests, NULL, 1, 0},
    {"_register", dqlite__vfs_register_tests, NULL, 1, 0},
    {"_snapshot", dqlite__vfs_snapshot_tests, NULL, 1, 0},
    {"", dqlite__vfs_snapshot_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
