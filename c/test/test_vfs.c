#include <errno.h>

#include <CUnit/CUnit.h>
#include <sqlite3.h>

#include "../include/dqlite.h"

#include "suite.h"

static sqlite3_vfs* vfs;
static sqlite3_file* file1;
static sqlite3_file* file2;

void test_dqlite__vfs_setup()
{
	int err = dqlite_vfs_register("volatile", &vfs);
	if (err != 0) {
		test_suite_errorf("failed to register vfs: %s - %d", sqlite3_errstr(err), err);
		CU_FAIL("test setup failed");
	}

	file1 = (sqlite3_file*)sqlite3_malloc(vfs->szOsFile);
	if (file1 == NULL) {
		test_suite_errorf("failed allocate db file");
		CU_FAIL("test setup failed");
	}

	file2 = (sqlite3_file*)sqlite3_malloc(vfs->szOsFile);
	if (file2 == NULL) {
		test_suite_errorf("failed allocate wal file");
		CU_FAIL("test setup failed");
	}
}

void test_dqlite__vfs_teardown()
{
	sqlite3_free(file2);
	sqlite3_free(file1);
	dqlite_vfs_unregister(vfs);
}

/* If the file does not exist and the SQLITE_OPEN_CREATE flag is not passed, an
 * error is returned. */
void test_dqlite__vfs_open_noent()
{
	int flags;
	int err;

	err = vfs->xOpen(vfs, "test.db", file1, 0, &flags);

	CU_ASSERT_EQUAL(err, SQLITE_CANTOPEN);
	CU_ASSERT_EQUAL(ENOENT, vfs->xGetLastError(vfs, 0, 0));
}

/* Open a file and close it. */
void test_dqlite__vfs_open_and_close()
{
	int flags;
	int err;

	err = vfs->xOpen(vfs, "test.db", file1, SQLITE_OPEN_CREATE, &flags);

	CU_ASSERT_EQUAL(err, 0);

	err = file1->pMethods->xClose(file1);
	CU_ASSERT_EQUAL(err, 0);
}

/* Accessing an existing file returns true. */
void test_dqlite__vfs_access()
{
	int flags;
	int err;
	int exists;

	err = vfs->xOpen(vfs, "test.db", file1, SQLITE_OPEN_CREATE, &flags);

	CU_ASSERT_EQUAL(err, 0);

	err = file1->pMethods->xClose(file1);
	CU_ASSERT_EQUAL(err, 0);

	err = vfs->xAccess(vfs, "test.db", 0, &exists);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(exists, 1);
}

/* Trying to access a non existing file returns false. */
void test_dqlite__vfs_access_noent()
{
	int err;
	int exists;

	err = vfs->xAccess(vfs, "test.db", 0, &exists);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(exists, 0);
}

/* Delete a file. */
void test_dqlite__vfs_delete()
{
	int flags;
	int err;

	err = vfs->xOpen(vfs, "test.db", file1, SQLITE_OPEN_CREATE, &flags);
	CU_ASSERT_EQUAL(err, 0);

	err = file1->pMethods->xClose(file1);
	CU_ASSERT_EQUAL(err, 0);

	err = vfs->xDelete(vfs, "test.db", 0);
	CU_ASSERT_EQUAL(err, 0);

	/* Trying to open the file again without the SQLITE_OPEN_CREATE flag
	 * results in an error. */
	err = vfs->xOpen(vfs, "test.db", file1, 0, &flags);
	CU_ASSERT_EQUAL(err, SQLITE_CANTOPEN);
}

/* Attempt to delete a file with open file descriptors. */
void test_dqlite__vfs_delete_busy()
{
	int flags;
	int err;

	err = vfs->xOpen(vfs, "test.db", file1, SQLITE_OPEN_CREATE, &flags);
	CU_ASSERT_EQUAL(err, 0);

	err = vfs->xDelete(vfs, "test.db", 0);
	CU_ASSERT_EQUAL(err, SQLITE_IOERR_DELETE);
	CU_ASSERT_EQUAL(EBUSY, vfs->xGetLastError(vfs, 0, 0));

	err = file1->pMethods->xClose(file1);
	CU_ASSERT_EQUAL(err, 0);
}

/* Attempt to read a file that was not written yet, results in an error. */
void test_dqlite__vfs_read_never_written()
{
	int flags;
	int err;
	char buf[1];

	err = vfs->xOpen(vfs, "test.db", file1, SQLITE_OPEN_CREATE, &flags);
	CU_ASSERT_EQUAL(err, 0);

	err = file1->pMethods->xRead(file1, (void*)buf, 1, 0);
	CU_ASSERT_EQUAL(err, SQLITE_IOERR_SHORT_READ);

	err = file1->pMethods->xClose(file1);
	CU_ASSERT_EQUAL(err, 0);
}

/* Write the header of the database file. */
void test_dqlite__vfs_write_database_header()
{
	int flags;
	int err;
	char buf[100];

	/* Set page size to 512. */
	buf[16] = 2;
	buf[17] = 0;

	err = vfs->xOpen(vfs, "test.db", file1, SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB, &flags);
	CU_ASSERT_EQUAL(err, 0);

	err = file1->pMethods->xWrite(file1, (void*)buf, 100, 0);
	CU_ASSERT_EQUAL(err, 0);

	err = file1->pMethods->xClose(file1);
	CU_ASSERT_EQUAL(err, 0);
}

/* Write the header of the database file, then the full first page and a second
 * page. */
void test_dqlite__vfs_write_and_read_database_pages()
{
	int flags;
	int err;
	char buf[512];

	err = vfs->xOpen(vfs, "test.db", file1, SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB, &flags);
	CU_ASSERT_EQUAL(err, 0);

	/* Write the header. */
	buf[16] = 2;
	buf[17] = 0;

	err = file1->pMethods->xWrite(file1, (void*)buf, 100, 0);
	CU_ASSERT_EQUAL(err, 0);

	/* Write the first page, containing the header and some other content. */
	buf[101] = 1;
	buf[256] = 2;
	buf[511] = 3;

	err = file1->pMethods->xWrite(file1, (void*)buf, 512, 0);
	CU_ASSERT_EQUAL(err, 0);

	/* Write a second page. */
	memset(buf, 0, 512);
	buf[0] = 4;
	buf[256] = 5;
	buf[511] = 6;

	err = file1->pMethods->xWrite(file1, (void*)buf, 512, 512);
	CU_ASSERT_EQUAL(err, 0);

	/* Read the page header. */
	memset(buf, 0, 512);
	err = file1->pMethods->xRead(file1, (void*)buf, 512, 0);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(buf[16], 2);
	CU_ASSERT_EQUAL(buf[17], 0);
	CU_ASSERT_EQUAL(buf[101], 1);
	CU_ASSERT_EQUAL(buf[256], 2);
	CU_ASSERT_EQUAL(buf[511], 3);

	/* Read the second page. */
	memset(buf, 0, 512);
	err = file1->pMethods->xRead(file1, (void*)buf, 512, 512);
	CU_ASSERT_EQUAL(err, 0);

	CU_ASSERT_EQUAL(buf[0], 4);
	CU_ASSERT_EQUAL(buf[256], 5);
	CU_ASSERT_EQUAL(buf[511], 6);

	err = file1->pMethods->xClose(file1);
	CU_ASSERT_EQUAL(err, 0);

	err = vfs->xDelete(vfs, "test.db", 0);
	CU_ASSERT_EQUAL(err, 0);
}

/* Write the header of a WAL file, then two frames. */
void test_dqlite__vfs_write_and_read_wal_frames()
{
	int flags;
	int err;
	char buf[512];

	/* First write the main database header, which sets the page size. */
	err = vfs->xOpen(vfs, "test.db", file1, SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB, &flags);
	CU_ASSERT_EQUAL(err, 0);

	buf[16] = 2;
	buf[17] = 0;

	err = file1->pMethods->xWrite(file1, (void*)buf, 100, 0);
	CU_ASSERT_EQUAL(err, 0);

	memset(buf, 0, 512);

	/* Open the associated WAL file and write the WAL header. */
	err = vfs->xOpen(vfs, "test.db-wal", file2, SQLITE_OPEN_CREATE | SQLITE_OPEN_WAL, &flags);
	CU_ASSERT_EQUAL(err, 0);
	buf[10] = 2;
	buf[11] = 0;

	err = file2->pMethods->xWrite(file2, (void*)buf, 32, 0);
	CU_ASSERT_EQUAL(err, 0);

	memset(buf, 0, 512);

	/* Write the header of the first frame. */
	err = file2->pMethods->xWrite(file2, (void*)buf, 24, 32);
	CU_ASSERT_EQUAL(err, 0);

	/* Write the page of the first frame. */
	err = file2->pMethods->xWrite(file2, (void*)buf, 512, 32 +24);
	CU_ASSERT_EQUAL(err, 0);

	/* Write the header of the second frame. */
	err = file2->pMethods->xWrite(file2, (void*)buf, 24, 32 + 24 + 512);
	CU_ASSERT_EQUAL(err, 0);

	/* Write the page of the second frame. */
	err = file2->pMethods->xWrite(file2, (void*)buf, 512, 32 + 24 + 512 + 24);
	CU_ASSERT_EQUAL(err, 0);

	/* Read the WAL header. */
	err = file2->pMethods->xRead(file2, (void*)buf, 32, 0);
	CU_ASSERT_EQUAL(err, 0);

	/* Read the header of the first frame. */
	err = file2->pMethods->xRead(file2, (void*)buf, 24, 32);
	CU_ASSERT_EQUAL(err, 0);

	/* Read the page of the first frame. */
	err = file2->pMethods->xRead(file2, (void*)buf, 512, 32 + 24);
	CU_ASSERT_EQUAL(err, 0);

	/* Read the header of the second frame. */
	err = file2->pMethods->xRead(file2, (void*)buf, 24, 32 + 24 + 512);
	CU_ASSERT_EQUAL(err, 0);

	/* Read the page of the second frame. */
	err = file2->pMethods->xRead(file2, (void*)buf, 512, 32 + 24 + 512 + 24);
	CU_ASSERT_EQUAL(err, 0);
}

/* Truncate the main database file. */
void test_dqlite__vfs_truncate_database()
{
	int err;
	int flags;
	char buf[512];
	sqlite_int64 size;

	err = vfs->xOpen(vfs, "test.db", file1, SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB, &flags);
	CU_ASSERT_EQUAL(err, 0);

	/* Initial size is 0. */
	err = file1->pMethods->xFileSize(file1, &size);
	CU_ASSERT_EQUAL(err, 0);
	CU_ASSERT_EQUAL(size, 0);

	/* Truncating an empty file is a no-op. */
	err = file1->pMethods->xTruncate(file1, 0);
	CU_ASSERT_EQUAL(err, 0);

	/* The size is still 0. */
	err = file1->pMethods->xFileSize(file1, &size);
	CU_ASSERT_EQUAL(err, 0);
	CU_ASSERT_EQUAL(size, 0);

	/* Write the first page, containing the header. */
	buf[16] = 2;
	buf[17] = 0;
	err = file1->pMethods->xWrite(file1, (void*)buf, 512, 0);
	CU_ASSERT_EQUAL(err, 0);

	memset(buf, 0, 512);

	/* Write a second page. */
	err = file1->pMethods->xWrite(file1, (void*)buf, 512, 512);
	CU_ASSERT_EQUAL(err, 0);

	/* The size is 1024. */
	err = file1->pMethods->xFileSize(file1, &size);
	CU_ASSERT_EQUAL(err, 0);
	CU_ASSERT_EQUAL(size, 1024);

	/* Truncate the second page. */
	err = file1->pMethods->xTruncate(file1, 512);
	CU_ASSERT_EQUAL(err, 0);

	/* The size is 512. */
	err = file1->pMethods->xFileSize(file1, &size);
	CU_ASSERT_EQUAL(err, 0);
	CU_ASSERT_EQUAL(size, 512);

	/* Truncate also the first. */
	err = file1->pMethods->xTruncate(file1, 0);
	CU_ASSERT_EQUAL(err, 0);

	/* The size is 0. */
	err = file1->pMethods->xFileSize(file1, &size);
	CU_ASSERT_EQUAL(err, 0);
	CU_ASSERT_EQUAL(size, 0);
}

void test_dqlite__vfs_truncate_wal()
{
	int flags;
	int err;
	char buf[512];
	sqlite3_int64 size;

	/* First write the main database header, which sets the page size. */
	err = vfs->xOpen(vfs, "test.db", file1, SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB, &flags);
	CU_ASSERT_EQUAL(err, 0);

	buf[16] = 2;
	buf[17] = 0;

	err = file1->pMethods->xWrite(file1, (void*)buf, 100, 0);
	CU_ASSERT_EQUAL(err, 0);

	memset(buf, 0, 512);

	/* Open the associated WAL  */
	err = vfs->xOpen(vfs, "test.db-wal", file2, SQLITE_OPEN_CREATE | SQLITE_OPEN_WAL, &flags);
	CU_ASSERT_EQUAL(err, 0);

	/* Initial size is 0. */
	err = file2->pMethods->xFileSize(file2, &size);
	CU_ASSERT_EQUAL(err, 0);
	CU_ASSERT_EQUAL(size, 0);

	/* Truncating an empty file is a no-op. */
	err = file2->pMethods->xTruncate(file2, 0);
	CU_ASSERT_EQUAL(err, 0);

	/* The size is still 0. */
	err = file2->pMethods->xFileSize(file2, &size);
	CU_ASSERT_EQUAL(err, 0);
	CU_ASSERT_EQUAL(size, 0);

	/* Write the WAL header. */
	buf[10] = 2;
	buf[11] = 0;
	err = file2->pMethods->xWrite(file2, (void*)buf, 32, 0);
	CU_ASSERT_EQUAL(err, 0);

	memset(buf, 0, 512);

	/* Write the header of the first frame. */
	err = file2->pMethods->xWrite(file2, (void*)buf, 24, 32);
	CU_ASSERT_EQUAL(err, 0);

	/* Write the page of the first frame. */
	err = file2->pMethods->xWrite(file2, (void*)buf, 512, 32 +24);
	CU_ASSERT_EQUAL(err, 0);

	/* Write the header of the second frame. */
	err = file2->pMethods->xWrite(file2, (void*)buf, 24, 32 + 24 + 512);
	CU_ASSERT_EQUAL(err, 0);

	/* Write the page of the second frame. */
	err = file2->pMethods->xWrite(file2, (void*)buf, 512, 32 + 24 + 512 + 24);
	CU_ASSERT_EQUAL(err, 0);

	/* The size is 1104. */
	err = file2->pMethods->xFileSize(file2, &size);
	CU_ASSERT_EQUAL(err, 0);
	CU_ASSERT_EQUAL(size, 1104);

	/* Truncate the WAL file. */
	err = file2->pMethods->xTruncate(file2, 0);
	CU_ASSERT_EQUAL(err, 0);

	/* The size is 0. */
	err = file2->pMethods->xFileSize(file2, &size);
	CU_ASSERT_EQUAL(err, 0);
	CU_ASSERT_EQUAL(size, 0);

}

void test_dqlite__vfs_integration()
{
	int rc;
	sqlite3 *db;
	sqlite3_stmt *stmt;
	const char *tail;
	int i;
	int size;
	int ckpt;

	rc = sqlite3_open_v2("test.db", &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "volatile");
	CU_ASSERT_EQUAL(rc, SQLITE_OK);

	/* Set the page size */
	rc = sqlite3_prepare(db, "PRAGMA page_size=4096", -1, &stmt, &tail);
	CU_ASSERT_EQUAL(rc, SQLITE_OK);

	rc = sqlite3_step(stmt);
	CU_ASSERT_EQUAL(rc, SQLITE_DONE);

	rc = sqlite3_finalize(stmt);
	CU_ASSERT_EQUAL(rc, SQLITE_OK);

	/* Disable syncs. */
	rc = sqlite3_prepare(db, "PRAGMA synchronous=OFF", -1, &stmt, &tail);
	CU_ASSERT_EQUAL(rc, SQLITE_OK);

	rc = sqlite3_step(stmt);
	CU_ASSERT_EQUAL(rc, SQLITE_DONE);

	rc = sqlite3_finalize(stmt);
	CU_ASSERT_EQUAL(rc, SQLITE_OK);

	/* Set WAL journaling. */
	rc = sqlite3_prepare(db, "PRAGMA journal_mode=WAL", -1, &stmt, &tail);
	CU_ASSERT_EQUAL(rc, SQLITE_OK);

	rc = sqlite3_step(stmt);
	CU_ASSERT_EQUAL(rc, SQLITE_ROW);

	rc = sqlite3_finalize(stmt);
	CU_ASSERT_EQUAL(rc, SQLITE_OK);

	/* Create a test table and insert a few rows into it. */
	rc = sqlite3_prepare(db, "CREATE TABLE test (n INT)", -1, &stmt, &tail);
	CU_ASSERT_EQUAL(rc, SQLITE_OK);

	rc = sqlite3_step(stmt);
	CU_ASSERT_EQUAL(rc, SQLITE_DONE);

	rc = sqlite3_finalize(stmt);
	CU_ASSERT_EQUAL(rc, SQLITE_OK);

	rc = sqlite3_prepare(db, "INSERT INTO test(n) VALUES(?)", -1, &stmt, &tail);
	CU_ASSERT_EQUAL(rc, SQLITE_OK);

	for (i = 0; i < 100; i++) {
		rc = sqlite3_bind_int(stmt, 1, i);
		CU_ASSERT_EQUAL(rc, SQLITE_OK);

		rc = sqlite3_step(stmt);
		CU_ASSERT_EQUAL(rc, SQLITE_DONE);

		rc = sqlite3_reset(stmt);
		CU_ASSERT_EQUAL(rc, SQLITE_OK);
	}

	rc = sqlite3_finalize(stmt);
	CU_ASSERT_EQUAL(rc, SQLITE_OK);

        rc = sqlite3_wal_checkpoint_v2(db, "main", SQLITE_CHECKPOINT_TRUNCATE, &size, &ckpt);
	CU_ASSERT_EQUAL(rc, SQLITE_OK);

	rc = sqlite3_close(db);
	CU_ASSERT_EQUAL(rc, SQLITE_OK);
}
