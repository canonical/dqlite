#ifndef DQLITE_VFS_TEST_H
#define DQLITE_VFS_TEST_H

#include <CUnit/CUnit.h>

void test_dqlite__vfs_setup();
void test_dqlite__vfs_teardown();

void test_dqlite__vfs_open_noent();
void test_dqlite__vfs_open_and_close();
void test_dqlite__vfs_access();
void test_dqlite__vfs_access_noent();
void test_dqlite__vfs_delete();
void test_dqlite__vfs_delete_busy();
void test_dqlite__vfs_read_never_written();
void test_dqlite__vfs_write_database_header();
void test_dqlite__vfs_write_and_read_database_pages();
void test_dqlite__vfs_write_and_read_wal_frames();
void test_dqlite__vfs_truncate_database();
void test_dqlite__vfs_truncate_wal();
void test_dqlite_vfs_register();
void test_dqlite_vfs_content();

CU_TestInfo dqlite__vfs_suite[] = {
	{"open noent",                test_dqlite__vfs_open_noent},
	{"open and close",            test_dqlite__vfs_open_and_close},
	{"access",                    test_dqlite__vfs_access},
	{"access noent",              test_dqlite__vfs_access_noent},
	{"delete",                    test_dqlite__vfs_delete},
	{"read never written",        test_dqlite__vfs_read_never_written},
	{"write database header",     test_dqlite__vfs_write_database_header},
	{"write and read db pages",   test_dqlite__vfs_write_and_read_database_pages},
	{"write and read wal frames", test_dqlite__vfs_write_and_read_wal_frames},
	{"truncate database",         test_dqlite__vfs_truncate_database},
	{"truncate wal",              test_dqlite__vfs_truncate_wal},
	{"register",                  test_dqlite_vfs_register},
	{"content",                   test_dqlite_vfs_content},
	CU_TEST_INFO_NULL,
};

CU_SuiteInfo dqlite__vfs_suites[] = {
	{
		"dqlite__vfs",
		NULL, NULL,
		test_dqlite__vfs_setup, test_dqlite__vfs_teardown,
		dqlite__vfs_suite
	},
	CU_SUITE_INFO_NULL,
};

#endif /* DQLITE_VFS_TEST_H */
