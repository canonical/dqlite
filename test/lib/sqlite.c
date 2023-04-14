#include <sqlite3.h>

#include "sqlite.h"

void test_sqlite_setup(const MunitParameter params[])
{
	int rc;
	(void)params;
	rc = sqlite3_initialize();
	if (rc != SQLITE_OK) {
		munit_errorf("sqlite_init(): %s", sqlite3_errstr(rc));
	}
}

void test_sqlite_tear_down()
{
	int rc;
	rc = sqlite3_shutdown();
	if (rc != SQLITE_OK) {
		munit_errorf("sqlite_shutdown(): %s", sqlite3_errstr(rc));
	}
}
