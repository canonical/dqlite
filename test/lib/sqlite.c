#include <sqlite3.h>
#include <stdio.h>

#include "sqlite.h"

static inline void log_sqlite_error(void *arg, int e, const char *msg)
{
	(void)arg;
	fprintf(stderr, "SQLITE %d %s\n", e, msg);
}

__attribute__((constructor)) static void test_sqlite_init(void)
{
	int rc = sqlite3_config(SQLITE_CONFIG_LOG, log_sqlite_error, NULL);
	munit_assert(rc == SQLITE_OK);
}

void test_sqlite_setup(const MunitParameter params[])
{
	int rc;
	(void)params;
	rc = sqlite3_initialize();
	if (rc != SQLITE_OK) {
		munit_errorf("sqlite_init(): %s", sqlite3_errstr(rc));
	}
	rc = sqlite3_threadsafe();
	if (!(rc == 1 || rc == 2)) {
		munit_errorf("sqlite3_threadsafe(): %d", rc);
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
