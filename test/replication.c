#include <stdlib.h>

#include <sqlite3.h>

static int test__replication_begin(sqlite3_wal_replication *r, void *pArg)
{
	return 0;
}

static int test__replication_abort(sqlite3_wal_replication *r, void *pArg)
{
	return 0;
}

static int test__replication_frames(
	sqlite3_wal_replication *r, void *pArg,
	int szPage, int nFrame,
	sqlite3_wal_replication_frame *aFrame, unsigned nTruncate, int isCommit)
{
	return 0;
}

static int test__replication_undo(sqlite3_wal_replication *r, void *pArg)
{
	return 0;
}

static int test__replication_end(sqlite3_wal_replication *r, void *pArg)
{
	return 0;
}

static sqlite3_wal_replication test__replication = {
	1,
	NULL,
	"test",
	NULL,
	test__replication_begin,
	test__replication_abort,
	test__replication_frames,
	test__replication_undo,
	test__replication_end,
};

sqlite3_wal_replication* test_replication()
{
	return &test__replication;
}
