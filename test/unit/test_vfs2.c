#include "../../src/vfs2.h"
#include "../lib/fs.h"
#include "../lib/runner.h"

#include <sqlite3.h>

#include <limits.h>
#include <stdio.h>

SUITE(vfs2);

struct fixture
{
	sqlite3_vfs *vfs;
	char *dir;
};

static void *set_up(const MunitParameter params[], void *user_data)
{
	(void)params;
	(void)user_data;
	struct fixture *f = munit_malloc(sizeof(*f));
	f->dir = test_dir_setup();
	f->vfs = vfs2_make(sqlite3_vfs_find("unix"), "dqlite-vfs2", 0);
	sqlite3_vfs_register(f->vfs, 1 /* make default */);
	return f;
}

static void tear_down(void *data)
{
	struct fixture *f = data;
	sqlite3_vfs_unregister(f->vfs);
	vfs2_destroy(f->vfs);
	test_dir_tear_down(f->dir);
	free(f);
}

TEST(vfs2, basic, set_up, tear_down, 0, NULL)
{
	struct fixture *f = data;
	int rv;
	char buf[PATH_MAX];

	snprintf(buf, PATH_MAX, "%s/%s", f->dir, "test.db");
	sqlite3 *db;
	rv = sqlite3_open(buf, &db);
	munit_assert_int(rv, ==, SQLITE_OK);

	rv = sqlite3_exec(db,
		"PRAGMA journal_mode=WAL;"
		"PRAGMA page_size=4096;"
		"PRAGMA wal_autocheckpoint=0",
		NULL, NULL, NULL);
	munit_assert_int(rv, ==, SQLITE_OK);

	rv = sqlite3_exec(db, "CREATE TABLE foo (bar INTEGER)", NULL, NULL, NULL);
	munit_assert_int(rv, ==, SQLITE_OK);

	sqlite3_file *fp;
	sqlite3_file_control(db, "main", SQLITE_FCNTL_FILE_POINTER, &fp);
	struct vfs2_wal_slice sl;
	rv = vfs2_shallow_poll(fp, &sl);
	munit_assert_int(rv, ==, 0);
	rv = vfs2_apply(fp);
	munit_assert_int(rv, ==, 0);
	munit_assert_uint32(sl.start, ==, 0);
	munit_assert_uint32(sl.len, ==, 2);

	rv = sqlite3_exec(db,
		"INSERT INTO foo (bar) VALUES (17)",
		NULL, NULL, NULL);
	munit_assert_int(rv, ==, SQLITE_OK);
	rv = vfs2_abort(fp);
	munit_assert_int(rv, ==, 0);

	rv = sqlite3_exec(db,
		"INSERT INTO foo (bar) values (22)",
		NULL, NULL, NULL);
	munit_assert_int(rv, ==, 0);
	rv = vfs2_shallow_poll(fp, &sl);
	munit_assert_int(rv, ==, 0);
	munit_assert_uint32(sl.start, ==, 2);
	munit_assert_uint32(sl.len, ==, 1);
	rv = vfs2_apply(fp);
	munit_assert_int(rv, ==, 0);

	sqlite3_stmt *stmt;
	rv = sqlite3_prepare_v2(db, "SELECT * FROM foo", -1, &stmt, NULL);
	munit_assert_int(rv, ==, SQLITE_OK);
	rv = sqlite3_step(stmt);
	munit_assert_int(rv, ==, SQLITE_ROW);
	munit_assert_int(sqlite3_column_count(stmt), ==, 1);
	munit_assert_int(sqlite3_column_int(stmt, 0), ==, 22);
	rv = sqlite3_step(stmt);
	munit_assert_int(rv, ==, SQLITE_DONE);

	int nlog;
	int nckpt;
	rv = sqlite3_wal_checkpoint_v2(db, "main", SQLITE_CHECKPOINT_PASSIVE, &nlog, &nckpt);
	munit_assert_int(rv, ==, SQLITE_OK);
	munit_assert_int(nlog, ==, 3);
	munit_assert_int(nckpt, ==, 3);

	rv = sqlite3_exec(db,
		"INSERT INTO foo (bar) VALUES (101)",
		NULL, NULL, NULL);
	munit_assert_int(rv, ==, SQLITE_OK);

	rv = sqlite3_reset(stmt);
	munit_assert_int(rv, ==, SQLITE_OK);
	rv = sqlite3_step(stmt);
	munit_assert_int(rv, ==, SQLITE_ROW);
	munit_assert_int(sqlite3_column_count(stmt), ==, 1);
	munit_assert_int(sqlite3_column_int(stmt, 0), ==, 22);
	/* Can't see the new row yet. */
	rv = sqlite3_step(stmt);
	munit_assert_int(rv, ==, SQLITE_DONE);

	dqlite_vfs_frame *frames;
	unsigned n;
	rv = vfs2_poll(fp, &frames, &n);
	munit_assert_int(rv, ==, 0);
	munit_assert_uint(n, ==, 1);
	munit_assert_not_null(frames);
	munit_assert_not_null(frames[0].data);
	sqlite3_free(frames[0].data);
	sqlite3_free(frames);

	rv = vfs2_apply(fp);
	munit_assert_int(rv, ==, 0);

	rv = sqlite3_reset(stmt);
	munit_assert_int(rv, ==, SQLITE_OK);
	rv = sqlite3_step(stmt);
	munit_assert_int(rv, ==, SQLITE_ROW);
	munit_assert_int(sqlite3_column_count(stmt), ==, 1);
	munit_assert_int(sqlite3_column_int(stmt, 0), ==, 22);
	rv = sqlite3_step(stmt);
	munit_assert_int(rv, ==, SQLITE_ROW);
	munit_assert_int(sqlite3_column_int(stmt, 0), ==, 101);
	rv = sqlite3_step(stmt);
	munit_assert_int(rv, ==, SQLITE_DONE);

	rv = sqlite3_finalize(stmt);
	munit_assert_int(rv, ==, SQLITE_OK);
	sqlite3_close(db);
	return MUNIT_OK;
}
