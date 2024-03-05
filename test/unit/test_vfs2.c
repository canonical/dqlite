#pragma GCC diagnostic ignored "-Wformat-truncation"

#include "../../src/vfs2.h"
#include "../lib/fs.h"
#include "../lib/runner.h"

#include <sqlite3.h>

#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <unistd.h>

SUITE(vfs2);

struct fixture {
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
	munit_assert_ptr_not_null(f->vfs);
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

static void prepare_wals(const char *dbname,
			 const unsigned char *wal1,
			 size_t wal1_len,
			 const unsigned char *wal2,
			 size_t wal2_len)
{
	char buf[PATH_MAX];
	ssize_t n;
	if (wal1 != NULL) {
		snprintf(buf, sizeof(buf), "%s-xwal1", dbname);
		int fd1 = open(buf, O_RDWR | O_CREAT,
			       S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
		munit_assert_int(fd1, !=, -1);
		n = write(fd1, wal1, wal1_len);
		munit_assert_llong(n, ==, wal1_len);
		close(fd1);
	}
	if (wal2 != NULL) {
		snprintf(buf, sizeof(buf), "%s-xwal2", dbname);
		int fd2 = open(buf, O_RDWR | O_CREAT,
			       S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
		munit_assert_int(fd2, !=, -1);
		n = write(fd2, wal2, wal2_len);
		munit_assert_llong(n, ==, wal2_len);
		close(fd2);
	}
}

static void check_wals(const char *dbname,
		       const unsigned char *wal1,
		       size_t wal1_len,
		       const unsigned char *wal2,
		       size_t wal2_len)
{
	char buf[PATH_MAX];
	snprintf(buf, sizeof(buf), "%s-xwal1", dbname);
	int fd1 = open(buf, O_RDONLY);
	munit_assert_true((fd1 == -1) == (wal1 == NULL));
	snprintf(buf, sizeof(buf), "%s-xwal2", dbname);
	int fd2 = open(buf, O_RDONLY);
	munit_assert_true((fd2 == -1) == (wal2 == NULL));

	char *fbuf = NULL;
	ssize_t n;
	if (wal1 != NULL) {
		fbuf = realloc(fbuf, wal1_len);
		munit_assert_ptr_not_null(fbuf);
		n = read(fd1, fbuf, wal1_len);
		close(fd1);
		munit_assert_llong(n, ==, wal1_len);
		munit_assert_int(memcmp(fbuf, wal1, wal1_len), ==, 0);
	}
	if (wal2 != NULL) {
		fbuf = realloc(fbuf, wal2_len);
		munit_assert_ptr_not_null(fbuf);
		n = read(fd2, fbuf, wal2_len);
		close(fd2);
		munit_assert_llong(n, ==, wal2_len);
		munit_assert_int(memcmp(fbuf, wal2, wal2_len), ==, 0);
	}
	free(fbuf);
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

	rv = sqlite3_exec(db, "CREATE TABLE foo (bar INTEGER)", NULL, NULL,
			  NULL);
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

	rv = sqlite3_exec(db, "INSERT INTO foo (bar) VALUES (17)", NULL, NULL,
			  NULL);
	munit_assert_int(rv, ==, SQLITE_OK);
	rv = vfs2_abort(fp);
	munit_assert_int(rv, ==, 0);

	rv = sqlite3_exec(db, "INSERT INTO foo (bar) values (22)", NULL, NULL,
			  NULL);
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
	rv = sqlite3_wal_checkpoint_v2(db, "main", SQLITE_CHECKPOINT_PASSIVE,
				       &nlog, &nckpt);
	munit_assert_int(rv, ==, SQLITE_OK);
	munit_assert_int(nlog, ==, 3);
	munit_assert_int(nckpt, ==, 3);

	rv = sqlite3_exec(db, "INSERT INTO foo (bar) VALUES (101)", NULL, NULL,
			  NULL);
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

TEST(vfs2, startup, set_up, tear_down, 0, NULL)
{
	struct fixture *f = data;
	int rv;
	char buf[PATH_MAX];

	snprintf(buf, PATH_MAX, "%s/%s", f->dir, "test.db");
	sqlite3 *db;
	rv = sqlite3_open(buf, &db);
	munit_assert_int(rv, ==, SQLITE_OK);

	prepare_wals(buf, NULL, 0, NULL, 0);

	rv = sqlite3_exec(db,
			  "PRAGMA journal_mode=WAL;"
			  "PRAGMA page_size=4096;"
			  "PRAGMA wal_autocheckpoint=0",
			  NULL, NULL, NULL);
	munit_assert_int(rv, ==, SQLITE_OK);
	rv = sqlite3_close(db);
	munit_assert_int(rv, ==, SQLITE_OK);

	check_wals(buf, NULL, 0, NULL, 0);

	return MUNIT_OK;
}
