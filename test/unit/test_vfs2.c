#pragma GCC diagnostic ignored "-Wformat-truncation" // XXX

#include "../../src/vfs2.h"
#include "../../src/lib/byte.h"
#include "../lib/fs.h"
#include "../lib/runner.h"
#include "../lib/sqlite.h"

#include <sqlite3.h>

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#define DB_PAGE_SIZE 512
#define DB_PAGE_SIZE_STR "512"

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

	SETUP_SQLITE;

	f->dir = test_dir_setup();
	f->vfs = vfs2_make(sqlite3_vfs_find("unix"), "dqlite-vfs2");
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

	TEAR_DOWN_SQLITE;
}

static void prepare_wals(const char *dbname,
			 const unsigned char *wal1,
			 size_t wal1_len,
			 const unsigned char *wal2,
			 size_t wal2_len)
{
	char buf[PATH_MAX];
	ssize_t n;
	int rv;
	if (wal1 != NULL) {
		snprintf(buf, sizeof(buf), "%s-xwal1", dbname);
		int fd1 = open(buf, O_RDWR | O_CREAT,
			       S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
		munit_assert_int(fd1, !=, -1);
		rv = ftruncate(fd1, 0);
		munit_assert_int(rv, ==, 0);
		n = write(fd1, wal1, wal1_len);
		munit_assert_llong(n, ==, wal1_len);
		close(fd1);
	}
	if (wal2 != NULL) {
		snprintf(buf, sizeof(buf), "%s-xwal2", dbname);
		int fd2 = open(buf, O_RDWR | O_CREAT,
			       S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
		munit_assert_int(fd2, !=, -1);
		rv = ftruncate(fd2, 0);
		munit_assert_int(rv, ==, 0);
		n = write(fd2, wal2, wal2_len);
		munit_assert_llong(n, ==, wal2_len);
		close(fd2);
	}
}

static void check_wals(const char *dbname, off_t wal1_len, off_t wal2_len)
{
	char buf[PATH_MAX];
	struct stat st;
	int rv;

	snprintf(buf, sizeof(buf), "%s-xwal1", dbname);
	rv = stat(buf, &st);
	munit_assert_true((rv == 0 && st.st_size == wal1_len) || (rv < 0 && errno == ENOENT && wal1_len == 0));

	snprintf(buf, sizeof(buf), "%s-xwal2", dbname);
	rv = stat(buf, &st);
	munit_assert_true((rv == 0 && st.st_size == wal2_len) || (rv < 0 && errno == ENOENT && wal2_len == 0));
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
			  "PRAGMA page_size=" DB_PAGE_SIZE_STR ";"
			  "PRAGMA journal_mode=WAL;"
			  "PRAGMA wal_autocheckpoint=0",
			  NULL, NULL, NULL);
	munit_assert_int(rv, ==, SQLITE_OK);

	char *args[] = {NULL, "page_size", NULL};
	rv = sqlite3_file_control(db, "main", SQLITE_FCNTL_PRAGMA, args);
	sqlite3_file *fp;
	sqlite3_file_control(db, "main", SQLITE_FCNTL_FILE_POINTER, &fp);
	rv = vfs2_commit_barrier(fp);
	munit_assert_int(rv, ==, 0);
	rv = sqlite3_exec(db, "CREATE TABLE foo (bar INTEGER)", NULL, NULL,
			  NULL);
	munit_assert_int(rv, ==, SQLITE_OK);

	struct vfs2_wal_slice sl;
	rv = vfs2_poll(fp, NULL, NULL, &sl);
	munit_assert_int(rv, ==, 0);
	rv = vfs2_unhide(fp);
	munit_assert_int(rv, ==, 0);
	munit_assert_uint32(sl.start, ==, 0);
	munit_assert_uint32(sl.len, ==, 2);

	rv = sqlite3_exec(db, "INSERT INTO foo (bar) VALUES (17)", NULL, NULL,
			  NULL);
	munit_assert_int(rv, ==, SQLITE_OK);
	tracef("aborting...");
	rv = vfs2_abort(fp);
	munit_assert_int(rv, ==, 0);

	rv = sqlite3_exec(db, "INSERT INTO foo (bar) values (22)", NULL, NULL,
			  NULL);
	munit_assert_int(rv, ==, 0);
	rv = vfs2_poll(fp, NULL, NULL, &sl);
	munit_assert_int(rv, ==, 0);
	munit_assert_uint32(sl.start, ==, 2);
	munit_assert_uint32(sl.len, ==, 1);
	rv = vfs2_unhide(fp);
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

	struct vfs2_wal_frame *frames;
	unsigned n;
	rv = vfs2_poll(fp, &frames, &n, &sl);
	munit_assert_int(rv, ==, 0);
	munit_assert_uint(n, ==, 1);
	munit_assert_not_null(frames);
	munit_assert_not_null(frames[0].page);
	sqlite3_free(frames[0].page);
	sqlite3_free(frames);

	rv = vfs2_unhide(fp);
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

#define WAL_SIZE_FROM_FRAMES(n) (32 + (24 + DB_PAGE_SIZE) * (n))

static void make_wal_hdr(uint8_t *buf, uint32_t ckpoint_seqno, uint32_t salt1, uint32_t salt2)
{
	uint8_t *p = buf;

	/* checksum */
	BytePutBe32(0x377f0683, p);
	p += 4;
	BytePutBe32(3007000, p);
	p += 4;
	BytePutBe32(DB_PAGE_SIZE, p);
	p += 4;
	BytePutBe32(ckpoint_seqno, p);
	p += 4;
	BytePutBe32(salt1, p);
	p += 4;
	BytePutBe32(salt2, p);
	p += 4;

	uint32_t s0 = 0;
	uint32_t s1 = 0;
	size_t off = 0;

	s0 += ByteGetBe32(buf + off) + s1;
	s1 += ByteGetBe32(buf + off + 4) + s0;
	off += 8;

	s0 += ByteGetBe32(buf + off) + s1;
	s1 += ByteGetBe32(buf + off + 4) + s0;
	off += 8;

	s0 += ByteGetBe32(buf + off) + s1;
	s1 += ByteGetBe32(buf + off + 4) + s0;
	off += 8;

	BytePutBe32(s0, p);
	p += 4;
	BytePutBe32(s1, p);
	p += 4;
}

TEST(vfs2, startup_one_nonempty, set_up, tear_down, 0, NULL)
{
	struct fixture *f = data;
	char buf[PATH_MAX];

	snprintf(buf, PATH_MAX, "%s/%s", f->dir, "test.db");

	check_wals(buf, 0, 0);

	uint8_t wal2_hdronly[WAL_SIZE_FROM_FRAMES(0)] = {0};
	make_wal_hdr(wal2_hdronly, 0, 17, 103);
	prepare_wals(buf, NULL, 0, wal2_hdronly, sizeof(wal2_hdronly));
	sqlite3 *db;
	tracef("opening...");
	int rv = sqlite3_open(buf, &db);
	munit_assert_int(rv, ==, SQLITE_OK);
	tracef("setup...");
	rv = sqlite3_exec(db,
			  "PRAGMA page_size=" DB_PAGE_SIZE_STR ";"
			  "PRAGMA journal_mode=WAL;"
			  "PRAGMA wal_autocheckpoint=0",
			  NULL, NULL, NULL);
	munit_assert_int(rv, ==, SQLITE_OK);
	sqlite3_file *fp;
	sqlite3_file_control(db, "main", SQLITE_FCNTL_FILE_POINTER, &fp);
	tracef("barrier...");
	rv = vfs2_commit_barrier(fp);
	munit_assert_int(rv, ==, 0);
	tracef("create table...");
	rv = sqlite3_exec(db, "CREATE TABLE foo (n INTEGER)", NULL, NULL, NULL);
	munit_assert_int(rv, ==, SQLITE_OK);
	tracef("closing...");
	rv = sqlite3_close(db);
	munit_assert_int(rv, ==, SQLITE_OK);

	check_wals(buf, WAL_SIZE_FROM_FRAMES(2), WAL_SIZE_FROM_FRAMES(0));

	return MUNIT_OK;
}

TEST(vfs2, startup_both_nonempty, set_up, tear_down, 0, NULL)
{
	struct fixture *f = data;
	char buf[PATH_MAX];

	snprintf(buf, PATH_MAX, "%s/%s", f->dir, "test.db");

	check_wals(buf, 0, 0);

	uint8_t wal1_hdronly[WAL_SIZE_FROM_FRAMES(0)] = {0};
	make_wal_hdr(wal1_hdronly, 0, 18, 103);
	uint8_t wal2_hdronly[WAL_SIZE_FROM_FRAMES(0)] = {0};
	make_wal_hdr(wal2_hdronly, 0, 17, 103);
	prepare_wals(buf, wal1_hdronly, sizeof(wal1_hdronly), wal2_hdronly, sizeof(wal2_hdronly));
	sqlite3 *db;
	int rv = sqlite3_open(buf, &db);
	munit_assert_int(rv, ==, SQLITE_OK);
	rv = sqlite3_exec(db,
			  "PRAGMA page_size=" DB_PAGE_SIZE_STR ";"
			  "PRAGMA journal_mode=WAL;"
			  "PRAGMA wal_autocheckpoint=0",
			  NULL, NULL, NULL);
	munit_assert_int(rv, ==, SQLITE_OK);
	rv = sqlite3_exec(db, "CREATE TABLE foo (n INTEGER)", NULL, NULL, NULL);
	munit_assert_int(rv, ==, SQLITE_OK);
	rv = sqlite3_close(db);
	munit_assert_int(rv, ==, SQLITE_OK);

	check_wals(buf, WAL_SIZE_FROM_FRAMES(0), WAL_SIZE_FROM_FRAMES(2));

	return MUNIT_OK;
}

TEST(vfs2, rollback, set_up, tear_down, 0, NULL)
{
	struct fixture *f = data;
	char buf[PATH_MAX];

	snprintf(buf, PATH_MAX, "%s/%s", f->dir, "test.db");

	sqlite3 *db;
	int rv = sqlite3_open(buf, &db);
	munit_assert_int(rv, ==, SQLITE_OK);

	rv = sqlite3_exec(db,
			  "PRAGMA journal_mode=WAL;"
			  "PRAGMA wal_autocheckpoint=0",
			  NULL, NULL, NULL);
	munit_assert_int(rv, ==, SQLITE_OK);
	rv = sqlite3_exec(db, "CREATE TABLE foo (n INTEGER)", NULL, NULL, NULL);
	munit_assert_int(rv, ==, SQLITE_OK);
	sqlite3_file *fp;
	sqlite3_file_control(db, "main", SQLITE_FCNTL_FILE_POINTER, &fp);
	struct vfs2_wal_slice sl;
	rv = vfs2_poll(fp, NULL, NULL, &sl);
	munit_assert_int(rv, ==, 0);
	rv = vfs2_unhide(fp);
	rv = sqlite3_exec(db, "BEGIN", NULL, NULL, NULL);
	munit_assert_int(rv, ==, SQLITE_OK);
	char sql[100];
	for (unsigned i = 0; i < 500; i++) {
		snprintf(sql, sizeof(sql), "INSERT INTO foo (n) VALUES (%d)", i);
		rv = sqlite3_exec(db, sql, NULL, NULL, NULL);
		munit_assert_int(rv, ==, SQLITE_OK);
	}
	rv = sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
	munit_assert_int(rv, ==, SQLITE_OK);
	rv = sqlite3_close(db);
	munit_assert_int(rv, ==, SQLITE_OK);

	return MUNIT_OK;
}
