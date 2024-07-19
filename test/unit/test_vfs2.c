#include "../../src/lib/byte.h"
#include "../../src/vfs2.h"
#include "../lib/fs.h"
#include "../lib/runner.h"

#include <sqlite3.h>

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#define NUM_NODES 3
#define PAGE_SIZE 512
#define PAGE_SIZE_STR "512"

#define OK(rv) munit_assert_int(rv, ==, 0)

SUITE(vfs2);

struct node {
	sqlite3_vfs *vfs;
	char *vfs_name;
	char *dir;
};

struct fixture {
	struct node nodes[NUM_NODES];
};

static void *set_up(const MunitParameter params[], void *user_data)
{
	(void)params;
	(void)user_data;
	struct fixture *f = munit_malloc(sizeof(*f));
	struct node *node;
	for (unsigned i = 0; i < NUM_NODES; i++) {
		node = &f->nodes[i];
		node->dir = test_dir_setup();
		node->vfs_name = sqlite3_mprintf("vfs2-%u", i);
		munit_assert_ptr_not_null(node->vfs_name);
		node->vfs = vfs2_make(sqlite3_vfs_find("unix"), node->vfs_name);
		munit_assert_ptr_not_null(node->vfs);
		sqlite3_vfs_register(node->vfs, 0);
	}
	return f;
}

static void tear_down(void *data)
{
	struct fixture *f = data;
	const struct node *node;
	for (unsigned i = 0; i < NUM_NODES; i++) {
		node = &f->nodes[i];
		sqlite3_vfs_unregister(node->vfs);
		vfs2_destroy(node->vfs);
		sqlite3_free(node->vfs_name);
		test_dir_tear_down(node->dir);
	}
	free(f);
}

/**
 * Open a connection to a test database for this node.
 */
static sqlite3 *open_test_db(const struct node *node)
{
	char buf[PATH_MAX];
	snprintf(buf, sizeof(buf), "%s/test.db", node->dir);
	sqlite3 *db;
	int rv = sqlite3_open_v2(buf, &db,
				 SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
				 node->vfs_name);
	munit_assert_int(rv, ==, SQLITE_OK);
	munit_assert_ptr_not_null(db);
	rv = sqlite3_exec(db,
			  "PRAGMA page_size=" PAGE_SIZE_STR
			  ";"
			  "PRAGMA journal_mode=WAL;"
			  "PRAGMA wal_autocheckpoint=0",
			  NULL, NULL, NULL);
	munit_assert_int(rv, ==, SQLITE_OK);
	return db;
}

/**
 * Write two WALs to disk with the given contents.
 */
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
		OK(ftruncate(fd1, 0));
		n = write(fd1, wal1, wal1_len);
		munit_assert_llong(n, ==, (ssize_t)wal1_len);
		close(fd1);
	}
	if (wal2 != NULL) {
		snprintf(buf, sizeof(buf), "%s-xwal2", dbname);
		int fd2 = open(buf, O_RDWR | O_CREAT,
			       S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
		munit_assert_int(fd2, !=, -1);
		OK(ftruncate(fd2, 0));
		n = write(fd2, wal2, wal2_len);
		munit_assert_llong(n, ==, (ssize_t)wal2_len);
		close(fd2);
	}
}

/**
 * Assert the lengths of WAL1 and WAL2 on disk.
 */
static void check_wals(const char *dbname, off_t wal1_len, off_t wal2_len)
{
	char buf[PATH_MAX];
	struct stat st;
	int rv;

	snprintf(buf, sizeof(buf), "%s-xwal1", dbname);
	rv = stat(buf, &st);
	munit_assert_true((rv == 0 && st.st_size == wal1_len) ||
			  (rv < 0 && errno == ENOENT && wal1_len == 0));

	snprintf(buf, sizeof(buf), "%s-xwal2", dbname);
	rv = stat(buf, &st);
	munit_assert_true((rv == 0 && st.st_size == wal2_len) ||
			  (rv < 0 && errno == ENOENT && wal2_len == 0));
}

static sqlite3_file *main_file(sqlite3 *db)
{
	sqlite3_file *fp;
	sqlite3_file_control(db, "main", SQLITE_FCNTL_FILE_POINTER, &fp);
	return fp;
}

/**
 * Single-node test with several transactions and a checkpoint.
 */
TEST(vfs2, basic, set_up, tear_down, 0, NULL)
{
	struct fixture *f = data;
	struct node *node = &f->nodes[0];
	int rv;

	sqlite3 *db = open_test_db(node);
	sqlite3_file *fp = main_file(db);
	OK(sqlite3_exec(db, "CREATE TABLE foo (bar INTEGER)", NULL, NULL,
			NULL));

	struct vfs2_wal_slice sl;
	OK(vfs2_poll(fp, NULL, &sl));
	OK(vfs2_unhide(fp));
	munit_assert_uint32(sl.start, ==, 0);
	munit_assert_uint32(sl.len, ==, 2);

	OK(sqlite3_exec(db, "INSERT INTO foo (bar) VALUES (17)", NULL, NULL,
			NULL));
	OK(vfs2_abort(fp));

	OK(sqlite3_exec(db, "INSERT INTO foo (bar) values (22)", NULL, NULL,
			NULL));
	OK(vfs2_poll(fp, NULL, &sl));
	munit_assert_uint32(sl.start, ==, 2);
	munit_assert_uint32(sl.len, ==, 1);
	OK(vfs2_unhide(fp));

	sqlite3_stmt *stmt;
	OK(sqlite3_prepare_v2(db, "SELECT * FROM foo", -1, &stmt, NULL));
	rv = sqlite3_step(stmt);
	munit_assert_int(rv, ==, SQLITE_ROW);
	munit_assert_int(sqlite3_column_count(stmt), ==, 1);
	munit_assert_int(sqlite3_column_int(stmt, 0), ==, 22);
	rv = sqlite3_step(stmt);
	munit_assert_int(rv, ==, SQLITE_DONE);

	int nlog;
	int nckpt;
	OK(sqlite3_wal_checkpoint_v2(db, "main", SQLITE_CHECKPOINT_PASSIVE,
				     &nlog, &nckpt));
	munit_assert_int(nlog, ==, 3);
	munit_assert_int(nckpt, ==, 3);

	OK(sqlite3_exec(db, "INSERT INTO foo (bar) VALUES (101)", NULL, NULL,
			NULL));

	OK(sqlite3_reset(stmt));
	rv = sqlite3_step(stmt);
	munit_assert_int(rv, ==, SQLITE_ROW);
	munit_assert_int(sqlite3_column_count(stmt), ==, 1);
	munit_assert_int(sqlite3_column_int(stmt, 0), ==, 22);
	/* Can't see the new row yet. */
	rv = sqlite3_step(stmt);
	munit_assert_int(rv, ==, SQLITE_DONE);

	dqlite_vfs_frame *frames;
	OK(vfs2_poll(fp, &frames, &sl));
	munit_assert_uint(sl.len, ==, 1);
	munit_assert_not_null(frames);
	munit_assert_not_null(frames[0].data);
	sqlite3_free(frames[0].data);
	sqlite3_free(frames);

	OK(vfs2_unhide(fp));

	OK(sqlite3_reset(stmt));
	rv = sqlite3_step(stmt);
	munit_assert_int(rv, ==, SQLITE_ROW);
	munit_assert_int(sqlite3_column_count(stmt), ==, 1);
	munit_assert_int(sqlite3_column_int(stmt, 0), ==, 22);
	rv = sqlite3_step(stmt);
	munit_assert_int(rv, ==, SQLITE_ROW);
	munit_assert_int(sqlite3_column_int(stmt, 0), ==, 101);
	rv = sqlite3_step(stmt);
	munit_assert_int(rv, ==, SQLITE_DONE);

	OK(sqlite3_finalize(stmt));
	sqlite3_close(db);
	return MUNIT_OK;
}

#define WAL_SIZE_FROM_FRAMES(n) (32 + (24 + PAGE_SIZE) * (n))

/**
 * Create a WAL header with the sequence number and salts set to the
 * given values.
 */
static void make_wal_hdr(uint8_t *buf,
			 uint32_t ckpoint_seqno,
			 uint32_t salt1,
			 uint32_t salt2)
{
	uint8_t *p = buf;

	/* checksum */
	BytePutBe32(0x377f0683, p);
	p += 4;
	BytePutBe32(3007000, p);
	p += 4;
	BytePutBe32(PAGE_SIZE, p);
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

/**
 * When only one WAL is nonempty at startup, that WAL becomes WAL-cur.
 */
TEST(vfs2, startup_one_nonempty, set_up, tear_down, 0, NULL)
{
	struct fixture *f = data;
	struct node *node = &f->nodes[0];
	char buf[PATH_MAX];

	snprintf(buf, PATH_MAX, "%s/%s", node->dir, "test.db");

	check_wals(buf, 0, 0);

	uint8_t wal2_hdronly[WAL_SIZE_FROM_FRAMES(0)] = { 0 };
	make_wal_hdr(wal2_hdronly, 0, 17, 103);
	prepare_wals(buf, NULL, 0, wal2_hdronly, sizeof(wal2_hdronly));
	sqlite3 *db = open_test_db(node);
	OK(sqlite3_exec(db, "CREATE TABLE foo (n INTEGER)", NULL, NULL, NULL));
	OK(sqlite3_close(db));

	check_wals(buf, WAL_SIZE_FROM_FRAMES(2), WAL_SIZE_FROM_FRAMES(0));

	return MUNIT_OK;
}

/**
 * When both WALs are nonempty at startup, the one with the higher salt1
 * value becomes WAL-cur.
 */
TEST(vfs2, startup_both_nonempty, set_up, tear_down, 0, NULL)
{
	struct fixture *f = data;
	struct node *node = &f->nodes[0];
	char buf[PATH_MAX];
	int rv;

	snprintf(buf, PATH_MAX, "%s/%s", node->dir, "test.db");
	check_wals(buf, 0, 0);

	uint8_t wal1_hdronly[WAL_SIZE_FROM_FRAMES(0)] = { 0 };
	make_wal_hdr(wal1_hdronly, 0, 18, 103);
	uint8_t wal2_hdronly[WAL_SIZE_FROM_FRAMES(0)] = { 0 };
	make_wal_hdr(wal2_hdronly, 0, 17, 103);
	prepare_wals(buf, wal1_hdronly, sizeof(wal1_hdronly), wal2_hdronly,
		     sizeof(wal2_hdronly));
	sqlite3 *db = open_test_db(node);
	rv = sqlite3_exec(db,
			  "PRAGMA page_size=" PAGE_SIZE_STR
			  ";"
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

/**
 * Single-node test of rolling back a transaction.
 */
TEST(vfs2, rollback, set_up, tear_down, 0, NULL)
{
	struct fixture *f = data;
	struct node *node = &f->nodes[0];
	char buf[PATH_MAX];
	int rv;

	snprintf(buf, PATH_MAX, "%s/%s", node->dir, "test.db");

	sqlite3 *db = open_test_db(node);

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
	rv = vfs2_poll(fp, NULL, &sl);
	munit_assert_int(rv, ==, 0);
	rv = vfs2_unhide(fp);
	rv = sqlite3_exec(db, "BEGIN", NULL, NULL, NULL);
	munit_assert_int(rv, ==, SQLITE_OK);
	char sql[100];
	for (unsigned i = 0; i < 500; i++) {
		snprintf(sql, sizeof(sql), "INSERT INTO foo (n) VALUES (%d)",
			 i);
		rv = sqlite3_exec(db, sql, NULL, NULL, NULL);
		munit_assert_int(rv, ==, SQLITE_OK);
	}
	rv = sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
	munit_assert_int(rv, ==, SQLITE_OK);
	rv = sqlite3_close(db);
	munit_assert_int(rv, ==, SQLITE_OK);

	return MUNIT_OK;
}

/**
 * Two-node test covering the full replication cycle.
 */
TEST(vfs2, leader_and_follower, set_up, tear_down, 0, NULL)
{
	struct fixture *f = data;
	struct node *leader = &f->nodes[0];
	struct node *follower = &f->nodes[1];

	/* The leader executes and polls a transaction. */
	sqlite3 *leader_db = open_test_db(leader);
	OK(sqlite3_exec(leader_db, "CREATE TABLE foo (n INTEGER)", NULL, NULL,
			NULL));
	sqlite3_file *leader_fp = main_file(leader_db);
	dqlite_vfs_frame *frames;
	struct vfs2_wal_slice leader_sl;
	OK(vfs2_poll(leader_fp, &frames, &leader_sl));
	munit_assert_uint(leader_sl.len, ==, 2);

	/* The follower receives the transaction. */
	sqlite3 *follower_db = open_test_db(follower);
	sqlite3_file *follower_fp = main_file(follower_db);
	struct vfs2_wal_slice follower_sl;
	OK(vfs2_add_uncommitted(follower_fp, PAGE_SIZE, frames, leader_sl.len,
				&follower_sl));
	sqlite3_free(frames[0].data);
	sqlite3_free(frames[1].data);
	sqlite3_free(frames);

	/* The leader receives the follower's acknowledgement
	 * and applies the transaction locally. */
	OK(vfs2_unhide(leader_fp));

	/* The follower learns the new commit index and applies
	 * the transaction locally. */
	OK(vfs2_apply(follower_fp, follower_sl));

	sqlite3_close(follower_db);
	sqlite3_close(leader_db);
	return MUNIT_OK;
}
