#include "../../include/dqlite.h"
#include "../../src/server.h"
#include "../lib/fs.h"
#include "../lib/munit.h"
#include "../lib/runner.h"

#include <stdatomic.h>
#include <stdio.h>
#include <time.h>

SUITE(server);

#define N_SERVERS 3

struct evil_connect_context
{
	dqlite_connect_func base;
	atomic_int fd;
	atomic_bool fail;
};

struct fixture
{
	char *dirs[N_SERVERS];
	dqlite_server *servers[N_SERVERS];
	struct evil_connect_context evil[N_SERVERS];
};

static void *setup(const MunitParameter params[], void *user_data)
{
	(void)params;
	(void)user_data;
	struct fixture *f = munit_calloc(1, sizeof *f);
	unsigned i;
	int rv;

	for (i = 0; i < N_SERVERS; i += 1) {
		f->dirs[i] = test_dir_setup();
		rv = dqlite_server_create(f->dirs[i], &f->servers[i]);
		munit_assert_int(rv, ==, 0);
		f->evil[i].base = f->servers[i]->connect;
	}

	return f;
}

static void teardown(void *data)
{
	struct fixture *f = data;
	unsigned i;

	for (i = 0; i < N_SERVERS; i += 1) {
		dqlite_server_destroy(f->servers[i]);
		test_dir_tear_down(f->dirs[i]);
	}
	free(f);
}

/* XXX this could just be a function */
#define PREPARE_FILE(i, name, ...)                              \
	do {                                                    \
		char path[100];                                 \
		snprintf(path, 100, "%s/%s", f->dirs[i], name); \
		FILE *fp = fopen(path, "w+");                   \
		fprintf(fp, __VA_ARGS__);                       \
		fclose(fp);                                     \
	} while (0)

#define NODE(x) x

#define NODE0_ID "3297041220608546238"

static int evil_connect(void *arg, const char *addr, int *out)
{
	struct evil_connect_context *evil = arg;
	int fd;
	int rv;

	if (atomic_load(&evil->fail)) {
		*out = -1;
		return 1;
	} else {
		rv = evil->base(NULL, addr, &fd);
		atomic_store(&evil->fd, fd);
		*out = fd;
		return rv;
	}
}

void start_each_server(struct fixture *f)
{
	const char *addrs[] = {"127.0.0.1:8880", "127.0.0.1:8881"};
	int rv;

	rv = dqlite_server_set_address(f->servers[0], "127.0.0.1:8880");
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_set_auto_bootstrap(f->servers[0], true);
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_set_connect_func(f->servers[0], evil_connect,
					    &f->evil);
	f->servers[0]->refresh_period = 100;
	rv = dqlite_server_start(f->servers[0]);
	munit_assert_int(rv, ==, 0);

	rv = dqlite_server_set_address(f->servers[1], "127.0.0.1:8881");
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_set_auto_join(f->servers[1], addrs, 1);
	munit_assert_int(rv, ==, 0);
	f->servers[1]->refresh_period = 100;
	rv = dqlite_server_start(f->servers[1]);
	munit_assert_int(rv, ==, 0);

	rv = dqlite_server_set_address(f->servers[2], "127.0.0.1:8882");
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_set_auto_join(f->servers[2], addrs, 2);
	munit_assert_int(rv, ==, 0);
	f->servers[2]->refresh_period = 100;
	rv = dqlite_server_start(f->servers[2]);
	munit_assert_int(rv, ==, 0);
}

void stop_each_server(struct fixture *f)
{
	int rv;

	rv = dqlite_server_stop(f->servers[2]);
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_stop(f->servers[1]);
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_stop(f->servers[0]);
	munit_assert_int(rv, ==, 0);
}

TEST(server, restart_follower, setup, teardown, 0, NULL)
{
	struct fixture *f = data;
	struct timespec ts = {0};
	int rv;

	/* Between operations we sleep for 200 milliseconds, twice
	 * the configured refresh period, so that the refresh task
	 * has a chance to be triggered. */
	ts.tv_nsec = 200 * 1000 * 1000;

	start_each_server(f);

	nanosleep(&ts, NULL);

	rv = dqlite_server_stop(f->servers[1]);
	munit_assert_int(rv, ==, 0);

	nanosleep(&ts, NULL);

	rv = dqlite_server_start(f->servers[1]);
	munit_assert_int(rv, ==, 0);

	nanosleep(&ts, NULL);

	stop_each_server(f);

	return MUNIT_OK;
}

TEST(server, restart_leader, setup, teardown, 0, NULL)
{
	struct fixture *f = data;
	struct timespec ts = {0};
	int rv;

	/* Between operations we sleep for 200 milliseconds, twice
	 * the configured refresh period, so that the refresh task
	 * has a chance to be triggered. */
	ts.tv_nsec = 200 * 1000 * 1000;

	start_each_server(f);

	nanosleep(&ts, NULL);

	rv = dqlite_server_stop(f->servers[0]);
	munit_assert_int(rv, ==, 0);

	nanosleep(&ts, NULL);

	rv = dqlite_server_start(f->servers[0]);
	munit_assert_int(rv, ==, 0);

	nanosleep(&ts, NULL);

	stop_each_server(f);

	return MUNIT_OK;
}

TEST(server, bad_info_file, setup, teardown, 0, NULL)
{
	struct fixture *f = data;
	int rv;

	PREPARE_FILE(NODE(0), "server-info", "blah");

	rv = dqlite_server_set_address(f->servers[0], "127.0.0.1:8880");
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_set_auto_bootstrap(f->servers[0], true);
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_start(f->servers[0]);
	munit_assert_int(rv, !=, 0);

	return MUNIT_OK;
}

TEST(server, bad_node_store, setup, teardown, 0, NULL)
{
	struct fixture *f = data;
	int rv;

	PREPARE_FILE(NODE(0), "server-info",
		     "v1\n127.0.0.1:8880\n" NODE0_ID "\n");
	PREPARE_FILE(NODE(0), "node-store", "blah");

	rv = dqlite_server_set_address(f->servers[0], "127.0.0.1:8880");
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_set_auto_bootstrap(f->servers[0], true);
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_start(f->servers[0]);
	munit_assert_int(rv, !=, 0);

	return MUNIT_OK;
}

TEST(server, node_store_but_no_info, setup, teardown, 0, NULL)
{
	struct fixture *f = data;
	int rv;

	PREPARE_FILE(NODE(0), "node-store",
		     "v1\n127.0.0.1:8880\n" NODE0_ID "\nvoter\n");

	rv = dqlite_server_set_address(f->servers[0], "127.0.0.1:8880");
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_set_auto_bootstrap(f->servers[0], true);
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_start(f->servers[0]);
	munit_assert_int(rv, !=, 0);

	return MUNIT_OK;
}

TEST(server, missing_bootstrap, setup, teardown, 0, NULL)
{
	struct fixture *f = data;
	const char *addrs[] = {"127.0.0.1:8880"};
	int rv;

	rv = dqlite_server_set_address(f->servers[1], "127.0.0.1:8881");
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_set_auto_join(f->servers[1], addrs, 1);
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_start(f->servers[1]);
	munit_assert_int(rv, !=, 0);

	return MUNIT_OK;
}

TEST(server, start_twice, setup, teardown, 0, NULL)
{
	struct fixture *f = data;
	int rv;

	rv = dqlite_server_set_address(f->servers[0], "127.0.0.1:8880");
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_set_auto_bootstrap(f->servers[0], true);
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_start(f->servers[0]);
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_start(f->servers[0]);
	munit_assert_int(rv, !=, 0);
	rv = dqlite_server_stop(f->servers[0]);
	munit_assert_int(rv, ==, 0);

	return MUNIT_OK;
}

TEST(server, stop_twice, setup, teardown, 0, NULL)
{
	struct fixture *f = data;
	int rv;

	rv = dqlite_server_set_address(f->servers[0], "127.0.0.1:8880");
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_set_auto_bootstrap(f->servers[0], true);
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_start(f->servers[0]);
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_stop(f->servers[0]);
	munit_assert_int(rv, ==, 0);
	rv = dqlite_server_stop(f->servers[0]);
	munit_assert_int(rv, !=, 0);

	return MUNIT_OK;
}

dqlite *simple_open(dqlite_server *server)
{
	dqlite *db;
	int rv;

	rv = dqlite_open(server, "test", &db, 0);
	munit_assert_int(rv, ==, SQLITE_OK);
	return db;
}

dqlite_stmt *simple_prepare(dqlite *db, const char *sql)
{
	dqlite_stmt *stmt;
	int rv;

	rv = dqlite_prepare(db, sql, -1, &stmt, NULL);
	munit_assert_int(rv, ==, 0);
	return stmt;
}

TEST(server, insert_and_select, setup, teardown, 0, NULL)
{
	struct fixture *f = data;
	dqlite *db;
	dqlite *db2;
	dqlite *db3;
	dqlite_stmt *stmt;
	int rv;

	start_each_server(f);

	db = simple_open(f->servers[0]);

	stmt = simple_prepare(
	    db, "CREATE TABLE pairs (k TEXT, v INTEGER, f FLOAT, b BLOB)");
	rv = dqlite_step(stmt);
	munit_assert_int(rv, ==, SQLITE_DONE);
	rv = dqlite_finalize(stmt);
	munit_assert_int(rv, ==, SQLITE_OK);

	stmt = simple_prepare(
	    db, "INSERT INTO pairs (k, v, f, b) VALUES (?, ?, ?, ?)");
	rv = dqlite_bind_text(stmt, 1, "blah", -1, SQLITE_TRANSIENT);
	munit_assert_int(rv, ==, SQLITE_OK);
	rv = dqlite_bind_int64(stmt, 2, 17);
	munit_assert_int(rv, ==, SQLITE_OK);
	rv = dqlite_bind_double(stmt, 3, 0.5);
	munit_assert_int(rv, ==, 0);
	rv = dqlite_bind_blob(stmt, 4, "this is a blob", 14, SQLITE_TRANSIENT);
	munit_assert_int(rv, ==, 0);

	rv = dqlite_step(stmt);
	munit_assert_int(rv, ==, SQLITE_DONE);
	rv = dqlite_finalize(stmt);
	munit_assert_int(rv, ==, SQLITE_OK);

	stmt = simple_prepare(db, "SELECT * FROM pairs");
	rv = dqlite_step(stmt);
	munit_assert_int(rv, ==, SQLITE_ROW);
	const char *txt = (const char *)dqlite_column_text(stmt, 0);
	munit_assert_string_equal(txt, "blah");
	int64_t n = dqlite_column_int64(stmt, 1);
	munit_assert_int64(n, ==, 17);
	double d = dqlite_column_double(stmt, 2);
	munit_assert_double(d, ==, 0.5);
	rv = dqlite_step(stmt);
	munit_assert_int(rv, ==, SQLITE_DONE);
	rv = dqlite_finalize(stmt);
	munit_assert_int(rv, ==, SQLITE_OK);

	db2 = simple_open(f->servers[1]);

	stmt = simple_prepare(db2, "INSERT INTO pairs (k, v) VALUES (?, ?)");
	rv = dqlite_bind_text(stmt, 1, "glug", -1, SQLITE_STATIC);
	munit_assert_int(rv, ==, SQLITE_OK);
	rv = dqlite_bind_int64(stmt, 2, 22);
	munit_assert_int(rv, ==, SQLITE_OK);
	rv = dqlite_step(stmt);
	munit_assert_int(rv, ==, SQLITE_DONE);
	rv = dqlite_finalize(stmt);
	munit_assert_int(rv, ==, SQLITE_OK);

	stmt = simple_prepare(db2, "SELECT * FROM pairs");
	rv = dqlite_step(stmt);
	munit_assert_int(rv, ==, SQLITE_ROW);
	txt = (const char *)dqlite_column_text(stmt, 0);
	munit_assert_string_equal(txt, "blah");
	n = dqlite_column_int64(stmt, 1);
	munit_assert_int64(n, ==, 17);
	rv = dqlite_step(stmt);
	munit_assert_int(rv, ==, SQLITE_ROW);
	txt = (const char *)dqlite_column_text(stmt, 0);
	munit_assert_string_equal(txt, "glug");
	n = dqlite_column_int64(stmt, 1);
	munit_assert_int64(n, ==, 22);
	rv = dqlite_step(stmt);
	munit_assert_int(rv, ==, SQLITE_DONE);
	rv = dqlite_finalize(stmt);
	munit_assert_int(rv, ==, SQLITE_OK);

	db3 = simple_open(f->servers[2]);

	stmt = simple_prepare(db3, "SELECT * FROM pairs WHERE v = 3");
	rv = dqlite_step(stmt);
	munit_assert_int(rv, ==, SQLITE_DONE);
	rv = dqlite_finalize(stmt);
	munit_assert_int(rv, ==, SQLITE_OK);

	rv = dqlite_close(db3);
	munit_assert_int(rv, ==, SQLITE_OK);

	rv = dqlite_close(db2);
	munit_assert_int(rv, ==, SQLITE_OK);

	rv = dqlite_close(db);
	munit_assert_int(rv, ==, SQLITE_OK);

	stop_each_server(f);

	return MUNIT_OK;
}

TEST(server, lots_of_rows, setup, teardown, 0, NULL)
{
	struct fixture *f = data;
	dqlite *db;
	dqlite_stmt *stmt;
	int i;
	char buf[100];
	int rv;

	start_each_server(f);

	db = simple_open(f->servers[0]);

	stmt = simple_prepare(db, "CREATE TABLE pairs (k TEXT, v INTEGER)");
	rv = dqlite_step(stmt);
	munit_assert_int(rv, ==, SQLITE_DONE);
	rv = dqlite_finalize(stmt);
	munit_assert_int(rv, ==, SQLITE_OK);

	stmt = simple_prepare(db, "INSERT INTO pairs (k, v) VALUES (?, ?)");
	for (i = 0; i < 10000; i += 1) {
		snprintf(buf, 100, "%d", i);
		rv = dqlite_bind_text(stmt, 1, buf, -1, SQLITE_TRANSIENT);
		munit_assert_int(rv, ==, SQLITE_OK);
		rv = dqlite_bind_int64(stmt, 2, i);
		munit_assert_int(rv, ==, SQLITE_OK);
		rv = dqlite_step(stmt);
		munit_assert_int(rv, ==, SQLITE_DONE);
		rv = dqlite_reset(stmt);
		munit_assert_int(rv, ==, SQLITE_OK);
	}
	rv = dqlite_finalize(stmt);
	munit_assert_int(rv, ==, SQLITE_OK);

	stmt = simple_prepare(db, "SELECT * FROM pairs");
	for (i = 0; i < 10000; i += 1) {
		snprintf(buf, 100, "%d", i);
		rv = dqlite_step(stmt);
		munit_assert_int(rv, ==, SQLITE_ROW);
		const char *txt = (const char *)dqlite_column_text(stmt, 0);
		munit_assert_string_equal(txt, buf);
		int64_t n = dqlite_column_int64(stmt, 1);
		munit_assert_int64(n, ==, i);
	}
	rv = dqlite_step(stmt);
	munit_assert_int(rv, ==, SQLITE_DONE);
	rv = dqlite_finalize(stmt);
	munit_assert_int(rv, ==, SQLITE_OK);

	rv = dqlite_close(db);
	munit_assert_int(rv, ==, SQLITE_OK);

	stop_each_server(f);

	return MUNIT_OK;
}

TEST(server, prepare_connect_fail, setup, teardown, 0, NULL)
{
	struct fixture *f = data;
	dqlite *db;
	dqlite_stmt *stmt;
	int rv;

	start_each_server(f);

	db = simple_open(f->servers[0]);
	atomic_store(&f->evil[0].fail, true);
	rv = dqlite_prepare(db, "CREATE TABLE pairs (k TEXT, v INTEGER)", -1,
			    &stmt, NULL);
	munit_assert_int(rv, !=, 0);
	rv = dqlite_close(db);

	stop_each_server(f);

	return MUNIT_OK;
}
