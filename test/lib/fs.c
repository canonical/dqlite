#ifndef _WIN32
#include <ftw.h>
#endif
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <uv.h>

#include "fs.h"
#include "munit.h"

char *test_dir_setup()
{
	char *dir = munit_malloc(strlen(TEST__DIR_TEMPLATE) + 1);

	strcpy(dir, TEST__DIR_TEMPLATE);

	uv_fs_t req;
	int rv = uv_fs_mkdtemp(NULL, &req, dir, NULL);
	munit_assert_int(rv, ==, 0);
	strcpy(dir, req.path);
	uv_fs_req_cleanup(&req);

	return dir;
}

#ifndef _WIN32
static int test__dir_tear_down_nftw_fn(const char *path,
				       const struct stat *sb,
				       int type,
				       struct FTW *ftwb)
{
	int rc;

	(void)sb;
	(void)type;
	(void)ftwb;

	rc = remove(path);
	munit_assert_int(rc, ==, 0);

	return 0;
}
#else
static void test_dir_remove_recursive(const char *dir)
{
	uv_fs_t scan_req;
	uv_dirent_t entry;
	int n = uv_fs_scandir(NULL, &scan_req, dir, 0, NULL);
	munit_assert_int(n, >=, 0);
	for (;;) {
		int rv = uv_fs_scandir_next(&scan_req, &entry);
		if (rv == UV_EOF) {
			break;
		}
		munit_assert_int(rv, ==, 0);
		char path[1024];
		int printed = snprintf(path, sizeof path, "%s/%s", dir, entry.name);
		munit_assert_int(printed, >=, 0);
		munit_assert_size((size_t)printed, <, sizeof path);
		if (entry.type == UV_DIRENT_DIR) {
			test_dir_remove_recursive(path);
		} else {
			uv_fs_t unlink_req;
			rv = uv_fs_unlink(NULL, &unlink_req, path, NULL);
			munit_assert_int(rv, ==, 0);
			uv_fs_req_cleanup(&unlink_req);
		}
	}
	uv_fs_req_cleanup(&scan_req);
	uv_fs_t rmdir_req;
	int rv = uv_fs_rmdir(NULL, &rmdir_req, dir, NULL);
	munit_assert_int(rv, ==, 0);
	uv_fs_req_cleanup(&rmdir_req);
}
#endif

void test_dir_tear_down(char *dir)
{
#ifndef _WIN32
	int rc;
#endif

	if (dir == NULL) {
		return;
	}

#ifdef _WIN32
	test_dir_remove_recursive(dir);
#else
	rc = nftw(dir, test__dir_tear_down_nftw_fn, 10,
		  FTW_DEPTH | FTW_MOUNT | FTW_PHYS);
	munit_assert_int(rc, ==, 0);
#endif
	free(dir);
}
