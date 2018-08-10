#define _GNU_SOURCE

#include <ftw.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "fs.h"
#include "munit.h"

const char *test_dir_setup() {
	char *dir = munit_malloc(strlen(TEST__DIR_TEMPLATE) + 1);

	strcpy(dir, TEST__DIR_TEMPLATE);

	munit_assert_ptr_not_null(mkdtemp(dir));

	return dir;
}

static int test__dir_tear_down_nftw_fn(const char *       path,
                                       const struct stat *sb,
                                       int                type,
                                       struct FTW *       ftwb) {
	int rc;

	(void)sb;
	(void)type;
	(void)ftwb;

	rc = remove(path);
	munit_assert_int(rc, ==, 0);

	return 0;
}

void test_dir_tear_down(const char *dir) {
	int rc;

	rc = nftw(dir,
	          test__dir_tear_down_nftw_fn,
	          10,
	          FTW_DEPTH | FTW_MOUNT | FTW_PHYS);
	munit_assert_int(rc, ==, 0);
}
