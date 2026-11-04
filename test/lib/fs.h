#ifndef DQLITE_TEST_FS_H
#define DQLITE_TEST_FS_H

#ifdef _WIN32
#define TEST__DIR_TEMPLATE "./dqlite-test-XXXXXX"
#else
#define TEST__DIR_TEMPLATE "/tmp/dqlite-test-XXXXXX"
#endif

/* Setup a temporary directory. */
char *test_dir_setup(void);

/* Remove the temporary directory. */
void test_dir_tear_down(char *dir);

#endif /* DQLITE_TEST_FS_H */
