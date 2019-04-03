#ifndef DQLITE_TEST_FS_H
#define DQLITE_TEST_FS_H

#define TEST__DIR_TEMPLATE "/tmp/dqlite-test-XXXXXX"

/* Setup a temporary directory. */
char *test_dir_setup();

/* Remove the temporary directory. */
void test_dir_tear_down(char *dir);

#endif /* DQLITE_TEST_FS_H */
