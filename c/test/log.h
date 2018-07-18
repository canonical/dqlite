#ifndef DQLITE_TEST_LOG_H
#define DQLITE_TEST_LOG_H

#include <stdio.h>

#include "../include/dqlite.h"

dqlite_logger *test_logger();

typedef struct test_log test_log;

test_log *test_log_open();
FILE *    test_log_stream(test_log *);
int       test_log_is_empty(test_log *);
char *    test_log_output(test_log *);
void      test_log_close(test_log *);
void      test_log_destroy(test_log *);

#endif /* DQLITE_TEST_LOG_H */
