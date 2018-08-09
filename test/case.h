/******************************************************************************
 *
 * Common utilities accross all test cases.
 *
 *****************************************************************************/

#ifndef DQLITE_TEST_CASE_H
#define DQLITE_TEST_CASE_H

#include "munit.h"

void *test_case_setup(const MunitParameter params[], void *user_data);
void  test_case_tear_down(void *data);

#endif /* DQLITE_TEST_CASE_H */
