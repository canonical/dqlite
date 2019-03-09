/**
 * Test logger.
 */

#ifndef TEST_LOGGER_H
#define TEST_LOGGER_H

#include "../../include/dqlite.h"

#include "munit.h"

void test_logger_setup(const MunitParameter params[], struct dqlite_logger *l);
void test_logger_tear_down(struct dqlite_logger *l);

#endif /* TEST_LOGGER_H */
