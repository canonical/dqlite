/**
 * Test logger.
 */

#ifndef TEST_LOGGER_H
#define TEST_LOGGER_H

#include "../../include/dqlite.h"

#include "munit.h"

void test_logger_setup(const MunitParameter params[], struct dqlite_logger *l);
void test_logger_tear_down(struct dqlite_logger *l);

#define FIXTURE_LOGGER struct dqlite_logger logger;
#define SETUP_LOGGER test_logger_setup(params, &f->logger);
#define TEAR_DOWN_LOGGER test_logger_tear_down(&f->logger);

#endif /* TEST_LOGGER_H */
