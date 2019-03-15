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
#define SETUP_LOGGER SETUP_LOGGER_X(f)
#define TEAR_DOWN_LOGGER TEAR_DOWN_LOGGER_X(f)

#define SETUP_LOGGER_X(F) test_logger_setup(params, &F->logger);
#define TEAR_DOWN_LOGGER_X(F) test_logger_tear_down(&F->logger);

#endif /* TEST_LOGGER_H */
