/**
 * Test logger.
 */

#ifndef TEST_LOGGER_H
#define TEST_LOGGER_H

#include "../../src/logger.h"

#include "munit.h"

void test_logger_setup(const MunitParameter params[], struct logger *l);
void test_logger_tear_down(struct logger *l);

struct test_logger
{
	unsigned id;
	void *data;
};

void test_logger_emit(void *data, int level, const char *fmt, va_list args);

#define FIXTURE_LOGGER struct logger logger;
#define SETUP_LOGGER test_logger_setup(params, &f->logger);
#define TEAR_DOWN_LOGGER test_logger_tear_down(&f->logger);

#endif /* TEST_LOGGER_H */
