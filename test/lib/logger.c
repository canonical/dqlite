#include <stdarg.h>
#include <stdio.h>

#include "logger.h"
#include "munit.h"

struct test_logger
{
	unsigned id;
	void *data;
};

static void test_logger__emit(void *data,
			      int level,
			      const char *format,
			      va_list args)
{
	struct test_logger *t = data;
	char buf[1024];
	const char *level_name;
	int i;

	(void)data;

	switch (level) {
		case DQLITE_LOG_DEBUG:
			level_name = "DEBUG";
			break;
		case DQLITE_LOG_INFO:
			level_name = "INFO ";
			break;
		case DQLITE_LOG_WARN:
			level_name = "WARN ";
			break;
		case DQLITE_LOG_ERROR:
			level_name = "ERROR";
			break;
	};

	buf[0] = 0;

	sprintf(buf + strlen(buf), "%2d -> [%s] ", t->id, level_name);

	vsnprintf(buf + strlen(buf), 1024 - strlen(buf), format, args);
	munit_log(MUNIT_LOG_INFO, buf);
	return;

	snprintf(buf + strlen(buf), 1024 - strlen(buf), " ");

	for (i = strlen(buf); i < 85; i++) {
		buf[i] = ' ';
	}

	munit_log(MUNIT_LOG_INFO, buf);
}

void test_logger_setup(const MunitParameter params[], struct dqlite_logger *l)
{
	struct test_logger *t;

	(void)params;

	t = munit_malloc(sizeof *t);
	t->data = NULL;

	l->data = t;
	l->emit = test_logger__emit;
}

void test_logger_tear_down(struct dqlite_logger *l)
{
	free(l->data);
}
