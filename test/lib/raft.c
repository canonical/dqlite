#include "./raft.h"

void test_raft_emit(struct raft_logger *l,
		    int level,
		    raft_time time,
		    const char *file,
		    int line,
		    const char *format,
		    ...)
{
	va_list args;
	struct test_logger t;
	(void)l;
	(void)time;
	(void)level;
	(void)file;
	(void)line;
	va_start(args, format);
	test_logger_emit(&t, RAFT_DEBUG, format, args);
	va_end(args);
}
