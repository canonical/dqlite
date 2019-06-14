#include "./raft.h"

void test_raft_emit(struct raft_logger *l,
		    int level,
		    unsigned server_id,
		    raft_time time,
		    const char *format,
		    ...)
{
	va_list args;
	struct test_logger t;
	(void)time;
	va_start(args, format);
	t.id = server_id;
	test_logger_emit(&t, RAFT_DEBUG, format, args);
	va_end(args);
}
