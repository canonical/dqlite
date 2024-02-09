#include "loop.h"

void test_loop_walk_cb(uv_handle_t *handle, void *arg)
{
    (void)arg;
    munit_logf(MUNIT_LOG_INFO, "handle %d", handle->type);
}
