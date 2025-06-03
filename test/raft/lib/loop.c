#include "loop.h"

void test_loop_walk_cb(uv_handle_t *handle, void *arg)
{
    (void)arg;
    munit_logf(MUNIT_LOG_INFO, "handle %s (%d)", uv_handle_type_name(handle->type), handle->type);
}
