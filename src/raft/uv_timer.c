#include "./uv.h"
#include "./heap.h"


static void uvTimerCallback(uv_timer_t* handle) {
    struct raft_timer *req = handle->data;
    req->cb(req);
}

static void uvTimerFree(uv_handle_t *handle) {
    RaftHeapFree(handle);
}

int UvTimerStart(struct raft_io *io, struct raft_timer *req, uint64_t timeout, uint64_t repeat, raft_timer_cb cb) {
    int rv;
    struct uv *uv = io->impl;
    assert(!uv->closing);

    uv_timer_t *timer = RaftHeapMalloc(sizeof *timer);
    if (timer == NULL) {
        return RAFT_NOMEM;
    }
    rv = uv_timer_init(uv->loop, timer);
    if (rv != 0) {
        goto error;
    }

    timer->data = req;
    rv = uv_timer_start(timer, uvTimerCallback, timeout, repeat);
    if (rv != 0) {
        goto error;
    }
    req->handle = timer;
    req->cb = cb;
    return RAFT_OK;

error:
    RaftHeapFree(timer);
    return rv;
}

int UvTimerStop(struct raft_io *io, struct raft_timer *req) {
    (void)io;

    int rv;
    uv_timer_t *timer = req->handle;
    if (timer == NULL) {
        return RAFT_OK;
    }

    rv = uv_timer_stop(timer);
    if (rv != 0) {
        return rv;
    }
    uv_close((uv_handle_t*)timer, uvTimerFree);
    req->handle = NULL;
    return RAFT_OK;
}
