#include "fsm.h"

struct fsm
{
	struct dqlite_logger *logger;
};

static int fsm__apply(struct raft_fsm *fsm, const struct raft_buffer *buf)
{
	struct fsm *f = fsm->data;
	return 0;
}

static int fsm__snapshot(struct raft_fsm *fsm,
			 struct raft_buffer *bufs[],
			 unsigned *n_bufs)
{
	struct fsm *f = fsm->data;
	return 0;
}

static int fsm__restore(struct raft_fsm *fsm, struct raft_buffer *buf)
{
	struct fsm *f = fsm->data;
	return 0;
}

int fsm__init(struct raft_fsm *fsm, struct dqlite_logger *logger)
{
	struct fsm *f = raft_malloc(sizeof *fsm);

	if (f == NULL) {
		return DQLITE_NOMEM;
	}

	f->logger = logger;

	fsm->version = 1;
	fsm->data = f;
	fsm->apply = fsm__apply;
	fsm->snapshot = fsm__snapshot;
	fsm->restore = fsm__restore;

	return 0;
}

void fsm__close(struct raft_fsm *fsm)
{
	struct fsm *f = fsm->data;
	raft_free(f);
}
