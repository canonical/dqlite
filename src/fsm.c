#include <raft.h>

#include "./lib/logger.h"

#include "command.h"
#include "fsm.h"

struct fsm
{
	struct dqlite_logger *logger;
	struct registry *registry;
};

static int fsm__apply_open(struct fsm *f, const struct command_open *c)
{
	struct db *db;
	int rc;

	rc = registry__db_get(f->registry, c->filename, &db);
	if (rc != 0) {
		return rc;
	}
	rc = db__open_follower(db);
	if (rc != 0) {
		return rc;
	}
	return 0;
}

static int fsm__apply_frames(struct fsm *f, const struct command_frames *c) {
	return 0;
}

static int fsm__apply(struct raft_fsm *fsm, const struct raft_buffer *buf)
{
	struct fsm *f = fsm->data;
	int type;
	void *command;
	int rc;
	rc = command__decode(buf, &type, &command);
	if (rc != 0) {
		errorf(f->logger, "fsm: decode command: %d", rc);
		goto err;
	}
	switch (type) {
		case COMMAND_OPEN:
			rc = fsm__apply_open(f, command);
			break;
		case COMMAND_FRAMES:
			rc = fsm__apply_frames(f, command);
			break;
		default:
			rc = RAFT_ERR_IO_MALFORMED;
			goto err_after_command_decode;
	}
	raft_free(command);

	return 0;
err_after_command_decode:
	raft_free(command);
err:
	return rc;
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

int fsm__init(struct raft_fsm *fsm,
	      struct dqlite_logger *logger,
	      struct registry *registry)
{
	struct fsm *f = raft_malloc(sizeof *fsm);

	if (f == NULL) {
		return DQLITE_NOMEM;
	}

	f->logger = logger;
	f->registry = registry;

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
