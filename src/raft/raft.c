#include "../raft.h"

#include <string.h>

#include "../tracing.h"
#include "assert.h"
#include "byte.h"
#include "callbacks.h"
#include "configuration.h"
#include "convert.h"
#include "election.h"
#include "err.h"
#include "flags.h"
#include "heap.h"
#include "log.h"
#include "membership.h"

#define DEFAULT_ELECTION_TIMEOUT 1000          /* One second */
#define DEFAULT_HEARTBEAT_TIMEOUT 100          /* One tenth of a second */
#define DEFAULT_INSTALL_SNAPSHOT_TIMEOUT 30000 /* 30 seconds */
#define DEFAULT_SNAPSHOT_THRESHOLD 1024
#define DEFAULT_SNAPSHOT_TRAILING 2048

/* Number of milliseconds after which a server promotion will be aborted if the
 * server hasn't caught up with the logs yet. */
#define DEFAULT_MAX_CATCH_UP_ROUNDS 10
#define DEFAULT_MAX_CATCH_UP_ROUND_DURATION (5 * 1000)

int raft_version_number(void)
{
	return RAFT_VERSION_NUMBER;
}

static int ioFsmVersionCheck(struct raft *r,
			     struct raft_io *io,
			     struct raft_fsm *fsm);

int raft_init(struct raft *r,
	      struct raft_io *io,
	      struct raft_fsm *fsm,
	      const raft_id id,
	      const char *address)
{
	int rv;
	assert(r != NULL);

	rv = ioFsmVersionCheck(r, io, fsm);
	if (rv != 0) {
		goto err;
	}

	r->io = io;
	r->io->data = r;
	r->fsm = fsm;

	r->tracer = NULL;

	r->id = id;
	/* Make a copy of the address */
	r->address = RaftHeapMalloc(strlen(address) + 1);
	if (r->address == NULL) {
		rv = RAFT_NOMEM;
		goto err;
	}
	strcpy(r->address, address);
	r->current_term = 0;
	r->voted_for = 0;
	r->log = logInit();
	if (r->log == NULL) {
		rv = RAFT_NOMEM;
		goto err_after_address_alloc;
	}

	raft_configuration_init(&r->configuration);
	raft_configuration_init(&r->configuration_last_snapshot);
	r->configuration_committed_index = 0;
	r->configuration_uncommitted_index = 0;
	r->election_timeout = DEFAULT_ELECTION_TIMEOUT;
	r->heartbeat_timeout = DEFAULT_HEARTBEAT_TIMEOUT;
	r->install_snapshot_timeout = DEFAULT_INSTALL_SNAPSHOT_TIMEOUT;
	r->commit_index = 0;
	r->last_applied = 0;
	r->last_stored = 0;
	r->state = RAFT_UNAVAILABLE;
	r->leader_state.voter_contacts = 0;
	rv = raftInitCallbacks(r);
	if (rv != 0) {
		goto err_after_address_alloc;
	}
	r->transfer = NULL;
	r->snapshot.pending.term = 0;
	r->snapshot.threshold = DEFAULT_SNAPSHOT_THRESHOLD;
	r->snapshot.trailing = DEFAULT_SNAPSHOT_TRAILING;
	r->snapshot.put.data = NULL;
	r->close_cb = NULL;
	memset(r->errmsg, 0, sizeof r->errmsg);
	r->pre_vote = false;
	r->max_catch_up_rounds = DEFAULT_MAX_CATCH_UP_ROUNDS;
	r->max_catch_up_round_duration = DEFAULT_MAX_CATCH_UP_ROUND_DURATION;
	rv = r->io->init(r->io, r->id, r->address);
	if (rv != 0) {
		ErrMsgTransfer(r->io->errmsg, r->errmsg, "io");
		goto err_after_callbacks_alloc;
	}
	return 0;

err_after_callbacks_alloc:
	raftDestroyCallbacks(r);
err_after_address_alloc:
	RaftHeapFree(r->address);
err:
	assert(rv != 0);
	return rv;
}

static void ioCloseCb(struct raft_io *io)
{
	struct raft *r = io->data;
	tracef("io close cb");
	if (r->close_cb != NULL) {
		r->close_cb(r);
	}
}

void raft_close(struct raft *r, void (*cb)(struct raft *r))
{
	assert(r->close_cb == NULL);
	if (r->state != RAFT_UNAVAILABLE) {
		convertToUnavailable(r);
	}
	r->close_cb = cb;
	r->io->close(r->io, ioCloseCb);
}

void raft_register_state_cb(struct raft *r, raft_state_cb cb)
{
	struct raft_callbacks *cbs = raftGetCallbacks(r);
	assert(cbs != NULL);
	cbs->state_cb = cb;
}

void raft_register_initial_barrier_cb(struct raft *r, raft_initial_barrier_cb cb)
{
	struct raft_callbacks *cbs = raftGetCallbacks(r);
	assert(cbs != NULL);
	cbs->ib_cb = cb;
}

void raft_set_election_timeout(struct raft *r, const unsigned msecs)
{
	r->election_timeout = msecs;
}

void raft_set_heartbeat_timeout(struct raft *r, const unsigned msecs)
{
	r->heartbeat_timeout = msecs;
}

void raft_set_install_snapshot_timeout(struct raft *r, const unsigned msecs)
{
	r->install_snapshot_timeout = msecs;
}

void raft_set_snapshot_threshold(struct raft *r, unsigned n)
{
	r->snapshot.threshold = n;
}

void raft_set_snapshot_trailing(struct raft *r, unsigned n)
{
	r->snapshot.trailing = n;
}

void raft_set_max_catch_up_rounds(struct raft *r, unsigned n)
{
	r->max_catch_up_rounds = n;
}

void raft_set_max_catch_up_round_duration(struct raft *r, unsigned msecs)
{
	r->max_catch_up_round_duration = msecs;
}

void raft_set_pre_vote(struct raft *r, bool enabled)
{
	r->pre_vote = enabled;
}

const char *raft_errmsg(struct raft *r)
{
	return r->errmsg;
}

int raft_voter_contacts(struct raft *r)
{
	int ret;
	if (r->state == RAFT_LEADER) {
		ret = (int)r->leader_state.voter_contacts;
	} else {
		ret = -1;
	}
	return ret;
}

int raft_bootstrap(struct raft *r, const struct raft_configuration *conf)
{
	int rv;

	if (r->state != RAFT_UNAVAILABLE) {
		return RAFT_BUSY;
	}

	rv = r->io->bootstrap(r->io, conf);
	if (rv != 0) {
		return rv;
	}

	return 0;
}

int raft_recover(struct raft *r, const struct raft_configuration *conf)
{
	int rv;

	if (r->state != RAFT_UNAVAILABLE) {
		return RAFT_BUSY;
	}

	rv = r->io->recover(r->io, conf);
	if (rv != 0) {
		return rv;
	}

	return 0;
}

const char *raft_strerror(int errnum)
{
	return errCodeToString(errnum);
}

void raft_configuration_init(struct raft_configuration *c)
{
	configurationInit(c);
}

void raft_configuration_close(struct raft_configuration *c)
{
	configurationClose(c);
}

int raft_configuration_add(struct raft_configuration *c,
			   const raft_id id,
			   const char *address,
			   const int role)
{
	return configurationAdd(c, id, address, role);
}

int raft_configuration_encode(const struct raft_configuration *c,
			      struct raft_buffer *buf)
{
	return configurationEncode(c, buf);
}

unsigned long long raft_digest(const char *text, unsigned long long n)
{
	struct byteSha1 sha1;
	uint8_t value[20];
	uint64_t n64 = byteFlip64((uint64_t)n);
	uint64_t digest;

	byteSha1Init(&sha1);
	byteSha1Update(&sha1, (const uint8_t *)text, (uint32_t)strlen(text));
	byteSha1Update(&sha1, (const uint8_t *)&n64, (uint32_t)(sizeof n64));
	byteSha1Digest(&sha1, value);

	memcpy(&digest, value + (sizeof value - sizeof digest), sizeof digest);

	return byteFlip64(digest);
}

static int ioFsmVersionCheck(struct raft *r,
			     struct raft_io *io,
			     struct raft_fsm *fsm)
{
	if (io->version == 0) {
		ErrMsgPrintf(r->errmsg, "io->version must be set");
		return -1;
	}

	if (fsm->version == 0) {
		ErrMsgPrintf(r->errmsg, "fsm->version must be set");
		return -1;
	}

	if ((fsm->version > 2 && fsm->snapshot_async != NULL) &&
	    ((io->version < 2) || (io->async_work == NULL))) {
		ErrMsgPrintf(r->errmsg,
			     "async snapshot requires io->version > 1 and "
			     "async_work method.");
		return -1;
	}

	return 0;
}

void raft_fini(struct raft *r)
{
	raftDestroyCallbacks(r);
	raft_free(r->address);
	logClose(r->log);
	raft_configuration_close(&r->configuration);
	raft_configuration_close(&r->configuration_last_snapshot);
}
