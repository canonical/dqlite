#include "../raft.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/random.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include "../raft.h"
#include "../tracing.h"
#include "assert.h"
#include "byte.h"
#include "configuration.h"
#include "entry.h"
#include "heap.h"
#include "snapshot.h"
#include "uv.h"
#include "uv_encoding.h"
#include "uv_os.h"

/* Retry to connect to peer servers every second.
 *
 * TODO: implement an exponential backoff instead.  */
#define CONNECT_RETRY_DELAY 1000

/* Cleans up files that are no longer used by the system */
static int uvMaintenance(const char *dir, char *errmsg)
{
	struct uv_fs_s req;
	struct uv_dirent_s entry;
	int n;
	int i;
	int rv;
	int rv2;

	n = uv_fs_scandir(NULL, &req, dir, 0, NULL);
	if (n < 0) {
		ErrMsgPrintf(errmsg, "scan data directory: %s", uv_strerror(n));
		return RAFT_IOERR;
	}

	rv = 0;
	for (i = 0; i < n; i++) {
		const char *filename;
		rv = uv_fs_scandir_next(&req, &entry);
		assert(rv == 0); /* Can't fail in libuv */

		filename = entry.name;
		/* Remove leftover tmp-files */
		if (strncmp(filename, TMP_FILE_PREFIX,
			    strlen(TMP_FILE_PREFIX)) == 0) {
			UvFsRemoveFile(dir, filename,
				       errmsg); /* Ignore errors */
			continue;
		}

		/* Remove orphaned snapshot files */
		bool orphan = false;
		if ((UvSnapshotIsOrphan(dir, filename, &orphan) == 0) &&
		    orphan) {
			UvFsRemoveFile(dir, filename,
				       errmsg); /* Ignore errors */
			continue;
		}

		/* Remove orphaned snapshot metadata files */
		if ((UvSnapshotMetaIsOrphan(dir, filename, &orphan) == 0) &&
		    orphan) {
			UvFsRemoveFile(dir, filename,
				       errmsg); /* Ignore errors */
		}
	}

	rv2 = uv_fs_scandir_next(&req, &entry);
	assert(rv2 == UV_EOF);
	return rv;
}

/* Implementation of raft_io->config. */
static int uvInit(struct raft_io *io, raft_id id, const char *address)
{
	struct uv *uv;
	size_t direct_io;
	struct uvMetadata metadata;
	int rv;
	uv = io->impl;
	uv->id = id;

	rv = UvFsCheckDir(uv->dir, io->errmsg);
	if (rv != 0) {
		return rv;
	}

	/* Probe file system capabilities */
	rv = UvFsProbeCapabilities(uv->dir, &direct_io, &uv->async_io,
				   &uv->fallocate, io->errmsg);
	if (rv != 0) {
		return rv;
	}
	uv->direct_io = direct_io != 0;
	uv->block_size = direct_io != 0 ? direct_io : 4096;

	rv = uvMaintenance(uv->dir, io->errmsg);
	if (rv != 0) {
		return rv;
	}

	rv = uvMetadataLoad(uv->dir, &metadata, io->errmsg);
	if (rv != 0) {
		return rv;
	}
	uv->metadata = metadata;

	rv = uv->transport->init(uv->transport, id, address);
	if (rv != 0) {
		ErrMsgTransfer(uv->transport->errmsg, io->errmsg, "transport");
		return rv;
	}
	uv->transport->data = uv;

	rv = uv_timer_init(uv->loop, &uv->timer);
	assert(rv == 0); /* This should never fail */
	uv->timer.data = uv;

	return 0;
}

/* Periodic timer callback */
static void uvTickTimerCb(uv_timer_t *timer)
{
	struct uv *uv;
	uv = timer->data;
	if (uv->tick_cb != NULL) {
		uv->tick_cb(uv->io);
	}
}

/* Implementation of raft_io->start. */
static int uvStart(struct raft_io *io,
		   unsigned msecs,
		   raft_io_tick_cb tick_cb,
		   raft_io_recv_cb recv_cb)
{
	struct uv *uv;
	int rv;
	uv = io->impl;
	uv->state = UV__ACTIVE;
	uv->tick_cb = tick_cb;
	uv->recv_cb = recv_cb;
	rv = UvRecvStart(uv);
	if (rv != 0) {
		return rv;
	}
	rv = uv_timer_start(&uv->timer, uvTickTimerCb, msecs, msecs);
	assert(rv == 0);
	return 0;
}

void uvMaybeFireCloseCb(struct uv *uv)
{
	tracef("uv maybe fire close cb");
	if (!uv->closing) {
		return;
	}

	if (uv->transport->data != NULL) {
		return;
	}
	if (uv->timer.data != NULL) {
		return;
	}
	if (!queue_empty(&uv->append_segments)) {
		return;
	}
	if (!queue_empty(&uv->finalize_reqs)) {
		return;
	}
	if (uv->finalize_work.data != NULL) {
		return;
	}
	if (uv->prepare_inflight != NULL) {
		return;
	}
	if (uv->barrier != NULL) {
		return;
	}
	if (uv->snapshot_put_work.data != NULL) {
		return;
	}
	if (!queue_empty(&uv->snapshot_get_reqs)) {
		return;
	}
	if (!queue_empty(&uv->async_work_reqs)) {
		return;
	}
	if (!queue_empty(&uv->aborting)) {
		return;
	}

	assert(uv->truncate_work.data == NULL);

	if (uv->close_cb != NULL) {
		uv->close_cb(uv->io);
	}
}

static void uvTickTimerCloseCb(uv_handle_t *handle)
{
	struct uv *uv = handle->data;
	assert(uv->closing);
	uv->timer.data = NULL;
	uvMaybeFireCloseCb(uv);
}

static void uvTransportCloseCb(struct raft_uv_transport *transport)
{
	struct uv *uv = transport->data;
	assert(uv->closing);
	uv->transport->data = NULL;
	uvMaybeFireCloseCb(uv);
}

/* Implementation of raft_io->close. */
static void uvClose(struct raft_io *io, raft_io_close_cb cb)
{
	struct uv *uv;
	uv = io->impl;
	assert(uv != NULL);
	assert(!uv->closing);
	uv->close_cb = cb;
	uv->closing = true;
	UvSendClose(uv);
	UvRecvClose(uv);
	uvAppendClose(uv);
	if (uv->transport->data != NULL) {
		uv->transport->close(uv->transport, uvTransportCloseCb);
	}
	if (uv->timer.data != NULL) {
		uv_close((uv_handle_t *)&uv->timer, uvTickTimerCloseCb);
	}
	uvMaybeFireCloseCb(uv);
}

/* Filter the given segment list to find the most recent contiguous chunk of
 * closed segments that overlaps with the given snapshot last index. */
static int uvFilterSegments(struct uv *uv,
			    raft_index last_index,
			    const char *snapshot_filename,
			    struct uvSegmentInfo **segments,
			    size_t *n)
{
	struct uvSegmentInfo *segment;
	size_t i; /* First valid closed segment. */
	size_t j; /* Last valid closed segment. */

	/* If there are not segments at all, or only open segments, there's
	 * nothing to do. */
	if (*segments == NULL || (*segments)[0].is_open) {
		return 0;
	}

	/* Find the index of the most recent closed segment. */
	for (j = 0; j < *n; j++) {
		segment = &(*segments)[j];
		if (segment->is_open) {
			break;
		}
	}
	assert(j > 0);
	j--;

	segment = &(*segments)[j];
	tracef("most recent closed segment is %s", segment->filename);

	/* If the end index of the last closed segment is lower than the last
	 * snapshot index, there might be no entry that we can keep. We return
	 * an empty segment list, unless there is at least one open segment, in
	 * that case we keep everything hoping that they contain all the entries
	 * since the last closed segment (TODO: we should encode the starting
	 * entry in the open segment). */
	if (segment->end_index < last_index) {
		if (!(*segments)[*n - 1].is_open) {
			tracef(
			    "discarding all closed segments, since most recent "
			    "is behind "
			    "last snapshot");
			raft_free(*segments);
			*segments = NULL;
			*n = 0;
			return 0;
		}
		tracef(
		    "most recent closed segment %s is behind last snapshot, "
		    "yet there are open segments",
		    segment->filename);
	}

	/* Now scan the segments backwards, searching for the longest list of
	 * contiguous closed segments. */
	if (j >= 1) {
		for (i = j; i > 0; i--) {
			struct uvSegmentInfo *newer;
			struct uvSegmentInfo *older;
			newer = &(*segments)[i];
			older = &(*segments)[i - 1];
			if (older->end_index != newer->first_index - 1) {
				tracef("discarding non contiguous segment %s",
				       older->filename);
				break;
			}
		}
	} else {
		i = j;
	}

	/* Make sure that the first index of the first valid closed segment is
	 * not greater than the snapshot's last index plus one (so there are no
	 * missing entries). */
	segment = &(*segments)[i];
	if (segment->first_index > last_index + 1) {
		ErrMsgPrintf(uv->io->errmsg,
			     "closed segment %s is past last snapshot %s",
			     segment->filename, snapshot_filename);
		return RAFT_CORRUPT;
	}

	if (i != 0) {
		size_t new_n = *n - i;
		struct uvSegmentInfo *new_segments;
		new_segments = raft_malloc(new_n * sizeof *new_segments);
		if (new_segments == NULL) {
			return RAFT_NOMEM;
		}
		memcpy(new_segments, &(*segments)[i],
		       new_n * sizeof *new_segments);
		raft_free(*segments);
		*segments = new_segments;
		*n = new_n;
	}

	return 0;
}

/* Load the last snapshot (if any) and all entries contained in all segment
 * files of the data directory. This function can be called recursively, `depth`
 * is there to ensure we don't get stuck in a recursive loop. */
static int uvLoadSnapshotAndEntries(struct uv *uv,
				    struct raft_snapshot **snapshot,
				    raft_index *start_index,
				    struct raft_entry *entries[],
				    size_t *n,
				    int depth)
{
	struct uvSnapshotInfo *snapshots;
	struct uvSegmentInfo *segments;
	size_t n_snapshots;
	size_t n_segments;
	int rv;

	*snapshot = NULL;
	*start_index = 1;
	*entries = NULL;
	*n = 0;

	/* List available snapshots and segments. */
	rv = UvList(uv, &snapshots, &n_snapshots, &segments, &n_segments,
		    uv->io->errmsg);
	if (rv != 0) {
		goto err;
	}

	/* Load the most recent snapshot, if any. */
	if (snapshots != NULL) {
		char snapshot_filename[UV__FILENAME_LEN];
		*snapshot = RaftHeapMalloc(sizeof **snapshot);
		if (*snapshot == NULL) {
			rv = RAFT_NOMEM;
			goto err;
		}
		rv = UvSnapshotLoad(uv, &snapshots[n_snapshots - 1], *snapshot,
				    uv->io->errmsg);
		if (rv != 0) {
			RaftHeapFree(*snapshot);
			*snapshot = NULL;
			goto err;
		}
		uvSnapshotFilenameOf(&snapshots[n_snapshots - 1],
				     snapshot_filename);
		tracef("most recent snapshot at %lld", (*snapshot)->index);
		RaftHeapFree(snapshots);
		snapshots = NULL;

		/* Update the start index. If there are closed segments on disk
		 * let's make sure that the first index of the first closed
		 * segment is not greater than the snapshot's last index plus
		 * one (so there are no missing entries), and update the start
		 * index accordingly. */
		rv = uvFilterSegments(uv, (*snapshot)->index, snapshot_filename,
				      &segments, &n_segments);
		if (rv != 0) {
			goto err;
		}
		if (segments != NULL) {
			if (segments[0].is_open) {
				*start_index = (*snapshot)->index + 1;
			} else {
				*start_index = segments[0].first_index;
			}
		} else {
			*start_index = (*snapshot)->index + 1;
		}
	}

	/* Read data from segments, closing any open segments. */
	if (segments != NULL) {
		raft_index last_index;
		rv = uvSegmentLoadAll(uv, *start_index, segments, n_segments,
				      entries, n);
		if (rv != 0) {
			goto err;
		}

		/* Check if all entries that we loaded are actually behind the
		 * last snapshot. This can happen if the last closed segment was
		 * behind the last snapshot and there were open segments, but
		 * the entries in the open segments turned out to be behind the
		 * snapshot as well.  */
		last_index = *start_index + *n - 1;
		if (*snapshot != NULL && last_index < (*snapshot)->index) {
			ErrMsgPrintf(uv->io->errmsg,
				     "last entry on disk has index %llu, which "
				     "is behind "
				     "last snapshot's index %llu",
				     last_index, (*snapshot)->index);
			rv = RAFT_CORRUPT;
			goto err;
		}

		raft_free(segments);
		segments = NULL;
	}

	return 0;

err:
	assert(rv != 0);
	if (*snapshot != NULL) {
		snapshotDestroy(*snapshot);
		*snapshot = NULL;
	}
	if (snapshots != NULL) {
		raft_free(snapshots);
	}
	if (segments != NULL) {
		raft_free(segments);
	}
	if (*entries != NULL) {
		entryBatchesDestroy(*entries, *n);
		*entries = NULL;
		*n = 0;
	}
	/* Try to recover exactly once when corruption is detected, the first
	 * pass might have cleaned up corrupt data. Most of the arguments are
	 * already reset after the `err` label, except for `start_index`. */
	if (rv == RAFT_CORRUPT && uv->auto_recovery && depth == 0) {
		*start_index = 1;
		return uvLoadSnapshotAndEntries(uv, snapshot, start_index,
						entries, n, depth + 1);
	}
	return rv;
}

/* Implementation of raft_io->load. */
static int uvLoad(struct raft_io *io,
		  raft_term *term,
		  raft_id *voted_for,
		  struct raft_snapshot **snapshot,
		  raft_index *start_index,
		  struct raft_entry **entries,
		  size_t *n_entries)
{
	struct uv *uv;
	int rv;
	uv = io->impl;

	*term = uv->metadata.term;
	*voted_for = uv->metadata.voted_for;
	*snapshot = NULL;

	rv = uvLoadSnapshotAndEntries(uv, snapshot, start_index, entries,
				      n_entries, 0);
	if (rv != 0) {
		return rv;
	}
	tracef("start index %lld, %zu entries", *start_index, *n_entries);
	if (*snapshot == NULL) {
		tracef("no snapshot");
	}

	/* Set the index of the next entry that will be appended. */
	uv->append_next_index = *start_index + *n_entries;

	return 0;
}

/* Implementation of raft_io->set_term. */
static int uvSetTerm(struct raft_io *io, const raft_term term)
{
	struct uv *uv;
	int rv;
	uv = io->impl;
	uv->metadata.version++;
	uv->metadata.term = term;
	uv->metadata.voted_for = 0;
	rv = uvMetadataStore(uv, &uv->metadata);
	if (rv != 0) {
		return rv;
	}
	return 0;
}

/* Implementation of raft_io->set_term. */
static int uvSetVote(struct raft_io *io, const raft_id server_id)
{
	struct uv *uv;
	int rv;
	uv = io->impl;
	uv->metadata.version++;
	uv->metadata.voted_for = server_id;
	rv = uvMetadataStore(uv, &uv->metadata);
	if (rv != 0) {
		return rv;
	}
	return 0;
}

/* Implementation of raft_io->bootstrap. */
static int uvBootstrap(struct raft_io *io,
		       const struct raft_configuration *configuration)
{
	struct uv *uv;
	int rv;
	uv = io->impl;

	/* We shouldn't have written anything else yet. */
	if (uv->metadata.term != 0) {
		ErrMsgPrintf(io->errmsg, "metadata contains term %lld",
			     uv->metadata.term);
		return RAFT_CANTBOOTSTRAP;
	}

	/* Write the term */
	rv = uvSetTerm(io, 1);
	if (rv != 0) {
		return rv;
	}

	/* Create the first closed segment file, containing just one entry. */
	rv = uvSegmentCreateFirstClosed(uv, configuration);
	if (rv != 0) {
		return rv;
	}

	return 0;
}

/* Implementation of raft_io->recover. */
static int uvRecover(struct raft_io *io, const struct raft_configuration *conf)
{
	struct uv *uv = io->impl;
	struct raft_snapshot *snapshot;
	raft_index start_index;
	raft_index next_index;
	struct raft_entry *entries;
	size_t n_entries;
	int rv;

	/* Load the current state. This also closes any leftover open segment.
	 */
	rv = uvLoadSnapshotAndEntries(uv, &snapshot, &start_index, &entries,
				      &n_entries, 0);
	if (rv != 0) {
		return rv;
	}

	/* We don't care about the actual data, just index of the last entry. */
	if (snapshot != NULL) {
		snapshotDestroy(snapshot);
	}
	if (entries != NULL) {
		entryBatchesDestroy(entries, n_entries);
	}

	assert(start_index > 0);
	next_index = start_index + n_entries;

	rv = uvSegmentCreateClosedWithConfiguration(uv, next_index, conf);
	if (rv != 0) {
		return rv;
	}

	return 0;
}

/* Implementation of raft_io->time. */
static raft_time uvTime(struct raft_io *io)
{
	struct uv *uv;
	uv = io->impl;
	return uv_now(uv->loop);
}

/* Implementation of raft_io->random. */
static int uvRandom(struct raft_io *io, int min, int max)
{
	(void)io;
	return min + (abs(rand()) % (max - min));
}

static void uvSeedRand(struct uv *uv)
{
	ssize_t sz = -1;
	unsigned seed = 0; /* fed to srand() */

	sz = getrandom(&seed, sizeof seed, GRND_NONBLOCK);
	if (sz == -1 || sz < ((ssize_t)sizeof seed)) {
		/* Fall back to an inferior random seed when `getrandom` would
		 * have blocked or when not enough randomness was returned. */
		seed ^= (unsigned)uv->id;
		seed ^= (unsigned)uv_now(uv->loop);
		struct timeval time = {0};
		/* Ignore errors. */
		gettimeofday(&time, NULL);
		seed ^=
		    (unsigned)((time.tv_sec * 1000) + (time.tv_usec / 1000));
	}

	srand(seed);
}

int raft_uv_init(struct raft_io *io,
		 struct uv_loop_s *loop,
		 const char *dir,
		 struct raft_uv_transport *transport)
{
	struct uv *uv;
	void *data;
	int rv;

	assert(io != NULL);
	assert(loop != NULL);
	assert(dir != NULL);
	assert(transport != NULL);

	data = io->data;
	memset(io, 0, sizeof *io);
	io->data = data;

	if (transport->version == 0) {
		ErrMsgPrintf(io->errmsg, "transport->version must be set");
		return RAFT_INVALID;
	}

	/* Ensure that the given path doesn't exceed our static buffer limit. */
	if (!UV__DIR_HAS_VALID_LEN(dir)) {
		ErrMsgPrintf(io->errmsg, "directory path too long");
		return RAFT_NAMETOOLONG;
	}

	/* Allocate the raft_io_uv object */
	uv = raft_malloc(sizeof *uv);
	if (uv == NULL) {
		rv = RAFT_NOMEM;
		goto err;
	}
	memset(uv, 0, sizeof(struct uv));

	uv->io = io;
	uv->loop = loop;
	strncpy(uv->dir, dir, sizeof(uv->dir) - 1);
	uv->dir[sizeof(uv->dir) - 1] = '\0';
	uv->transport = transport;
	uv->transport->data = NULL;
	uv->tracer = NULL;
	uv->id = 0; /* Set by raft_io->config() */
	uv->state = UV__PRISTINE;
	uv->errored = false;
	uv->direct_io = false;
	uv->async_io = false;
	uv->fallocate = false;
#ifdef LZ4_ENABLED
	uv->snapshot_compression = true;
#else
	uv->snapshot_compression = false;
#endif
	uv->segment_size = UV__MAX_SEGMENT_SIZE;
	uv->block_size = 0;
	queue_init(&uv->clients);
	queue_init(&uv->servers);
	uv->connect_retry_delay = CONNECT_RETRY_DELAY;
	uv->prepare_inflight = NULL;
	queue_init(&uv->prepare_reqs);
	queue_init(&uv->prepare_pool);
	uv->prepare_next_counter = 1;
	uv->append_next_index = 1;
	queue_init(&uv->append_segments);
	queue_init(&uv->append_pending_reqs);
	queue_init(&uv->append_writing_reqs);
	uv->barrier = NULL;
	queue_init(&uv->finalize_reqs);
	uv->finalize_work.data = NULL;
	uv->truncate_work.data = NULL;
	queue_init(&uv->snapshot_get_reqs);
	queue_init(&uv->async_work_reqs);
	uv->snapshot_put_work.data = NULL;
	uv->timer.data = NULL;
	uv->tick_cb = NULL; /* Set by raft_io->start() */
	uv->recv_cb = NULL; /* Set by raft_io->start() */
	queue_init(&uv->aborting);
	uv->closing = false;
	uv->close_cb = NULL;
	uv->auto_recovery = true;

	uvSeedRand(uv);

	/* Set the raft_io implementation. */
	io->version = 2; /* future-proof'ing */
	io->impl = uv;
	io->init = uvInit;
	io->close = uvClose;
	io->start = uvStart;
	io->load = uvLoad;
	io->bootstrap = uvBootstrap;
	io->recover = uvRecover;
	io->set_term = uvSetTerm;
	io->set_vote = uvSetVote;
	io->append = UvAppend;
	io->truncate = UvTruncate;
	io->send = UvSend;
	io->snapshot_put = UvSnapshotPut;
	io->snapshot_get = UvSnapshotGet;
	io->async_work = UvAsyncWork;
	io->time = uvTime;
	io->random = uvRandom;

	return 0;

err:
	assert(rv != 0);
	if (rv == RAFT_NOMEM) {
		ErrMsgOom(io->errmsg);
	}
	return rv;
}

void raft_uv_close(struct raft_io *io)
{
	struct uv *uv;
	uv = io->impl;
	io->impl = NULL;
	raft_free(uv);
}

void raft_uv_set_segment_size(struct raft_io *io, size_t size)
{
	struct uv *uv;
	uv = io->impl;
	uv->segment_size = size;
}

void raft_uv_set_block_size(struct raft_io *io, size_t size)
{
	struct uv *uv;
	uv = io->impl;
	uv->block_size = size;
}

int raft_uv_set_snapshot_compression(struct raft_io *io, bool compressed)
{
	struct uv *uv;
	uv = io->impl;
#ifndef LZ4_AVAILABLE
	if (compressed) {
		return RAFT_INVALID;
	}
#endif
	uv->snapshot_compression = compressed;
	return 0;
}

void raft_uv_set_connect_retry_delay(struct raft_io *io, unsigned msecs)
{
	struct uv *uv;
	uv = io->impl;
	uv->connect_retry_delay = msecs;
}

void raft_uv_set_tracer(struct raft_io *io, struct raft_tracer *tracer)
{
	struct uv *uv;
	uv = io->impl;
	uv->tracer = tracer;
}

void raft_uv_set_auto_recovery(struct raft_io *io, bool flag)
{
	struct uv *uv;
	uv = io->impl;
	uv->auto_recovery = flag;
}

