/* Implementation of the @raft_io interface based on libuv. */

#ifndef UV_H_
#define UV_H_

#include "../raft.h"
#include "../tracing.h"
#include "err.h"
#include "../lib/queue.h"
#include "uv_fs.h"
#include "uv_os.h"

/* 8 Megabytes */
#define UV__MAX_SEGMENT_SIZE (8 * 1024 * 1024)

/* Template string for closed segment filenames: start index (inclusive), end
 * index (inclusive). */
#define UV__CLOSED_TEMPLATE "%016llu-%016llu"

/* Template string for open segment filenames: incrementing counter. */
#define UV__OPEN_TEMPLATE "open-%llu"

/* Enough to hold a segment filename (either open or closed) */
#define UV__SEGMENT_FILENAME_BUF_SIZE 34

/* Template string for snapshot filenames: snapshot term, snapshot index,
 * creation timestamp (milliseconds since epoch). */
#define UV__SNAPSHOT_TEMPLATE "snapshot-%llu-%llu-%llu"

#define UV__SNAPSHOT_META_SUFFIX ".meta"

/* Template string for snapshot metadata filenames: snapshot term,  snapshot
 * index, creation timestamp (milliseconds since epoch). */
#define UV__SNAPSHOT_META_TEMPLATE \
	UV__SNAPSHOT_TEMPLATE UV__SNAPSHOT_META_SUFFIX

/* State codes. */
enum {
	UV__PRISTINE, /* Metadata cache populated and I/O capabilities probed */
	UV__ACTIVE,
	UV__CLOSED
};

/* Open segment counter type */
typedef unsigned long long uvCounter;

/* Information persisted in a single metadata file. */
struct uvMetadata
{
	unsigned long long version; /* Monotonically increasing version */
	raft_term term;             /* Current term */
	raft_id voted_for;          /* Server ID of last vote, or 0 */
};

/* Hold state of a libuv-based raft_io implementation. */
struct uv
{
	struct raft_io *io;                  /* I/O object we're implementing */
	struct uv_loop_s *loop;              /* UV event loop */
	char dir[UV__DIR_LEN];               /* Data directory */
	struct raft_uv_transport *transport; /* Network transport */
	struct raft_tracer *tracer;          /* Debug tracing */
	raft_id id;                          /* Server ID */
	int state;                           /* Current state */
	bool snapshot_compression;           /* If compression is enabled */
	bool errored;                        /* If a disk I/O error was hit */
	bool direct_io;                 /* Whether direct I/O is supported */
	bool async_io;                  /* Whether async I/O is supported */
	bool fallocate;                 /* Whether fallocate is supported */
	size_t segment_size;            /* Initial size of open segments. */
	size_t block_size;              /* Block size of the data dir */
	queue clients;                  /* Outbound connections */
	queue servers;                  /* Inbound connections */
	unsigned connect_retry_delay;   /* Client connection retry delay */
	void *prepare_inflight;         /* Segment being prepared */
	queue prepare_reqs;             /* Pending prepare requests. */
	queue prepare_pool;             /* Prepared open segments */
	uvCounter prepare_next_counter; /* Counter of next open segment */
	raft_index append_next_index;   /* Index of next entry to append */
	queue append_segments;          /* Open segments in use. */
	queue append_pending_reqs;      /* Pending append requests. */
	queue append_writing_reqs;      /* Append requests in flight */
	struct UvBarrier *barrier;      /* Inflight barrier request */
	queue finalize_reqs;            /* Segments waiting to be closed */
	struct uv_work_s finalize_work; /* Resize and rename segments */
	struct uv_work_s truncate_work; /* Execute truncate log requests */
	queue snapshot_get_reqs;        /* Inflight get snapshot requests */
	queue async_work_reqs;          /* Inflight async work requests */
	struct uv_work_s snapshot_put_work; /* Execute snapshot put requests */
	struct uvMetadata metadata;         /* Cache of metadata on disk */
	struct uv_timer_s timer;            /* Timer for periodic ticks */
	raft_io_tick_cb tick_cb;            /* Invoked when the timer expires */
	raft_io_recv_cb recv_cb;            /* Invoked when upon RPC messages */
	queue aborting;            /* Cleanups upon errors or shutdown */
	bool closing;              /* True if we are closing */
	raft_io_close_cb close_cb; /* Invoked when finishing closing */
	bool auto_recovery;        /* Try to recover from corrupt segments */
};

/* Implementation of raft_io->truncate. */
int UvTruncate(struct raft_io *io,
	       struct raft_io_truncate *trunc,
	       raft_index index);

/* Load Raft metadata from disk, choosing the most recent version (either the
 * metadata1 or metadata2 file). */
int uvMetadataLoad(const char *dir, struct uvMetadata *metadata, char *errmsg);

/* Store the given metadata to disk, writing the appropriate metadata file
 * according to the metadata version (if the version is odd, write metadata1,
 * otherwise write metadata2). */
int uvMetadataStore(struct uv *uv, const struct uvMetadata *metadata);

/* Metadata about a segment file. */
struct uvSegmentInfo
{
	bool is_open; /* Whether the segment is open */
	union {
		struct
		{
			raft_index
			    first_index; /* First index in a closed segment */
			raft_index
			    end_index; /* Last index in a closed segment */
		};
		struct
		{
			unsigned long long counter; /* Open segment counter */
		};
	};
	char filename[UV__SEGMENT_FILENAME_BUF_SIZE]; /* Segment filename */
};

/* Append a new item to the given segment info list if the given filename
 * matches either the one of a closed segment (xxx-yyy) or the one of an open
 * segment (open-xxx). */
int uvSegmentInfoAppendIfMatch(const char *filename,
			       struct uvSegmentInfo *infos[],
			       size_t *n_infos,
			       bool *appended);

/* Sort the given list of segments by comparing their filenames. Closed segments
 * come before open segments. */
void uvSegmentSort(struct uvSegmentInfo *infos, size_t n_infos);

/* Keep only the closed segments whose entries are within the given trailing
 * amount past the given snapshot last index. If the given trailing amount is 0,
 * unconditionally delete all closed segments. */
int uvSegmentKeepTrailing(struct uv *uv,
			  struct uvSegmentInfo *segments,
			  size_t n,
			  raft_index last_index,
			  size_t trailing,
			  char *errmsg);

/* Load all entries contained in the given closed segment. */
int uvSegmentLoadClosed(struct uv *uv,
			struct uvSegmentInfo *segment,
			struct raft_entry *entries[],
			size_t *n);

/* Load raft entries from the given segments. The @start_index is the expected
 * index of the first entry of the first segment. */
int uvSegmentLoadAll(struct uv *uv,
		     const raft_index start_index,
		     struct uvSegmentInfo *segments,
		     size_t n_segments,
		     struct raft_entry **entries,
		     size_t *n_entries);

/* Return the number of blocks in a segments. */
#define uvSegmentBlocks(UV) (UV->segment_size / UV->block_size)

/* A dynamically allocated buffer holding data to be written into a segment
 * file.
 *
 * The memory is aligned at disk block boundary, to allow for direct I/O. */
struct uvSegmentBuffer
{
	size_t block_size; /* Disk block size for direct I/O */
	uv_buf_t arena;    /* Previously allocated memory that can be re-used */
	size_t n;          /* Write offset */
};

/* Initialize an empty buffer. */
void uvSegmentBufferInit(struct uvSegmentBuffer *b, size_t block_size);

/* Release all memory used by the buffer. */
void uvSegmentBufferClose(struct uvSegmentBuffer *b);

/* Encode the format version at the very beginning of the buffer. This function
 * must be called when the buffer is empty. */
int uvSegmentBufferFormat(struct uvSegmentBuffer *b);

/* Extend the segment's buffer by encoding the given entries.
 *
 * Previous data in the buffer will be retained, and data for these new entries
 * will be appended. */
int uvSegmentBufferAppend(struct uvSegmentBuffer *b,
			  const struct raft_entry entries[],
			  unsigned n_entries);

/* After all entries to write have been encoded, finalize the buffer by zeroing
 * the unused memory of the last block. The out parameter will point to the
 * memory to write. */
void uvSegmentBufferFinalize(struct uvSegmentBuffer *b, uv_buf_t *out);

/* Reset the buffer preparing it for the next segment write.
 *
 * If the retain parameter is greater than zero, then the data of the retain'th
 * block will be copied at the beginning of the buffer and the write offset will
 * be set accordingly. */
void uvSegmentBufferReset(struct uvSegmentBuffer *b, unsigned retain);

/* Write a closed segment, containing just one entry at the given index
 * for the given configuration. */
int uvSegmentCreateClosedWithConfiguration(
    struct uv *uv,
    raft_index index,
    const struct raft_configuration *configuration);

/* Write the first closed segment, containing just one entry for the given
 * configuration. */
int uvSegmentCreateFirstClosed(struct uv *uv,
			       const struct raft_configuration *configuration);

/* Truncate a segment that was already closed. */
int uvSegmentTruncate(struct uv *uv,
		      struct uvSegmentInfo *segment,
		      raft_index index);

/* Info about a persisted snapshot stored in snapshot metadata file. */
struct uvSnapshotInfo
{
	raft_term term;
	raft_index index;
	unsigned long long timestamp;
	char filename[UV__FILENAME_LEN];
};

/* Render the filename of the data file of a snapshot */
void uvSnapshotFilenameOf(struct uvSnapshotInfo *info, char *filename);

/* Upon success `orphan` will be true if filename is a snapshot file without a
 * sibling .meta file */
int UvSnapshotIsOrphan(const char *dir, const char *filename, bool *orphan);

/* Upon success `orphan` will be true if filename is a snapshot .meta file
 * without a sibling snapshot file */
int UvSnapshotMetaIsOrphan(const char *dir, const char *filename, bool *orphan);

/* Append a new item to the given snapshot info list if the given filename
 * matches the pattern of a snapshot metadata file (snapshot-xxx-yyy-zzz.meta)
 * and there is actually a matching non-empty snapshot file on disk. */
int UvSnapshotInfoAppendIfMatch(struct uv *uv,
				const char *filename,
				struct uvSnapshotInfo *infos[],
				size_t *n_infos,
				bool *appended);

/* Sort the given list of snapshots by comparing their filenames. Older
 * snapshots will come first. */
void UvSnapshotSort(struct uvSnapshotInfo *infos, size_t n_infos);

/* Load the snapshot associated with the given metadata. */
int UvSnapshotLoad(struct uv *uv,
		   struct uvSnapshotInfo *meta,
		   struct raft_snapshot *snapshot,
		   char *errmsg);

/* Implementation raft_io->snapshot_put (defined in uv_snapshot.c). */
int UvSnapshotPut(struct raft_io *io,
		  unsigned trailing,
		  struct raft_io_snapshot_put *req,
		  const struct raft_snapshot *snapshot,
		  raft_io_snapshot_put_cb cb);

/* Implementation of raft_io->snapshot_get (defined in uv_snapshot.c). */
int UvSnapshotGet(struct raft_io *io,
		  struct raft_io_snapshot_get *req,
		  raft_io_snapshot_get_cb cb);

/* Implementation of raft_io->async_work (defined in uv_work.c). */
int UvAsyncWork(struct raft_io *io,
		struct raft_io_async_work *req,
		raft_io_async_work_cb cb);

/* Return a list of all snapshots and segments found in the data directory. Both
 * snapshots and segments are ordered by filename (closed segments come before
 * open ones). */
int UvList(struct uv *uv,
	   struct uvSnapshotInfo *snapshots[],
	   size_t *n_snapshots,
	   struct uvSegmentInfo *segments[],
	   size_t *n_segments,
	   char *errmsg);

/* Request to obtain a newly prepared open segment. */
struct uvPrepare;
typedef void (*uvPrepareCb)(struct uvPrepare *req, int status);
struct uvPrepare
{
	void *data;                 /* User data */
	uv_file fd;                 /* Resulting segment file descriptor */
	unsigned long long counter; /* Resulting segment counter */
	uvPrepareCb cb;             /* Completion callback */
	queue queue;                /* Links in uv_io->prepare_reqs */
};

/* Get a prepared open segment ready for writing. If a prepared open segment is
 * already available in the pool, it will be returned immediately using the fd
 * and counter pointers and the request callback won't be invoked. Otherwise the
 * request will be queued and its callback invoked once a newly prepared segment
 * is available. */
int UvPrepare(struct uv *uv,
	      uv_file *fd,
	      uvCounter *counter,
	      struct uvPrepare *req,
	      uvPrepareCb cb);

/* Cancel all pending prepare requests and start removing all unused prepared
 * open segments. If a segment currently being created, wait for it to complete
 * and then remove it immediately. */
void UvPrepareClose(struct uv *uv);

/* Implementation of raft_io->append. All the raft_buffers of the raft_entry
 * structs in the entries array are required to have a len that is a multiple
 * of 8. */
int UvAppend(struct raft_io *io,
	     struct raft_io_append *req,
	     const struct raft_entry entries[],
	     unsigned n,
	     raft_io_append_cb cb);

/* Pause request object and callback. */
struct UvBarrierReq;

/* A barrier cb that plans to perform work on the threadpool MUST exit early
 * and cleanup resources when it detects uv->closing, this is to allow forced
 * closing on shutdown. */
typedef void (*UvBarrierCb)(struct UvBarrierReq *req);
struct UvBarrierReq
{
	bool blocking;  /* Whether this barrier should block future writes */
	void *data;     /* User data */
	UvBarrierCb cb; /* Completion callback */
	queue queue;    /* Queue of reqs triggered by a UvBarrier */
};

struct UvBarrier
{
	bool blocking; /* Whether this barrier should block future writes */
	queue reqs;    /* Queue of UvBarrierReq */
};

/* Submit a barrier request to interrupt the normal flow of append
 * operations.
 *
 * The following will happen:
 *
 * - Replace uv->append_next_index with the given next_index, so the next entry
 *   that will be appended will have the new index.
 *
 * - Execution of new writes for subsequent append requests will be blocked
 *   until UvUnblock is called when the barrier is blocking.
 *
 * - Wait for all currently pending and inflight append requests against all
 *   open segments to complete, and for those open segments to be finalized,
 *   then invoke the barrier callback.
 *
 * This API is used to implement truncate and snapshot install operations, which
 * need to wait until all pending writes have settled and modify the log state,
 * changing the next index. */
int UvBarrier(struct uv *uv, raft_index next_index, struct UvBarrierReq *req);

/* Trigger a callback for a barrier request in this @barrier. Returns true if a
 * callback was triggered, false if there are no more requests to trigger.
 * A barrier callback will call UvUnblock, which in turn will try to run the
 * next callback, if any, from a barrier request in this barrier. */
bool UvBarrierMaybeTrigger(struct UvBarrier *barrier);

/* Add a Barrier @req to an existing @barrier. */
void UvBarrierAddReq(struct UvBarrier *barrier, struct UvBarrierReq *req);

/* Returns @true if there are no more segments referencing uv->barrier */
bool UvBarrierReady(struct uv *uv);

/* Resume writing append requests after UvBarrier has been called. */
void UvUnblock(struct uv *uv);

/* Cancel all pending write requests and request the current segment to be
 * finalized. Must be invoked at closing time. */
void uvAppendClose(struct uv *uv);

/* Submit a request to finalize the open segment with the given counter.
 *
 * Requests are processed one at a time, to avoid ending up closing open segment
 * N + 1 before closing open segment N. */
int UvFinalize(struct uv *uv,
	       unsigned long long counter,
	       size_t used,
	       raft_index first_index,
	       raft_index last_index);

/* Implementation of raft_io->send. */
int UvSend(struct raft_io *io,
	   struct raft_io_send *req,
	   const struct raft_message *message,
	   raft_io_send_cb cb);

/* Stop all clients by closing the outbound stream handles and canceling all
 * pending send requests.  */
void UvSendClose(struct uv *uv);

/* Start receiving messages from new incoming connections. */
int UvRecvStart(struct uv *uv);

/* Stop all servers by closing the inbound stream handles and aborting all
 * requests being received.  */
void UvRecvClose(struct uv *uv);

void uvMaybeFireCloseCb(struct uv *uv);

#endif /* UV_H_ */
