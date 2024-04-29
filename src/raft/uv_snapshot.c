#include <stdlib.h>
#include <string.h>

#include "array.h"
#include "assert.h"
#include "byte.h"
#include "compress.h"
#include "configuration.h"
#include "heap.h"
#include "uv.h"
#include "uv_encoding.h"
#include "uv_os.h"

/* Arbitrary maximum configuration size. Should be practically be enough */
#define UV__META_MAX_CONFIGURATION_SIZE 1024 * 1024

/* Returns true if the filename is a valid snapshot file or snapshot meta
 * filename depending on the `meta` switch. If the parse is successful, the
 * arguments will contain the parsed values. */
static bool uvSnapshotParseFilename(const char *filename,
				    bool meta,
				    raft_term *term,
				    raft_index *index,
				    raft_time *timestamp)
{
	/* Check if it's a well-formed snapshot filename */
	int consumed = 0;
	int matched;
	size_t filename_len = strlen(filename);
	assert(filename_len < UV__FILENAME_LEN);
	if (meta) {
		matched = sscanf(filename, UV__SNAPSHOT_META_TEMPLATE "%n",
				 term, index, timestamp, &consumed);
	} else {
		matched = sscanf(filename, UV__SNAPSHOT_TEMPLATE "%n", term,
				 index, timestamp, &consumed);
	}
	if (matched != 3 || consumed != (int)filename_len) {
		return false;
	}

	return true;
}

/* Check if the given filename matches the pattern of a snapshot metadata
 * filename (snapshot-xxx-yyy-zzz.meta), and fill the given info structure if
 * so.
 *
 * Return true if the filename matched, false otherwise. */
static bool uvSnapshotInfoMatch(const char *filename,
				struct uvSnapshotInfo *info)
{
	if (!uvSnapshotParseFilename(filename, true, &info->term, &info->index,
				     &info->timestamp)) {
		return false;
	}
	/* Allow room for '\0' terminator */
	size_t n = sizeof(info->filename) - 1;
	strncpy(info->filename, filename, n);
	info->filename[n] = '\0';
	return true;
}

void uvSnapshotFilenameOf(struct uvSnapshotInfo *info, char *filename)
{
	size_t len = strlen(info->filename) - strlen(".meta");
	assert(len < UV__FILENAME_LEN);
	strcpy(filename, info->filename);
	filename[len] = 0;
}

int UvSnapshotInfoAppendIfMatch(struct uv *uv,
				const char *filename,
				struct uvSnapshotInfo *infos[],
				size_t *n_infos,
				bool *appended)
{
	struct uvSnapshotInfo info;
	bool matched;
	char snapshot_filename[UV__FILENAME_LEN];
	bool exists;
	bool is_empty;
	char errmsg[RAFT_ERRMSG_BUF_SIZE];
	int rv;

	/* Check if it's a snapshot metadata filename */
	matched = uvSnapshotInfoMatch(filename, &info);
	if (!matched) {
		*appended = false;
		return 0;
	}

	/* Check if there's actually a valid snapshot file for this snapshot
	 * metadata. If there's none or it's empty, it means that we aborted
	 * before finishing the snapshot, or that another thread is still busy
	 * writing the snapshot. */
	uvSnapshotFilenameOf(&info, snapshot_filename);
	rv = UvFsFileExists(uv->dir, snapshot_filename, &exists, errmsg);
	if (rv != 0) {
		tracef("stat %s: %s", snapshot_filename, errmsg);
		rv = RAFT_IOERR;
		return rv;
	}
	if (!exists) {
		*appended = false;
		return 0;
	}

	/* TODO This check is strictly not needed, snapshot files are created by
	 * renaming fully written and synced tmp-files. Leaving it here, just to
	 * be extra-safe. Can probably be removed once more data integrity
	 * checks are performed at startup. */
	rv = UvFsFileIsEmpty(uv->dir, snapshot_filename, &is_empty, errmsg);
	if (rv != 0) {
		tracef("is_empty %s: %s", snapshot_filename, errmsg);
		rv = RAFT_IOERR;
		return rv;
	}
	if (is_empty) {
		*appended = false;
		return 0;
	}

	ARRAY__APPEND(struct uvSnapshotInfo, info, infos, n_infos, rv);
	if (rv == -1) {
		return RAFT_NOMEM;
	}
	*appended = true;

	return 0;
}

static int uvSnapshotIsOrphanInternal(const char *dir,
				      const char *filename,
				      bool meta,
				      bool *orphan)
{
	int rv;
	*orphan = false;

	raft_term term;
	raft_index index;
	raft_time timestamp;
	if (!uvSnapshotParseFilename(filename, meta, &term, &index,
				     &timestamp)) {
		return 0;
	}

	/* filename is a well-formed snapshot filename, check if the sibling
	 * file exists. */
	char sibling_filename[UV__FILENAME_LEN];
	if (meta) {
		rv = snprintf(sibling_filename, UV__FILENAME_LEN,
			      UV__SNAPSHOT_TEMPLATE, term, index, timestamp);
	} else {
		rv = snprintf(sibling_filename, UV__FILENAME_LEN,
			      UV__SNAPSHOT_META_TEMPLATE, term, index,
			      timestamp);
	}

	if (rv >= UV__FILENAME_LEN) {
		/* Output truncated */
		return -1;
	}

	bool sibling_exists = false;
	char ignored[RAFT_ERRMSG_BUF_SIZE];
	rv = UvFsFileExists(dir, sibling_filename, &sibling_exists, ignored);
	if (rv != 0) {
		return rv;
	}

	*orphan = !sibling_exists;
	return 0;
}

int UvSnapshotIsOrphan(const char *dir, const char *filename, bool *orphan)
{
	return uvSnapshotIsOrphanInternal(dir, filename, false, orphan);
}

int UvSnapshotMetaIsOrphan(const char *dir, const char *filename, bool *orphan)
{
	return uvSnapshotIsOrphanInternal(dir, filename, true, orphan);
}

/* Compare two snapshots to decide which one is more recent. */
static int uvSnapshotCompare(const void *p1, const void *p2)
{
	struct uvSnapshotInfo *s1 = (struct uvSnapshotInfo *)p1;
	struct uvSnapshotInfo *s2 = (struct uvSnapshotInfo *)p2;

	/* If terms are different, the snapshot with the highest term is the
	 * most recent. */
	if (s1->term != s2->term) {
		return s1->term < s2->term ? -1 : 1;
	}

	/* If the term are identical and the index differ, the snapshot with the
	 * highest index is the most recent */
	if (s1->index != s2->index) {
		return s1->index < s2->index ? -1 : 1;
	}

	/* If term and index are identical, compare the timestamp. */
	return s1->timestamp < s2->timestamp ? -1 : 1;
}

/* Sort the given snapshots. */
void UvSnapshotSort(struct uvSnapshotInfo *infos, size_t n_infos)
{
	qsort(infos, n_infos, sizeof *infos, uvSnapshotCompare);
}

/* Parse the metadata file of a snapshot and populate the metadata portion of
 * the given snapshot object accordingly. */
static int uvSnapshotLoadMeta(struct uv *uv,
			      struct uvSnapshotInfo *info,
			      struct raft_snapshot *snapshot,
			      char *errmsg)
{
	uint64_t header[1 + /* Format version */
			1 + /* CRC checksum */
			1 + /* Configuration index */
			1 /* Configuration length */];
	struct raft_buffer buf;
	uint64_t format;
	uint32_t crc1;
	uint32_t crc2;
	uv_file fd;
	int rv;

	snapshot->term = info->term;
	snapshot->index = info->index;

	rv = UvFsOpenFileForReading(uv->dir, info->filename, &fd, errmsg);
	if (rv != 0) {
		tracef("open %s: %s", info->filename, errmsg);
		rv = RAFT_IOERR;
		goto err;
	}
	buf.base = header;
	buf.len = sizeof header;
	rv = UvFsReadInto(fd, &buf, errmsg);
	if (rv != 0) {
		tracef("read %s: %s", info->filename, errmsg);
		rv = RAFT_IOERR;
		goto err_after_open;
	}

	format = byteFlip64(header[0]);
	if (format != UV__DISK_FORMAT) {
		tracef("load %s: unsupported format %ju", info->filename,
		       format);
		rv = RAFT_MALFORMED;
		goto err_after_open;
	}

	crc1 = (uint32_t)byteFlip64(header[1]);

	snapshot->configuration_index = byteFlip64(header[2]);
	buf.len = (size_t)byteFlip64(header[3]);
	if (buf.len > UV__META_MAX_CONFIGURATION_SIZE) {
		tracef("load %s: configuration data too big (%zd)",
		       info->filename, buf.len);
		rv = RAFT_CORRUPT;
		goto err_after_open;
	}
	if (buf.len == 0) {
		tracef("load %s: no configuration data", info->filename);
		rv = RAFT_CORRUPT;
		goto err_after_open;
	}
	buf.base = RaftHeapMalloc(buf.len);
	if (buf.base == NULL) {
		rv = RAFT_NOMEM;
		goto err_after_open;
	}

	rv = UvFsReadInto(fd, &buf, errmsg);
	if (rv != 0) {
		tracef("read %s: %s", info->filename, errmsg);
		rv = RAFT_IOERR;
		goto err_after_buf_malloc;
	}

	crc2 = byteCrc32(header + 2, sizeof header - sizeof(uint64_t) * 2, 0);
	crc2 = byteCrc32(buf.base, buf.len, crc2);

	if (crc1 != crc2) {
		ErrMsgPrintf(errmsg, "read %s: checksum mismatch",
			     info->filename);
		rv = RAFT_CORRUPT;
		goto err_after_buf_malloc;
	}

	rv = configurationDecode(&buf, &snapshot->configuration);
	if (rv != 0) {
		goto err_after_buf_malloc;
	}

	RaftHeapFree(buf.base);
	UvOsClose(fd);

	return 0;

err_after_buf_malloc:
	RaftHeapFree(buf.base);

err_after_open:
	close(fd);

err:
	assert(rv != 0);
	return rv;
}

/* Load the snapshot data file and populate the data portion of the given
 * snapshot object accordingly. */
static int uvSnapshotLoadData(struct uv *uv,
			      struct uvSnapshotInfo *info,
			      struct raft_snapshot *snapshot,
			      char *errmsg)
{
	char filename[UV__FILENAME_LEN];
	struct raft_buffer buf;
	int rv;

	uvSnapshotFilenameOf(info, filename);

	rv = UvFsReadFile(uv->dir, filename, &buf, errmsg);
	if (rv != 0) {
		tracef("stat %s: %s", filename, errmsg);
		goto err;
	}

	if (IsCompressed(buf.base, buf.len)) {
		struct raft_buffer decompressed = {0};
		tracef("snapshot decompress start");
		rv = Decompress(buf, &decompressed, errmsg);
		tracef("snapshot decompress end %d", rv);
		if (rv != 0) {
			tracef("decompress failed rv:%d", rv);
			goto err_after_read_file;
		}
		RaftHeapFree(buf.base);
		buf = decompressed;
	}

	snapshot->bufs = RaftHeapMalloc(sizeof *snapshot->bufs);
	snapshot->n_bufs = 1;
	if (snapshot->bufs == NULL) {
		rv = RAFT_NOMEM;
		goto err_after_read_file;
	}

	snapshot->bufs[0] = buf;
	return 0;

err_after_read_file:
	RaftHeapFree(buf.base);
err:
	assert(rv != 0);
	return rv;
}

int UvSnapshotLoad(struct uv *uv,
		   struct uvSnapshotInfo *meta,
		   struct raft_snapshot *snapshot,
		   char *errmsg)
{
	int rv;
	rv = uvSnapshotLoadMeta(uv, meta, snapshot, errmsg);
	if (rv != 0) {
		return rv;
	}
	rv = uvSnapshotLoadData(uv, meta, snapshot, errmsg);
	if (rv != 0) {
		return rv;
	}
	return 0;
}

struct uvSnapshotPut
{
	struct uv *uv;
	size_t trailing;
	struct raft_io_snapshot_put *req;
	const struct raft_snapshot *snapshot;
	struct
	{
		unsigned long long timestamp;
		uint64_t header[4]; /* Format, CRC, configuration index/len */
		struct raft_buffer bufs[2]; /* Preamble and configuration */
	} meta;
	char errmsg[RAFT_ERRMSG_BUF_SIZE];
	int status;
	struct UvBarrierReq barrier;
};

struct uvSnapshotGet
{
	struct uv *uv;
	struct raft_io_snapshot_get *req;
	struct raft_snapshot *snapshot;
	struct uv_work_s work;
	char errmsg[RAFT_ERRMSG_BUF_SIZE];
	int status;
	queue queue;
};

static int uvSnapshotKeepLastTwo(struct uv *uv,
				 struct uvSnapshotInfo *snapshots,
				 size_t n)
{
	size_t i;
	char errmsg[RAFT_ERRMSG_BUF_SIZE];
	int rv;

	/* Leave at least two snapshots, for safety. */
	if (n <= 2) {
		return 0;
	}

	for (i = 0; i < n - 2; i++) {
		struct uvSnapshotInfo *snapshot = &snapshots[i];
		char filename[UV__FILENAME_LEN];
		rv = UvFsRemoveFile(uv->dir, snapshot->filename, errmsg);
		if (rv != 0) {
			tracef("unlink %s: %s", snapshot->filename, errmsg);
			return RAFT_IOERR;
		}
		uvSnapshotFilenameOf(snapshot, filename);
		rv = UvFsRemoveFile(uv->dir, filename, errmsg);
		if (rv != 0) {
			tracef("unlink %s: %s", filename, errmsg);
			return RAFT_IOERR;
		}
	}

	return 0;
}

/* Remove all segments and snapshots that are not needed anymore, because their
   past the trailing amount. */
static int uvRemoveOldSegmentsAndSnapshots(struct uv *uv,
					   raft_index last_index,
					   size_t trailing,
					   char *errmsg)
{
	struct uvSnapshotInfo *snapshots;
	struct uvSegmentInfo *segments;
	size_t n_snapshots;
	size_t n_segments;
	int rv = 0;

	rv = UvList(uv, &snapshots, &n_snapshots, &segments, &n_segments,
		    errmsg);
	if (rv != 0) {
		goto out;
	}
	rv = uvSnapshotKeepLastTwo(uv, snapshots, n_snapshots);
	if (rv != 0) {
		goto out;
	}
	if (segments != NULL) {
		rv = uvSegmentKeepTrailing(uv, segments, n_segments, last_index,
					   trailing, errmsg);
		if (rv != 0) {
			goto out;
		}
	}
	rv = UvFsSyncDir(uv->dir, errmsg);

out:
	if (snapshots != NULL) {
		RaftHeapFree(snapshots);
	}
	if (segments != NULL) {
		RaftHeapFree(segments);
	}
	return rv;
}

static int makeFileCompressed(const char *dir,
			      const char *filename,
			      struct raft_buffer *bufs,
			      unsigned n_bufs,
			      char *errmsg)
{
	int rv;

	struct raft_buffer compressed = {0};
	rv = Compress(bufs, n_bufs, &compressed, errmsg);
	if (rv != 0) {
		ErrMsgWrapf(errmsg, "compress %s", filename);
		return RAFT_IOERR;
	}

	rv = UvFsMakeFile(dir, filename, &compressed, 1, errmsg);
	raft_free(compressed.base);
	return rv;
}

static void uvSnapshotPutWorkCb(uv_work_t *work)
{
	struct uvSnapshotPut *put = work->data;
	struct uv *uv = put->uv;
	char metadata[UV__FILENAME_LEN];
	char snapshot[UV__FILENAME_LEN];
	char errmsg[RAFT_ERRMSG_BUF_SIZE];
	int rv;

	sprintf(metadata, UV__SNAPSHOT_META_TEMPLATE, put->snapshot->term,
		put->snapshot->index, put->meta.timestamp);

	rv = UvFsMakeFile(uv->dir, metadata, put->meta.bufs, 2, put->errmsg);
	if (rv != 0) {
		tracef("snapshot.meta creation failed %d", rv);
		ErrMsgWrapf(put->errmsg, "write %s", metadata);
		put->status = RAFT_IOERR;
		return;
	}

	sprintf(snapshot, UV__SNAPSHOT_TEMPLATE, put->snapshot->term,
		put->snapshot->index, put->meta.timestamp);

	tracef("snapshot write start");
	if (uv->snapshot_compression) {
		rv = makeFileCompressed(uv->dir, snapshot, put->snapshot->bufs,
					put->snapshot->n_bufs, put->errmsg);
	} else {
		rv = UvFsMakeFile(uv->dir, snapshot, put->snapshot->bufs,
				  put->snapshot->n_bufs, put->errmsg);
	}
	tracef("snapshot write end %d", rv);

	if (rv != 0) {
		tracef("snapshot creation failed %d", rv);
		ErrMsgWrapf(put->errmsg, "write %s", snapshot);
		UvFsRemoveFile(uv->dir, metadata, errmsg);
		UvFsRemoveFile(uv->dir, snapshot, errmsg);
		put->status = RAFT_IOERR;
		return;
	}

	rv = UvFsSyncDir(uv->dir, put->errmsg);
	if (rv != 0) {
		put->status = RAFT_IOERR;
		return;
	}

	rv = uvRemoveOldSegmentsAndSnapshots(uv, put->snapshot->index,
					     put->trailing, put->errmsg);
	if (rv != 0) {
		put->status = rv;
		return;
	}

	put->status = 0;

	return;
}

/* Finish the put request, releasing all associated memory and invoking its
 * callback. */
static void uvSnapshotPutFinish(struct uvSnapshotPut *put)
{
	struct raft_io_snapshot_put *req = put->req;
	int status = put->status;
	struct uv *uv = put->uv;
	assert(uv->snapshot_put_work.data == NULL);
	RaftHeapFree(put->meta.bufs[1].base);
	RaftHeapFree(put);
	req->cb(req, status);
}

static void uvSnapshotPutAfterWorkCb(uv_work_t *work, int status)
{
	struct uvSnapshotPut *put = work->data;
	struct uv *uv = put->uv;
	assert(status == 0);
	uv->snapshot_put_work.data = NULL;
	uvSnapshotPutFinish(put);
	UvUnblock(uv);
}

/* Start processing the given put request. */
static void uvSnapshotPutStart(struct uvSnapshotPut *put)
{
	struct uv *uv = put->uv;
	int rv;

	/* If this is an install request, the barrier callback must have fired.
	 */
	if (put->trailing == 0) {
		assert(put->barrier.data == NULL);
	}

	uv->snapshot_put_work.data = put;
	rv = uv_queue_work(uv->loop, &uv->snapshot_put_work,
			   uvSnapshotPutWorkCb, uvSnapshotPutAfterWorkCb);
	if (rv != 0) {
		tracef("store snapshot %lld: %s", put->snapshot->index,
		       uv_strerror(rv));
		uv->errored = true;
	}
}

static void uvSnapshotPutBarrierCb(struct UvBarrierReq *barrier)
{
	/* Ensure that we don't invoke this callback more than once. */
	barrier->cb = NULL;
	struct uvSnapshotPut *put = barrier->data;
	if (put == NULL) {
		return;
	}

	struct uv *uv = put->uv;
	put->barrier.data = NULL;
	/* If we're closing, abort the request. */
	if (uv->closing) {
		put->status = RAFT_CANCELED;
		uvSnapshotPutFinish(put);
		uvMaybeFireCloseCb(uv);
		return;
	}
	uvSnapshotPutStart(put);
}

int UvSnapshotPut(struct raft_io *io,
		  unsigned trailing,
		  struct raft_io_snapshot_put *req,
		  const struct raft_snapshot *snapshot,
		  raft_io_snapshot_put_cb cb)
{
	struct uv *uv;
	struct uvSnapshotPut *put;
	void *cursor;
	unsigned crc;
	int rv;
	raft_index next_index;

	uv = io->impl;
	if (uv->closing) {
		return RAFT_CANCELED;
	}

	assert(uv->snapshot_put_work.data == NULL);

	tracef("put snapshot at %lld, keeping %d", snapshot->index, trailing);

	put = RaftHeapMalloc(sizeof *put);
	if (put == NULL) {
		rv = RAFT_NOMEM;
		goto err;
	}
	put->uv = uv;
	put->req = req;
	put->snapshot = snapshot;
	put->meta.timestamp = uv_now(uv->loop);
	put->trailing = trailing;
	put->barrier.data = put;
	put->barrier.blocking = trailing == 0;
	put->barrier.cb = uvSnapshotPutBarrierCb;

	req->cb = cb;

	/* Prepare the buffers for the metadata file. */
	put->meta.bufs[0].base = put->meta.header;
	put->meta.bufs[0].len = sizeof put->meta.header;

	rv = configurationEncode(&snapshot->configuration, &put->meta.bufs[1]);
	if (rv != 0) {
		goto err_after_req_alloc;
	}

	cursor = put->meta.header;
	bytePut64(&cursor, UV__DISK_FORMAT);
	bytePut64(&cursor, 0);
	bytePut64(&cursor, snapshot->configuration_index);
	bytePut64(&cursor, put->meta.bufs[1].len);

	crc = byteCrc32(&put->meta.header[2], sizeof(uint64_t) * 2, 0);
	crc = byteCrc32(put->meta.bufs[1].base, put->meta.bufs[1].len, crc);

	cursor = &put->meta.header[1];
	bytePut64(&cursor, crc);

	/* - If the trailing parameter is set to 0, it means that we're
	 * restoring a snapshot. Submit a barrier request setting the next
	 * append index to the snapshot's last index + 1.
	 * - When we are only writing a snapshot during normal operation, we
	 * close all current open segments. New writes can continue on newly
	 * opened segments that will only contain entries that are newer than
	 * the snapshot, and we don't change append_next_index. */
	next_index =
	    (trailing == 0) ? (snapshot->index + 1) : uv->append_next_index;
	rv = UvBarrier(uv, next_index, &put->barrier);
	if (rv != 0) {
		goto err_after_configuration_encode;
	}

	return 0;

err_after_configuration_encode:
	RaftHeapFree(put->meta.bufs[1].base);
err_after_req_alloc:
	RaftHeapFree(put);
err:
	assert(rv != 0);
	return rv;
}

static void uvSnapshotGetWorkCb(uv_work_t *work)
{
	struct uvSnapshotGet *get = work->data;
	struct uv *uv = get->uv;
	struct uvSnapshotInfo *snapshots;
	size_t n_snapshots;
	struct uvSegmentInfo *segments;
	size_t n_segments;
	int rv;
	get->status = 0;
	rv = UvList(uv, &snapshots, &n_snapshots, &segments, &n_segments,
		    get->errmsg);
	if (rv != 0) {
		get->status = rv;
		goto out;
	}
	if (snapshots != NULL) {
		rv = UvSnapshotLoad(uv, &snapshots[n_snapshots - 1],
				    get->snapshot, get->errmsg);
		if (rv != 0) {
			get->status = rv;
		}
		RaftHeapFree(snapshots);
	}
	if (segments != NULL) {
		RaftHeapFree(segments);
	}
out:
	return;
}

static void uvSnapshotGetAfterWorkCb(uv_work_t *work, int status)
{
	struct uvSnapshotGet *get = work->data;
	struct raft_io_snapshot_get *req = get->req;
	struct raft_snapshot *snapshot = get->snapshot;
	int req_status = get->status;
	struct uv *uv = get->uv;
	assert(status == 0);
	queue_remove(&get->queue);
	RaftHeapFree(get);
	req->cb(req, snapshot, req_status);
	uvMaybeFireCloseCb(uv);
}

int UvSnapshotGet(struct raft_io *io,
		  struct raft_io_snapshot_get *req,
		  raft_io_snapshot_get_cb cb)
{
	struct uv *uv;
	struct uvSnapshotGet *get;
	int rv;

	uv = io->impl;
	assert(!uv->closing);

	get = RaftHeapMalloc(sizeof *get);
	if (get == NULL) {
		rv = RAFT_NOMEM;
		goto err;
	}
	get->uv = uv;
	get->req = req;
	req->cb = cb;

	get->snapshot = RaftHeapMalloc(sizeof *get->snapshot);
	if (get->snapshot == NULL) {
		rv = RAFT_NOMEM;
		goto err_after_req_alloc;
	}
	get->work.data = get;

	queue_insert_tail(&uv->snapshot_get_reqs, &get->queue);
	rv = uv_queue_work(uv->loop, &get->work, uvSnapshotGetWorkCb,
			   uvSnapshotGetAfterWorkCb);
	if (rv != 0) {
		queue_remove(&get->queue);
		tracef("get last snapshot: %s", uv_strerror(rv));
		rv = RAFT_IOERR;
		goto err_after_snapshot_alloc;
	}

	return 0;

err_after_snapshot_alloc:
	RaftHeapFree(get->snapshot);
err_after_req_alloc:
	RaftHeapFree(get);
err:
	assert(rv != 0);
	return rv;
}

