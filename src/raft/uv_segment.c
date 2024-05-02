#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "array.h"
#include "assert.h"
#include "byte.h"
#include "configuration.h"
#include "entry.h"
#include "heap.h"
#include "uv.h"
#include "uv_encoding.h"

/* Check if the given filename matches the one of a closed segment (xxx-yyy), or
 * of an open segment (open-xxx), and fill the given info structure if so.
 *
 * Return true if the filename matched, false otherwise. */
static bool uvSegmentInfoMatch(const char *filename, struct uvSegmentInfo *info)
{
	int consumed;
	int matched;
	size_t n;
	size_t filename_len = strnlen(filename, UV__FILENAME_LEN + 1);

	assert(filename_len < UV__FILENAME_LEN);

	matched = sscanf(filename, UV__CLOSED_TEMPLATE "%n", &info->first_index,
			 &info->end_index, &consumed);
	if (matched == 2 && consumed == (int)filename_len) {
		info->is_open = false;
		goto match;
	}

	matched =
	    sscanf(filename, UV__OPEN_TEMPLATE "%n", &info->counter, &consumed);
	if (matched == 1 && consumed == (int)filename_len) {
		info->is_open = true;
		goto match;
	}

	return false;

match:
	n = sizeof(info->filename) - 1;
	strncpy(info->filename, filename, n);
	info->filename[n] = '\0';
	return true;
}

int uvSegmentInfoAppendIfMatch(const char *filename,
			       struct uvSegmentInfo *infos[],
			       size_t *n_infos,
			       bool *appended)
{
	struct uvSegmentInfo info;
	bool matched;
	int rv;

	/* Check if it's a closed or open filename */
	matched = uvSegmentInfoMatch(filename, &info);

	/* If this is neither a closed or an open segment, return. */
	if (!matched) {
		*appended = false;
		return 0;
	}

	ARRAY__APPEND(struct uvSegmentInfo, info, infos, n_infos, rv);
	if (rv == -1) {
		return RAFT_NOMEM;
	}

	*appended = true;

	return 0;
}

/* Compare two segments to decide which one is more recent. */
static int uvSegmentInfoCompare(const void *p1, const void *p2)
{
	struct uvSegmentInfo *s1 = (struct uvSegmentInfo *)p1;
	struct uvSegmentInfo *s2 = (struct uvSegmentInfo *)p2;

	/* Closed segments are less recent than open segments. */
	if (s1->is_open && !s2->is_open) {
		return 1;
	}
	if (!s1->is_open && s2->is_open) {
		return -1;
	}

	/* If the segments are open, compare the counter. */
	if (s1->is_open) {
		assert(s2->is_open);
		assert(s1->counter != s2->counter);
		return s1->counter < s2->counter ? -1 : 1;
	}

	/* If the segments are closed, compare the first index. The index ranges
	 * must be disjoint. */
	if (s2->first_index > s1->end_index) {
		return -1;
	}

	return 1;
}

void uvSegmentSort(struct uvSegmentInfo *infos, size_t n_infos)
{
	qsort(infos, n_infos, sizeof *infos, uvSegmentInfoCompare);
}

int uvSegmentKeepTrailing(struct uv *uv,
			  struct uvSegmentInfo *segments,
			  size_t n,
			  raft_index last_index,
			  size_t trailing,
			  char *errmsg)
{
	raft_index retain_index;
	size_t i;
	int rv;

	assert(last_index > 0);
	assert(n > 0);

	if (last_index <= trailing) {
		return 0;
	}

	/* Index of the oldest entry we want to retain. */
	retain_index = last_index - trailing + 1;

	for (i = 0; i < n; i++) {
		struct uvSegmentInfo *segment = &segments[i];
		if (segment->is_open) {
			break;
		}
		if (trailing == 0 || segment->end_index < retain_index) {
			rv = UvFsRemoveFile(uv->dir, segment->filename, errmsg);
			if (rv != 0) {
				ErrMsgWrapf(errmsg, "delete closed segment %s",
					    segment->filename);
				return rv;
			}
		} else {
			break;
		}
	}

	return 0;
}

/* Read a segment file and return its format version. */
static int uvReadSegmentFile(struct uv *uv,
			     const char *filename,
			     struct raft_buffer *buf,
			     uint64_t *format)
{
	char errmsg[RAFT_ERRMSG_BUF_SIZE];
	int rv;
	rv = UvFsReadFile(uv->dir, filename, buf, errmsg);
	if (rv != 0) {
		ErrMsgTransfer(errmsg, uv->io->errmsg, "read file");
		return RAFT_IOERR;
	}
	if (buf->len < 8) {
		ErrMsgPrintf(uv->io->errmsg, "file has only %zu bytes",
			     buf->len);
		RaftHeapFree(buf->base);
		return RAFT_IOERR;
	}
	*format = byteFlip64(*(uint64_t *)buf->base);
	return 0;
}

/* Consume the content buffer, returning a pointer to the current position and
 * advancing the offset of n bytes. Return an error if not enough bytes are
 * available. */
static int uvConsumeContent(const struct raft_buffer *content,
			    size_t *offset,
			    size_t n,
			    void **data,
			    char *errmsg)
{
	if (*offset + n > content->len) {
		size_t remaining = content->len - *offset;
		ErrMsgPrintf(errmsg, "short read: %zu bytes instead of %zu",
			     remaining, n);
		return RAFT_IOERR;
	}
	if (data != NULL) {
		*data = &((uint8_t *)content->base)[*offset];
	}
	*offset += n;
	return 0;
}

/* Load a single batch of entries from a segment.
 *
 * Set @last to #true if the loaded batch is the last one. */
static int uvLoadEntriesBatch(struct uv *uv,
			      const struct raft_buffer *content,
			      struct raft_entry **entries,
			      unsigned *n_entries,
			      size_t *offset, /* Offset of last batch */
			      bool *last)
{
	void *checksums;           /* CRC32 checksums */
	void *batch;               /* Entries batch */
	unsigned long n;           /* Number of entries in the batch */
	unsigned max_n;            /* Maximum number of entries we expect */
	unsigned i;                /* Iterate through the entries */
	struct raft_buffer header; /* Batch header */
	struct raft_buffer data;   /* Batch data */
	uint32_t crc1;             /* Target checksum */
	uint32_t crc2;             /* Actual checksum */
	char errmsg[RAFT_ERRMSG_BUF_SIZE];
	size_t start;
	int rv;

	/* Save the current offset, to provide more information when logging. */
	start = *offset;

	/* Read the checksums. */
	rv = uvConsumeContent(content, offset, sizeof(uint32_t) * 2, &checksums,
			      errmsg);
	if (rv != 0) {
		ErrMsgTransfer(errmsg, uv->io->errmsg, "read preamble");
		return RAFT_IOERR;
	}

	/* Read the first 8 bytes of the batch, which contains the number of
	 * entries in the batch. */
	rv =
	    uvConsumeContent(content, offset, sizeof(uint64_t), &batch, errmsg);
	if (rv != 0) {
		ErrMsgTransfer(errmsg, uv->io->errmsg, "read preamble");
		return RAFT_IOERR;
	}

	n = (size_t)byteFlip64(*(uint64_t *)batch);
	if (n == 0) {
		ErrMsgPrintf(uv->io->errmsg,
			     "entries count in preamble is zero");
		rv = RAFT_CORRUPT;
		goto err;
	}

	/* Very optimistic upper bound of the number of entries we should
	 * expect. This is mainly a protection against allocating too much
	 * memory. Each entry will consume at least 4 words (for term, type,
	 * size and payload). */
	max_n = UV__MAX_SEGMENT_SIZE / (sizeof(uint64_t) * 4);

	if (n > max_n) {
		ErrMsgPrintf(uv->io->errmsg,
			     "entries count %lu in preamble is too high", n);
		rv = RAFT_CORRUPT;
		goto err;
	}

	/* Consume the batch header, excluding the first 8 bytes containing the
	 * number of entries, which we have already read. */
	header.len = uvSizeofBatchHeader(n);
	header.base = batch;

	rv = uvConsumeContent(content, offset,
			      uvSizeofBatchHeader(n) - sizeof(uint64_t), NULL,
			      errmsg);
	if (rv != 0) {
		ErrMsgTransfer(errmsg, uv->io->errmsg, "read header");
		rv = RAFT_IOERR;
		goto err;
	}

	/* Check batch header integrity. */
	crc1 = byteFlip32(((uint32_t *)checksums)[0]);
	crc2 = byteCrc32(header.base, header.len, 0);
	if (crc1 != crc2) {
		ErrMsgPrintf(uv->io->errmsg, "header checksum mismatch");
		rv = RAFT_CORRUPT;
		goto err;
	}

	/* Decode the batch header, allocating the entries array. */
	rv = uvDecodeBatchHeader(header.base, entries, n_entries);
	if (rv != 0) {
		goto err;
	}

	/* Calculate the total size of the batch data */
	data.len = 0;
	for (i = 0; i < n; i++) {
		data.len += (*entries)[i].buf.len;
	}
	data.base = (uint8_t *)content->base + *offset;

	/* Consume the batch data */
	rv = uvConsumeContent(content, offset, data.len, NULL, errmsg);
	if (rv != 0) {
		ErrMsgTransfer(errmsg, uv->io->errmsg, "read data");
		rv = RAFT_IOERR;
		goto err_after_header_decode;
	}

	/* Check batch data integrity. */
	crc1 = byteFlip32(((uint32_t *)checksums)[1]);
	crc2 = byteCrc32(data.base, data.len, 0);
	if (crc1 != crc2) {
		ErrMsgPrintf(uv->io->errmsg, "data checksum mismatch");
		rv = RAFT_CORRUPT;
		goto err_after_header_decode;
	}

	uvDecodeEntriesBatch(content->base, *offset - data.len, *entries,
			     *n_entries);

	*last = *offset == content->len;

	return 0;

err_after_header_decode:
	RaftHeapFree(*entries);
err:
	*entries = NULL;
	*n_entries = 0;
	assert(rv != 0);
	*offset = start;
	return rv;
}

/* Append to @entries2 all entries in @entries1. */
static int extendEntries(const struct raft_entry *entries1,
			 const size_t n_entries1,
			 struct raft_entry **entries2,
			 size_t *n_entries2)
{
	struct raft_entry *entries; /* To re-allocate the given entries */
	size_t i;

	entries = raft_realloc(*entries2,
			       (*n_entries2 + n_entries1) * sizeof *entries);
	if (entries == NULL) {
		return RAFT_NOMEM;
	}

	for (i = 0; i < n_entries1; i++) {
		entries[*n_entries2 + i] = entries1[i];
	}

	*entries2 = entries;
	*n_entries2 += n_entries1;

	return 0;
}

int uvSegmentLoadClosed(struct uv *uv,
			struct uvSegmentInfo *info,
			struct raft_entry *entries[],
			size_t *n)
{
	bool empty;                     /* Whether the file is empty */
	uint64_t format;                /* Format version */
	bool last;                      /* Whether the last batch was reached */
	struct raft_entry *tmp_entries; /* Entries in current batch */
	struct raft_buffer buf;         /* Segment file content */
	size_t offset;                  /* Content read cursor */
	unsigned tmp_n;                 /* Number of entries in current batch */
	unsigned expected_n; /* Number of entries that we expect to find */
	int i;
	char errmsg[RAFT_ERRMSG_BUF_SIZE];
	int rv;

	expected_n = (unsigned)(info->end_index - info->first_index + 1);

	/* If the segment is completely empty, just bail out. */
	rv = UvFsFileIsEmpty(uv->dir, info->filename, &empty, errmsg);
	if (rv != 0) {
		tracef("stat %s: %s", info->filename, errmsg);
		rv = RAFT_IOERR;
		goto err;
	}
	if (empty) {
		ErrMsgPrintf(uv->io->errmsg, "file is empty");
		rv = RAFT_CORRUPT;
		goto err;
	}

	/* Open the segment file. */
	rv = uvReadSegmentFile(uv, info->filename, &buf, &format);
	if (rv != 0) {
		goto err;
	}
	if (format != UV__DISK_FORMAT) {
		ErrMsgPrintf(uv->io->errmsg, "unexpected format version %ju",
			     format);
		rv = RAFT_CORRUPT;
		goto err_after_read;
	}

	/* Load all batches in the segment. */
	*entries = NULL;
	*n = 0;

	last = false;
	offset = sizeof format;
	for (i = 1; !last; i++) {
		rv = uvLoadEntriesBatch(uv, &buf, &tmp_entries, &tmp_n, &offset,
					&last);
		if (rv != 0) {
			ErrMsgWrapf(uv->io->errmsg,
				    "entries batch %u starting at byte %zu", i,
				    offset);
			/* Clean up the last allocation from extendEntries. */
			goto err_after_extend_entries;
		}
		rv = extendEntries(tmp_entries, tmp_n, entries, n);
		if (rv != 0) {
			goto err_after_batch_load;
		}
		raft_free(tmp_entries);
	}

	if (*n != expected_n) {
		ErrMsgPrintf(uv->io->errmsg, "found %zu entries (expected %u)",
			     *n, expected_n);
		rv = RAFT_CORRUPT;
		goto err_after_extend_entries;
	}

	assert(i > 1);  /* At least one batch was loaded. */
	assert(*n > 0); /* At least one entry was loaded. */

	return 0;

err_after_batch_load:
	raft_free(tmp_entries[0].batch);
	raft_free(tmp_entries);

err_after_extend_entries:
	if (*entries != NULL) {
		RaftHeapFree(*entries);
	}

err_after_read:
	RaftHeapFree(buf.base);

err:
	assert(rv != 0);

	return rv;
}

/* Check if the content of the segment file contains all zeros from the current
 * offset onward. */
static bool uvContentHasOnlyTrailingZeros(const struct raft_buffer *buf,
					  size_t offset)
{
	size_t i;

	for (i = offset; i < buf->len; i++) {
		if (((char *)buf->base)[i] != 0) {
			return false;
		}
	}

	return true;
}

/* Load all entries contained in an open segment. */
static int uvSegmentLoadOpen(struct uv *uv,
			     struct uvSegmentInfo *info,
			     struct raft_entry *entries[],
			     size_t *n,
			     raft_index *next_index)
{
	raft_index first_index;         /* Index of first entry in segment */
	bool all_zeros;                 /* Whether the file is zero'ed */
	bool empty;                     /* Whether the segment file is empty */
	bool remove = false;            /* Whether to remove this segment */
	bool last = false;              /* Whether the last batch was reached */
	uint64_t format;                /* Format version */
	size_t n_batches = 0;           /* Number of loaded batches */
	struct raft_entry *tmp_entries; /* Entries in current batch */
	struct raft_buffer buf = {0};   /* Segment file content */
	size_t offset;                  /* Content read cursor */
	unsigned tmp_n_entries;         /* Number of entries in current batch */
	int i;
	char errmsg[RAFT_ERRMSG_BUF_SIZE];
	int rv;

	first_index = *next_index;

	rv = UvFsFileIsEmpty(uv->dir, info->filename, &empty, errmsg);
	if (rv != 0) {
		tracef("check if %s is empty: %s", info->filename, errmsg);
		rv = RAFT_IOERR;
		goto err;
	}

	if (empty) {
		/* Empty segment, let's discard it. */
		tracef("remove empty open segment %s", info->filename);
		remove = true;
		goto done;
	}

	rv = uvReadSegmentFile(uv, info->filename, &buf, &format);
	if (rv != 0) {
		goto err;
	}

	/* Check that the format is the expected one, or perhaps 0, indicating
	 * that the segment was allocated but never written. */
	offset = sizeof format;
	if (format != UV__DISK_FORMAT) {
		if (format == 0) {
			all_zeros = uvContentHasOnlyTrailingZeros(&buf, offset);
			if (all_zeros) {
				/* This is equivalent to the empty case, let's
				 * remove the segment. */
				tracef("remove zeroed open segment %s",
				       info->filename);
				remove = true;
				RaftHeapFree(buf.base);
				buf.base = NULL;
				goto done;
			}
		}
		ErrMsgPrintf(uv->io->errmsg, "unexpected format version %ju",
			     format);
		rv = RAFT_CORRUPT;
		goto err_after_read;
	}

	/* Load all batches in the segment. */
	for (i = 1; !last; i++) {
		rv = uvLoadEntriesBatch(uv, &buf, &tmp_entries, &tmp_n_entries,
					&offset, &last);
		if (rv != 0) {
			/* If this isn't a decoding error, just bail out. */
			if (rv != RAFT_CORRUPT) {
				ErrMsgWrapf(
				    uv->io->errmsg,
				    "entries batch %u starting at byte %zu", i,
				    offset);
				goto err_after_read;
			}

			/* If this is a decoding error, and not an OS error,
			 * check if the rest of the file is filled with zeros.
			 * In that case we assume that the server shutdown
			 * uncleanly and we just truncate this incomplete data.
			 */
			all_zeros = uvContentHasOnlyTrailingZeros(&buf, offset);
			if (!all_zeros) {
				tracef("%s has non-zero trail", info->filename);
			}

			tracef(
			    "truncate open segment %s at %zu (batch %d), since "
			    "it has "
			    "corrupted "
			    "entries",
			    info->filename, offset, i);

			break;
		}

		rv = extendEntries(tmp_entries, tmp_n_entries, entries, n);
		if (rv != 0) {
			goto err_after_batch_load;
		}

		raft_free(tmp_entries);

		n_batches++;
		*next_index += tmp_n_entries;
	}

	if (n_batches == 0) {
		RaftHeapFree(buf.base);
		buf.base = NULL;
		remove = true;
	}

done:
	/* If the segment has no valid entries in it, we remove it. Otherwise we
	 * rename it and keep it. */
	if (remove) {
		rv = UvFsRemoveFile(uv->dir, info->filename, errmsg);
		if (rv != 0) {
			tracef("unlink %s: %s", info->filename, errmsg);
			rv = RAFT_IOERR;
			goto err_after_read;
		}
	} else {
		char filename[UV__SEGMENT_FILENAME_BUF_SIZE];
		raft_index end_index = *next_index - 1;

		/* At least one entry was loaded */
		assert(end_index >= first_index);
		int nb = snprintf(filename, sizeof(filename),
				  UV__CLOSED_TEMPLATE, first_index, end_index);
		if ((nb < 0) || ((size_t)nb >= sizeof(filename))) {
			tracef("snprintf failed: %d", nb);
			rv = RAFT_IOERR;
			goto err;
		}

		tracef("finalize %s into %s", info->filename, filename);

		rv = UvFsTruncateAndRenameFile(
		    uv->dir, (size_t)offset, info->filename, filename, errmsg);
		if (rv != 0) {
			tracef("finalize %s: %s", info->filename, errmsg);
			rv = RAFT_IOERR;
			goto err;
		}

		info->is_open = false;
		info->first_index = first_index;
		info->end_index = end_index;
		memset(info->filename, '\0', sizeof(info->filename));
		_Static_assert(sizeof(info->filename) >= sizeof(filename),
			       "Destination buffer too small");
		/* info->filename is zeroed out, info->filename is at least as
		 * large as filename and we checked that nb < sizeof(filename)
		 * -> we won't overflow and the result will be zero terminated.
		 */
		memcpy(info->filename, filename, (size_t)nb);
	}

	return 0;

err_after_batch_load:
	raft_free(tmp_entries[0].batch);
	raft_free(tmp_entries);

err_after_read:
	if (buf.base != NULL) {
		RaftHeapFree(buf.base);
	}

err:
	assert(rv != 0);

	return rv;
}

/* Ensure that the write buffer of the given segment is large enough to hold the
 * the given number of bytes size. */
static int uvEnsureSegmentBufferIsLargeEnough(struct uvSegmentBuffer *b,
					      size_t size)
{
	unsigned n = (unsigned)(size / b->block_size);
	void *base;
	size_t len;

	if (b->arena.len >= size) {
		assert(b->arena.base != NULL);
		return 0;
	}

	if (size % b->block_size != 0) {
		n++;
	}

	len = b->block_size * n;
	base = raft_aligned_alloc(b->block_size, len);
	if (base == NULL) {
		return RAFT_NOMEM;
	}
	memset(base, 0, len);

	/* If the current arena is initialized, we need to copy its content,
	 * since it might have data that we want to retain in the next write. */
	if (b->arena.base != NULL) {
		assert(b->arena.len >= b->block_size);
		memcpy(base, b->arena.base, b->arena.len);
		raft_aligned_free(b->block_size, b->arena.base);
	}

	b->arena.base = base;
	b->arena.len = len;

	return 0;
}

void uvSegmentBufferInit(struct uvSegmentBuffer *b, size_t block_size)
{
	b->block_size = block_size;
	b->arena.base = NULL;
	b->arena.len = 0;
	b->n = 0;
}

void uvSegmentBufferClose(struct uvSegmentBuffer *b)
{
	if (b->arena.base != NULL) {
		raft_aligned_free(b->block_size, b->arena.base);
	}
}

int uvSegmentBufferFormat(struct uvSegmentBuffer *b)
{
	int rv;
	void *cursor;
	size_t n;
	assert(b->n == 0);
	n = sizeof(uint64_t);
	rv = uvEnsureSegmentBufferIsLargeEnough(b, n);
	if (rv != 0) {
		return rv;
	}
	b->n = n;
	cursor = b->arena.base;
	bytePut64(&cursor, UV__DISK_FORMAT);
	return 0;
}

int uvSegmentBufferAppend(struct uvSegmentBuffer *b,
			  const struct raft_entry entries[],
			  unsigned n_entries)
{
	size_t size;   /* Total size of the batch */
	uint32_t crc1; /* Header checksum */
	uint32_t crc2; /* Data checksum */
	void *crc1_p;  /* Pointer to header checksum slot */
	void *crc2_p;  /* Pointer to data checksum slot */
	void *header;  /* Pointer to the header section */
	void *cursor;
	unsigned i;
	int rv;

	size = sizeof(uint32_t) * 2;            /* CRC checksums */
	size += uvSizeofBatchHeader(n_entries); /* Batch header */
	for (i = 0; i < n_entries; i++) {       /* Entries data */
		size += bytePad64(entries[i].buf.len);
	}

	rv = uvEnsureSegmentBufferIsLargeEnough(b, b->n + size);
	if (rv != 0) {
		return rv;
	}
	cursor = b->arena.base + b->n;

	/* Placeholder of the checksums */
	crc1_p = cursor;
	bytePut32(&cursor, 0);
	crc2_p = cursor;
	bytePut32(&cursor, 0);

	/* Batch header */
	header = cursor;
	uvEncodeBatchHeader(entries, n_entries, cursor);
	crc1 = byteCrc32(header, uvSizeofBatchHeader(n_entries), 0);
	cursor = (uint8_t *)cursor + uvSizeofBatchHeader(n_entries);

	/* Batch data */
	crc2 = 0;
	for (i = 0; i < n_entries; i++) {
		const struct raft_entry *entry = &entries[i];
		assert(entry->buf.len % sizeof(uint64_t) == 0);
		memcpy(cursor, entry->buf.base, entry->buf.len);
		crc2 = byteCrc32(cursor, entry->buf.len, crc2);
		cursor = (uint8_t *)cursor + entry->buf.len;
	}

	bytePut32(&crc1_p, crc1);
	bytePut32(&crc2_p, crc2);
	b->n += size;

	return 0;
}

void uvSegmentBufferFinalize(struct uvSegmentBuffer *b, uv_buf_t *out)
{
	unsigned n_blocks;
	unsigned tail;

	n_blocks = (unsigned)(b->n / b->block_size);
	if (b->n % b->block_size != 0) {
		n_blocks++;
	}

	/* Set the remainder of the last block to 0 */
	tail = (unsigned)(b->n % b->block_size);
	if (tail != 0) {
		memset(b->arena.base + b->n, 0, b->block_size - tail);
	}

	out->base = b->arena.base;
	out->len = n_blocks * b->block_size;
}

void uvSegmentBufferReset(struct uvSegmentBuffer *b, unsigned retain)
{
	assert(b->n > 0);
	assert(b->arena.base != NULL);

	if (retain == 0) {
		b->n = 0;
		memset(b->arena.base, 0, b->block_size);
		return;
	}

	memcpy(b->arena.base, b->arena.base + retain * b->block_size,
	       b->block_size);
	b->n = b->n % b->block_size;
}

/* When a corrupted segment is detected, the segment is renamed.
 * Upon a restart, raft will not detect the segment anymore and will try
 * to start without it.
 * */
#define CORRUPT_FILE_FMT "corrupt-%" PRId64 "-%s"
static void uvMoveCorruptSegment(struct uv *uv, struct uvSegmentInfo *info)
{
	char errmsg[RAFT_ERRMSG_BUF_SIZE] = {0};
	char new_filename[UV__FILENAME_LEN + 1] = {0};
	size_t sz = sizeof(new_filename);
	int rv;

	struct timespec ts = {0};
	/* Ignore errors */
	clock_gettime(CLOCK_REALTIME, &ts);
	int64_t ns = ts.tv_sec * 1000000000 + ts.tv_nsec;
	rv = snprintf(new_filename, sz, CORRUPT_FILE_FMT, ns, info->filename);
	if (rv < 0 || rv >= (int)sz) {
		tracef("snprintf %d", rv);
		return;
	}

	UvFsRenameFile(uv->dir, info->filename, new_filename, errmsg);
	if (rv != 0) {
		tracef("%s", errmsg);
		return;
	}
}

/*
 * On startup, raft will try to recover when a corrupt segment is detected.
 *
 * When a corrupt open segment is encountered, it, and all subsequent open
 * segments, are renamed. Not renaming newer, possible non-corrupt, open
 * segments could lead to loading inconsistent data.
 *
 * When a corrupt closed segment is encountered, it will be renamed when
 * it is the last closed segment, in that case all open-segments are renamed
 * too.
 */
static void uvRecoverFromCorruptSegment(struct uv *uv,
					size_t i_corrupt,
					struct uvSegmentInfo *infos,
					size_t n_infos)
{
	struct uvSegmentInfo *info = &infos[i_corrupt];
	if (info->is_open) {
		for (size_t i = i_corrupt; i < n_infos; ++i) {
			info = &infos[i];
			uvMoveCorruptSegment(uv, info);
		}
	} else {
		size_t i_next = i_corrupt + 1;
		/* last segment or last closed segment. */
		if (i_next == n_infos || infos[i_next].is_open) {
			for (size_t i = i_corrupt; i < n_infos; ++i) {
				info = &infos[i];
				uvMoveCorruptSegment(uv, info);
			}
		}
	}
}

int uvSegmentLoadAll(struct uv *uv,
		     const raft_index start_index,
		     struct uvSegmentInfo *infos,
		     size_t n_infos,
		     struct raft_entry **entries,
		     size_t *n_entries)
{
	raft_index next_index;          /* Next entry to load from disk */
	struct raft_entry *tmp_entries; /* Entries in current segment */
	size_t tmp_n; /* Number of entries in current segment */
	size_t i;
	int rv;

	assert(start_index >= 1);
	assert(n_infos > 0);

	*entries = NULL;
	*n_entries = 0;

	next_index = start_index;

	for (i = 0; i < n_infos; i++) {
		struct uvSegmentInfo *info = &infos[i];

		tracef("load segment %s", info->filename);

		if (info->is_open) {
			rv = uvSegmentLoadOpen(uv, info, entries, n_entries,
					       &next_index);
			ErrMsgWrapf(uv->io->errmsg, "load open segment %s",
				    info->filename);
			if (rv != 0) {
				if (rv == RAFT_CORRUPT && uv->auto_recovery) {
					uvRecoverFromCorruptSegment(
					    uv, i, infos, n_infos);
				}
				goto err;
			}
		} else {
			assert(info->first_index >= start_index);
			assert(info->first_index <= info->end_index);

			/* Check that the start index encoded in the name of the
			 * segment matches what we expect and there are no gaps
			 * in the sequence. */
			if (info->first_index != next_index) {
				ErrMsgPrintf(uv->io->errmsg,
					     "unexpected closed segment %s: "
					     "first index should "
					     "have been %llu",
					     info->filename, next_index);
				rv = RAFT_CORRUPT;
				goto err;
			}

			rv =
			    uvSegmentLoadClosed(uv, info, &tmp_entries, &tmp_n);
			if (rv != 0) {
				ErrMsgWrapf(uv->io->errmsg,
					    "load closed segment %s",
					    info->filename);
				if (rv == RAFT_CORRUPT && uv->auto_recovery) {
					uvRecoverFromCorruptSegment(
					    uv, i, infos, n_infos);
				}
				goto err;
			}

			assert(tmp_n > 0);
			rv = extendEntries(tmp_entries, tmp_n, entries,
					   n_entries);
			if (rv != 0) {
				/* TODO: release memory of entries in
				 * tmp_entries */
				goto err;
			}

			raft_free(tmp_entries);
			next_index += tmp_n;
		}
	}

	return 0;

err:
	assert(rv != 0);

	/* Free any batch that we might have allocated and the entries array as
	 * well. */
	if (*entries != NULL) {
		void *batch = NULL;

		for (i = 0; i < *n_entries; i++) {
			struct raft_entry *entry = &(*entries)[i];

			if (entry->batch != batch) {
				batch = entry->batch;
				raft_free(batch);
			}
		}

		raft_free(*entries);
		*entries = NULL;
		*n_entries = 0;
	}

	return rv;
}

/* Write a closed segment */
static int uvWriteClosedSegment(struct uv *uv,
				raft_index first_index,
				raft_index last_index,
				const struct raft_buffer *conf)
{
	char filename[UV__FILENAME_LEN];
	struct uvSegmentBuffer buf = {0};
	struct raft_buffer data;
	struct raft_entry entry = {0};
	size_t cap;
	char errmsg[RAFT_ERRMSG_BUF_SIZE];
	int rv;

	assert(first_index <= last_index);

	/* Render the path */
	sprintf(filename, UV__CLOSED_TEMPLATE, first_index, last_index);

	/* Make sure that the given encoded configuration fits in the first
	 * block */
	cap = uv->block_size -
	      (sizeof(uint64_t) /* Format version */ +
	       sizeof(uint64_t) /* Checksums */ + uvSizeofBatchHeader(1));
	if (conf->len > cap) {
		return RAFT_TOOBIG;
	}

	uvSegmentBufferInit(&buf, uv->block_size);

	rv = uvSegmentBufferFormat(&buf);
	if (rv != 0) {
		return rv;
	}

	entry.term = 1;
	entry.type = RAFT_CHANGE;
	entry.buf = *conf;

	rv = uvSegmentBufferAppend(&buf, &entry, 1);
	if (rv != 0) {
		uvSegmentBufferClose(&buf);
		return rv;
	}

	data.base = buf.arena.base;
	data.len = buf.n;
	rv = UvFsMakeFile(uv->dir, filename, &data, 1, errmsg);
	uvSegmentBufferClose(&buf);
	if (rv != 0) {
		tracef("write segment %s: %s", filename, errmsg);
		return RAFT_IOERR;
	}

	return 0;
}

int uvSegmentCreateFirstClosed(struct uv *uv,
			       const struct raft_configuration *configuration)
{
	return uvSegmentCreateClosedWithConfiguration(uv, 1, configuration);
}

int uvSegmentCreateClosedWithConfiguration(
    struct uv *uv,
    raft_index index,
    const struct raft_configuration *configuration)
{
	struct raft_buffer buf;
	char filename[UV__FILENAME_LEN];
	int rv;

	/* Render the path */
	sprintf(filename, UV__CLOSED_TEMPLATE, index, index);

	/* Encode the given configuration. */
	rv = configurationEncode(configuration, &buf);
	if (rv != 0) {
		goto err;
	}

	/* Write the file */
	rv = uvWriteClosedSegment(uv, index, index, &buf);
	if (rv != 0) {
		goto err_after_configuration_encode;
	}

	raft_free(buf.base);

	rv = UvFsSyncDir(uv->dir, uv->io->errmsg);
	if (rv != 0) {
		return RAFT_IOERR;
	}

	return 0;

err_after_configuration_encode:
	raft_free(buf.base);
err:
	assert(rv != 0);
	return rv;
}

int uvSegmentTruncate(struct uv *uv,
		      struct uvSegmentInfo *segment,
		      raft_index index)
{
	char filename[UV__FILENAME_LEN];
	struct raft_entry *entries;
	struct uvSegmentBuffer buf;
	struct raft_buffer data;
	size_t n;
	unsigned m;
	char errmsg[RAFT_ERRMSG_BUF_SIZE];
	int rv;

	assert(!segment->is_open);

	tracef("truncate %llu-%llu at %llu", segment->first_index,
	       segment->end_index, index);

	rv = uvSegmentLoadClosed(uv, segment, &entries, &n);
	if (rv != 0) {
		ErrMsgWrapf(uv->io->errmsg, "load closed segment %s",
			    segment->filename);
		goto out;
	}

	/* Discard all entries after the truncate index (included) */
	assert(index - segment->first_index < n);
	m = (unsigned)(index - segment->first_index);

	uvSegmentBufferInit(&buf, uv->block_size);

	rv = uvSegmentBufferFormat(&buf);
	if (rv != 0) {
		goto out_after_buffer_init;
	}

	rv = uvSegmentBufferAppend(&buf, entries, m);
	if (rv != 0) {
		goto out_after_buffer_init;
	}

	/* Render the path.
	 *
	 * TODO: we should use a temporary file name so in case of crash we
	 * don't consider this segment as corrupted.
	 */
	sprintf(filename, UV__CLOSED_TEMPLATE, segment->first_index, index - 1);

	data.base = buf.arena.base;
	data.len = buf.n;

	rv = UvFsMakeFile(uv->dir, filename, &data, 1, errmsg);
	if (rv != 0) {
		tracef("write %s: %s", filename, errmsg);
		rv = RAFT_IOERR;
		goto out_after_buffer_init;
	}

out_after_buffer_init:
	uvSegmentBufferClose(&buf);
	entryBatchesDestroy(entries, n);
out:
	return rv;
}

