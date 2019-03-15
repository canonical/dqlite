#include "raft.h"

/**
 * Copy all entries in @src into @dst.
 */
void raft_copy_entries(const struct raft_entry *src,
		       struct raft_entry **dst,
		       unsigned n)
{
	size_t size = 0;
	void *batch;
	void *cursor;
	unsigned i;

	if (n == 0) {
		*dst = NULL;
		return;
	}

	/* Calculate the total size of the entries content and allocate the
	 * batch. */
	for (i = 0; i < n; i++) {
		size += src[i].buf.len;
	}

	batch = raft_malloc(size);
	munit_assert_ptr_not_null(batch);

	/* Copy the entries. */
	*dst = raft_malloc(n * sizeof **dst);
	munit_assert_ptr_not_null(*dst);

	cursor = batch;

	for (i = 0; i < n; i++) {
		(*dst)[i] = src[i];

		(*dst)[i].buf.base = cursor;
		memcpy((*dst)[i].buf.base, src[i].buf.base, src[i].buf.len);

		(*dst)[i].batch = batch;

		cursor += src[i].buf.len;
	}
}
