#ifndef COMPRESS_H_
#define COMPRESS_H_

#include "../raft.h"

#ifdef LZ4F_HEADER_SIZE_MAX
#define LZ4F_HEADER_SIZE_MAX_RAFT LZ4F_HEADER_SIZE_MAX
#else
#define LZ4F_HEADER_SIZE_MAX_RAFT 19UL
#endif

/*
 * Compresses the content of `bufs` into a newly allocated buffer that is
 * returned to the caller through `compressed`. Returns a non-0 value upon
 * failure.
 */
int Compress(struct raft_buffer bufs[],
	     unsigned n_bufs,
	     struct raft_buffer *compressed,
	     char *errmsg);

/*
 * Decompresses the content of `buf` into a newly allocated buffer that is
 * returned to the caller through `decompressed`. Returns a non-0 value upon
 * failure.
 */
int Decompress(struct raft_buffer buf,
	       struct raft_buffer *decompressed,
	       char *errmsg);

/* Returns `true` if `data` is compressed, `false` otherwise. */
bool IsCompressed(const void *data, size_t sz);

#endif /* COMPRESS_H_ */
