#include "compress.h"

#ifdef LZ4_AVAILABLE
#include <lz4frame.h>
#endif
#include <limits.h>
#include <string.h>

#include "assert.h"
#include "byte.h"
#include "err.h"

#define min(a, b) ((a) < (b) ? (a) : (b))
#define max(a, b) ((a) > (b) ? (a) : (b))
#define MEGABYTE 1048576

int Compress(struct raft_buffer bufs[],
	     unsigned n_bufs,
	     struct raft_buffer *compressed,
	     char *errmsg)
{
#ifndef LZ4_AVAILABLE
	(void)bufs;
	(void)n_bufs;
	(void)compressed;
	ErrMsgPrintf(errmsg, "LZ4 not available");
	return RAFT_INVALID;
#else
	assert(bufs != NULL);
	assert(n_bufs > 0);
	assert(compressed != NULL);
	assert(errmsg != NULL);

	int rv = RAFT_IOERR;
	size_t src_size = 0;
	size_t dst_size = 0;
	size_t src_offset = 0;
	size_t dst_offset = 0;
	size_t dst_size_needed = 0; /* Store minimal dst_size */
	size_t ret = 0;             /* Return value of LZ4F_XXX functions */
	compressed->base = NULL;
	compressed->len = 0;

	/* Determine total uncompressed size */
	for (unsigned i = 0; i < n_bufs; ++i) {
		src_size += bufs[i].len;
	}

	/* Work around a bug in liblz4 on bionic, in practice raft should only
	 * Compress non-0 length buffers, so this should be fine.
	 * https://github.com/lz4/lz4/issues/157
	 * */
	if (src_size == 0) {
		ErrMsgPrintf(errmsg, "total size must be larger then 0");
		rv = RAFT_INVALID;
		goto err;
	}

	/* Set LZ4 preferences */
	LZ4F_preferences_t lz4_pref;
	memset(&lz4_pref, 0, sizeof(lz4_pref));
	/* Detect data corruption when decompressing */
	lz4_pref.frameInfo.contentChecksumFlag = 1;
	/* For allocating a suitable buffer when decompressing */
	lz4_pref.frameInfo.contentSize = src_size;

	/* Context to track compression progress */
	LZ4F_compressionContext_t ctx;
	ret = LZ4F_createCompressionContext(&ctx, LZ4F_VERSION);
	if (LZ4F_isError(ret)) {
		ErrMsgPrintf(errmsg, "LZ4F_createDecompressionContext %s",
			     LZ4F_getErrorName(ret));
		rv = RAFT_NOMEM;
		goto err;
	}

	/* Guestimate of eventual compressed size, mainly not to allocate a huge
	 * buffer as `LZ4F_compressBound` calculates the worst case scenario. */
	dst_size = LZ4F_compressBound(
	    max(MEGABYTE, (size_t)lz4_pref.frameInfo.contentSize / 10),
	    &lz4_pref);
	dst_size += LZ4F_HEADER_SIZE_MAX_RAFT;
	compressed->base = raft_malloc(dst_size);
	if (compressed->base == NULL) {
		rv = RAFT_NOMEM;
		goto err_after_ctx_alloc;
	}

	/* Returns the size of the lz4 header, data should be written after the
	 * header */
	dst_offset =
	    LZ4F_compressBegin(ctx, compressed->base, dst_size, &lz4_pref);
	if (LZ4F_isError(dst_offset)) {
		ErrMsgPrintf(errmsg, "LZ4F_compressBegin %s",
			     LZ4F_getErrorName(dst_offset));
		rv = RAFT_IOERR;
		goto err_after_buff_alloc;
	}

	/* Compress all buffers */
	for (unsigned i = 0; i < n_bufs; ++i) {
		src_offset = 0;
		while (src_offset < bufs[i].len) {
			/* Compress in chunks of maximum 1MB and check if there
			 * is enough room in the dst buffer, if not realloc */
			src_size =
			    min(bufs[i].len - src_offset, (size_t)MEGABYTE);
			dst_size_needed =
			    LZ4F_compressBound(src_size, &lz4_pref);
			if (dst_size - dst_offset < dst_size_needed) {
				dst_size +=
				    max(dst_size_needed,
					(size_t)lz4_pref.frameInfo.contentSize /
					    10);
				compressed->base =
				    raft_realloc(compressed->base, dst_size);
				if (compressed->base == NULL) {
					rv = RAFT_NOMEM;
					goto err_after_ctx_alloc;
				}
			}
			/* There is guaranteed enough room in `dst` to perform
			 * the compression */
			ret = LZ4F_compressUpdate(
			    ctx, (char *)compressed->base + dst_offset,
			    dst_size - dst_offset,
			    (char *)bufs[i].base + src_offset, src_size, NULL);
			if (LZ4F_isError(ret)) {
				ErrMsgPrintf(errmsg, "LZ4F_compressUpdate %s",
					     LZ4F_getErrorName(ret));
				rv = RAFT_IOERR;
				goto err_after_buff_alloc;
			}
			dst_offset += ret;
			src_offset += src_size;
		}
	}

	/* Make sure LZ4F_compressEnd has enough room to succeed */
	dst_size_needed = LZ4F_compressBound(0, &lz4_pref);
	if ((dst_size - dst_offset) < dst_size_needed) {
		dst_size += dst_size_needed;
		compressed->base = raft_realloc(compressed->base, dst_size);
		if (compressed->base == NULL) {
			rv = RAFT_NOMEM;
			goto err_after_ctx_alloc;
		}
	}

	/* Finalize compression */
	ret = LZ4F_compressEnd(ctx, (char *)compressed->base + dst_offset,
			       dst_size - dst_offset, NULL);
	if (LZ4F_isError(ret)) {
		ErrMsgPrintf(errmsg, "LZ4F_compressEnd %s",
			     LZ4F_getErrorName(ret));
		rv = RAFT_IOERR;
		goto err_after_buff_alloc;
	}

	dst_offset += ret;
	compressed->len = dst_offset;

	LZ4F_freeCompressionContext(ctx);
	return 0;

err_after_buff_alloc:
	raft_free(compressed->base);
	compressed->base = NULL;
err_after_ctx_alloc:
	LZ4F_freeCompressionContext(ctx);
err:
	return rv;
#endif /* LZ4_AVAILABLE */
}

int Decompress(struct raft_buffer buf,
	       struct raft_buffer *decompressed,
	       char *errmsg)
{
#ifndef LZ4_AVAILABLE
	(void)buf;
	(void)decompressed;
	ErrMsgPrintf(errmsg, "LZ4 not available");
	return RAFT_INVALID;
#else
	assert(decompressed != NULL);

	int rv = RAFT_IOERR;
	size_t src_offset = 0;
	size_t dst_offset = 0;
	size_t src_size = 0;
	size_t dst_size = 0;
	size_t ret = 0;

	LZ4F_decompressionContext_t ctx;
	if (LZ4F_isError(LZ4F_createDecompressionContext(&ctx, LZ4F_VERSION))) {
		ErrMsgPrintf(errmsg, "LZ4F_createDecompressionContext");
		rv = RAFT_NOMEM;
		goto err;
	}

	src_size = buf.len;
	LZ4F_frameInfo_t frameInfo = {0};
	/* `src_size` will contain the size of the LZ4 Frame Header after the
	 * call, decompression must resume at that offset. */
	ret = LZ4F_getFrameInfo(ctx, &frameInfo, buf.base, &src_size);
	if (LZ4F_isError(ret)) {
		ErrMsgPrintf(errmsg, "LZ4F_getFrameInfo %s",
			     LZ4F_getErrorName(ret));
		rv = RAFT_IOERR;
		goto err_after_ctx_alloc;
	}
	src_offset = src_size;

	decompressed->base = raft_malloc((size_t)frameInfo.contentSize);
	decompressed->len = (size_t)frameInfo.contentSize;
	if (decompressed->base == NULL) {
		rv = RAFT_NOMEM;
		goto err_after_ctx_alloc;
	}

	ret = 1;
	while (ret != 0) {
		src_size = buf.len - src_offset;
		/* !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		 * The next line works around a bug in an older lz4 lib where
		 * the `size_t` dst_size parameter would overflow an `int`.
		 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		 */
		dst_size = min(decompressed->len - dst_offset, (size_t)INT_MAX);
		/* `dst_size` will contain the number of bytes written to
		 * decompressed->base, while `src_size` will contain the number
		 * of bytes consumed from buf.base */
		ret = LZ4F_decompress(
		    ctx, (char *)decompressed->base + dst_offset, &dst_size,
		    (char *)buf.base + src_offset, &src_size, NULL);
		if (LZ4F_isError(ret)) {
			ErrMsgPrintf(errmsg, "LZ4F_decompress %s",
				     LZ4F_getErrorName(ret));
			rv = RAFT_IOERR;
			goto err_after_buff_alloc;
		}
		src_offset += src_size;
		dst_offset += dst_size;
	}

	if (LZ4F_freeDecompressionContext(ctx) != 0) {
		raft_free(decompressed->base);
		decompressed->base = NULL;
		return RAFT_IOERR;
	}

	return 0;

err_after_buff_alloc:
	raft_free(decompressed->base);
	decompressed->base = NULL;
err_after_ctx_alloc:
	LZ4F_freeDecompressionContext(ctx);
err:
	return rv;
#endif /* LZ4_AVAILABLE */
}

bool IsCompressed(const void *data, size_t sz)
{
	if (data == NULL || sz < 4) {
		return false;
	}
	const void *cursor = data;
#ifdef LZ4F_MAGICNUMBER
#define RAFT_LZ4F_MAGICNUMBER LZ4F_MAGICNUMBER
#else
#define RAFT_LZ4F_MAGICNUMBER 0x184D2204U
#endif
	return byteGet32(&cursor) == RAFT_LZ4F_MAGICNUMBER;
}
