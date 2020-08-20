/* Utilities around SQLite file formats.
 *
 * See https://sqlite.org/fileformat.html. */

#ifndef FORMAT_H_
#define FORMAT_H_

#include <stdbool.h>
#include <stdint.h>

/* Minumum and maximum page size. */
#define FORMAT__PAGE_SIZE_MIN 512
#define FORMAT__PAGE_SIZE_MAX 65536

/* Database header size. */
#define FORMAT__DB_HDR_SIZE 100

/* Write ahead log header size. */
#define FORMAT__WAL_HDR_SIZE 32

/* Write ahead log frame header size. */
#define FORMAT__WAL_FRAME_HDR_SIZE 24

/* Number of reader marks in the wal index header. */
#define FORMAT__WAL_NREADER 5

/* Lock index given the offset I in the aReadMark array. See the equivalent
 * WAL_READ_LOCK definition in the wal.c file of the SQLite source code. */
#define FORMAT__WAL_READ_LOCK(I) (3 + (I))

/* Size of the first part of the WAL index header. */
#define FORMAT__WAL_IDX_HDR_SIZE 48

/* Size of each memory region in the WAL index. Same as WALINDEX_PGSZ defined in
 * wal.c of SQLite. */
#define FORMAT__WAL_IDX_PAGE_SIZE 32768

/* Given the page size, calculate the size of a full WAL frame (frame header
 * plus page data). */
#define formatWalCalcFrameSize(PAGE_SIZE) \
	(FORMAT__WAL_FRAME_HDR_SIZE + PAGE_SIZE)

/* Given the page size and the WAL file size, calculate the number of frames it
 * has. */
#define formatWalCalcFramesNumber(PAGE_SIZE, SIZE) \
	((SIZE - FORMAT__WAL_HDR_SIZE) / formatWalCalcFrameSize(PAGE_SIZE))

/* Given the page size, calculate the WAL page number of the frame starting at
 * the given offset. */
#define formatWalCalcFrameIndex(PAGE_SIZE, OFFSET) \
	(formatWalCalcFramesNumber(PAGE_SIZE, OFFSET) + 1)

/* Extract the page size from the content of the database header.
 *
 * The given buffer must hold at least FORMAT__DB_HDR_SIZE bytes.
 *
 * If the page size is invalid, 0 is returned. */
void formatDatabaseGetPageSize(const uint8_t *header, unsigned *page_size);

/* Extract the page size from the content of the WAL header.
 *
 * The given buffer must hold at least FORMAT__WAL_HDR_SIZE.
 *
 * If the page size is invalid, 0 is returned. */
void formatWalGetPageSize(const uint8_t *header, unsigned *page_size);

/* Get the Salt-1 and Salt-2 fields stored in the WAL header. */
void formatWalGetSalt(const uint8_t *header, uint32_t salt[2]);

/* Extract the page number from a WAL frame header. */
void formatWalGetFramePageNumber(const uint8_t *header, uint32_t *page_number);

/* Return true if native byte order should be used when calculating WAL
 * checksums, or false if bit-endian byte order should be used instead. */
void formatWalGetNativeChecksum(const uint8_t *header, bool *native);

/* Restart the header of a WAL file after a checkpoint. */
void formatWalRestartHeader(uint8_t *header);

/* Encode a frame header and return the calculated checksums. */
void formatWalPutFrameHeader(bool native,
			     uint32_t page_number,
			     uint32_t database_size,
			     uint32_t salt[2],
			     uint32_t checksum[2],
			     uint8_t *header,
			     uint8_t *data,
			     unsigned n_data);

#endif /* FORMAT_H */
