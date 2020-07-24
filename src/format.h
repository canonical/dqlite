/* Utilities around SQLite file formats.
 *
 * See https://sqlite.org/fileformat.html. */

#ifndef FORMAT_H_
#define FORMAT_H_

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

/* Given the page size, calculate the size of a full WAL frame (frame header
 * plus page data). */
#define formatWalCalcFrameSize(PAGE_SIZE) \
	(FORMAT__WAL_FRAME_HDR_SIZE + PAGE_SIZE)

/* Given the page size and the WAL file size, calculate the number of pages
 * currently in the WAL. */
#define formatWalCalcPages(PAGE_SIZE, SIZE) \
	((SIZE - FORMAT__WAL_HDR_SIZE) / formatWalCalcFrameSize(PAGE_SIZE))

/* Given the page size, calculate the WAL page number of the frame starting at
 * the given offset. */
#define formatWalCalcPgno(PAGE_SIZE, OFFSET) \
	formatWalCalcPages(PAGE_SIZE,        \
			   OFFSET + formatWalCalcFrameSize(PAGE_SIZE))

/* Extract the page size from the content of the WAL header.
 *
 * The given buffer must hold at least FORMAT__WAL_HDR_SIZE.
 *
 * If the page size is invalid, 0 is returned. */
void formatWalGetPageSize(const uint8_t *header, unsigned *page_size);

/* Extract the page size from the content of the database header.
 *
 * The given buffer must hold at least FORMAT__DB_HDR_SIZE bytes.
 *
 * If the page size is invalid, 0 is returned. */
void formatDatabaseGetPageSize(const uint8_t *header, unsigned *page_size);

/* Extract the mxFrame field from the WAL index header stored in the given
 * buffer */
void formatWalGetMxFrame(const uint8_t *header, uint32_t *mx_frame);

/* Extract the read marks array from the WAL index header stored in the given
 * buffer. */
void formatWalGetReadMarks(const uint8_t *header,
			   uint32_t read_marks[FORMAT__WAL_NREADER]);

#endif /* FORMAT_H */
