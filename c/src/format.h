/******************************************************************************
 *
 * Utilities around SQLite file formats.
 *
 * See https://sqlite.org/fileformat.html.
 *
 *****************************************************************************/

#ifndef DQLITE_FORMAT_H
#define DQLITE_FORMAT_H

#include <stdint.h>
#include <stdlib.h>

/* Possible file types. */
#define DQLITE__FORMAT_DB 0
#define DQLITE__FORMAT_WAL 1
#define DQLITE__FORMAT_OTHER 2

/* Minumum and maximum page size. */
#define DQLITE__FORMAT_PAGE_SIZE_MIN 512
#define DQLITE__FORMAT_PAGE_SIZE_MAX 65536

/* Database header size. */
#define DQLITE__FORMAT_DB_HDR_SIZE 100

/* Write ahead log header size. */
#define DQLITE__FORMAT_WAL_HDR_SIZE 32

/* Write ahead log frame header size. */
#define DQLITE__FORMAT_WAL_FRAME_HDR_SIZE 24

/* Number of reader marks in the wal index header. */
#define DQLITE__FORMAT_WAL_NREADER 5

/* Given the page size, calculate the size of a full WAL frame (frame header
 * plus page data). */
#define dqlite__format_wal_calc_frame_size(PAGE_SIZE)                               \
	(DQLITE__FORMAT_WAL_FRAME_HDR_SIZE + PAGE_SIZE)

/* Given the page size and the WAL file size, calculate the number of pages
 * currently in the WAL. */
#define dqlite__format_wal_calc_pages(PAGE_SIZE, SIZE)                              \
	((SIZE - DQLITE__FORMAT_WAL_HDR_SIZE) /                                     \
	 dqlite__format_wal_calc_frame_size(PAGE_SIZE))

/* Given the page size, calculate the WAL page number of the frame starting at
 * the given offset. */
#define dqlite__format_wal_calc_pgno(PAGE_SIZE, OFFSET)                             \
	dqlite__format_wal_calc_pages(                                              \
	    PAGE_SIZE, OFFSET + dqlite__format_wal_calc_frame_size(PAGE_SIZE))

/* Extract the page size from the content of the first database page or from the
 * WAL header.
 *
 * If type is DQLITE__FORMAT_DB the given buffer must hold at least
 * DQLITE__FORMAT_DB_HDR_SIZE bytes.
 *
 * If type is DQLITE__FORMAT_WAL the given buffer must hold at least
 * DQLITE__FORMAT_WAL_HDR_SIZE. */
int dqlite__format_get_page_size(int            type,
                                 const uint8_t *buf,
                                 unsigned int * page_size);

/* Extract the mxFrame field from the WAL index header stored in the given
 * buffer */
void dqlite__format_get_mx_frame(const uint8_t *buf, uint32_t *mx_frame);

/* Extract the read marks array from the WAL index header stored in the given
 * buffer. */
void dqlite__format_get_read_marks(const uint8_t *buf,
                                   uint32_t read_marks[DQLITE__FORMAT_WAL_NREADER]);

#endif /* DQLITE_FORMAT_H */
