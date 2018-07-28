#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <sqlite3.h>

#include "format.h"

int dqlite__format_get_page_size(int type, const uint8_t *buf, unsigned *page_size) {
	assert(buf != NULL);
	assert(page_size != NULL);
	assert(type == DQLITE__FORMAT_DB || type == DQLITE__FORMAT_WAL);

	if (type == DQLITE__FORMAT_DB) {
		/* The page size is stored in the 16th and 17th bytes (big-endian) */
		*page_size = (buf[16] << 8) + buf[17];
	} else {
		/* The page size is stored in the 4 bytes starting at 8
		 * (big-endian) */
		*page_size =
		    ((buf[8] << 24) + (buf[9] << 16) + (buf[10] << 8) + buf[11]);
	}

	/* Validate the page size ("Must be a power of two between 512 and 32768
	 * inclusive, or the value 1 representing a page size of 65536"). */
	if (*page_size == 1) {
		*page_size = DQLITE__FORMAT_PAGE_SIZE_MAX;
	} else if (*page_size < DQLITE__FORMAT_PAGE_SIZE_MIN) {
		return SQLITE_CORRUPT;
	} else if (*page_size > (DQLITE__FORMAT_PAGE_SIZE_MAX / 2)) {
		return SQLITE_CORRUPT;
	} else if (((*page_size - 1) & *page_size) != 0) {
		return SQLITE_CORRUPT;
	}

	return SQLITE_OK;
}

void dqlite__format_get_mx_frame(const uint8_t *buf, uint32_t *mx_frame) {
	assert(buf != NULL);
	assert(mx_frame != NULL);

	/* The mxFrame number is 16th byte of the WAL index header. See also
	 * https://sqlite.org/walformat.html. */
	*mx_frame = ((uint32_t *)buf)[4];
}

void dqlite__format_get_read_marks(const uint8_t *buf,
                                   uint32_t read_marks[DQLITE__FORMAT_WAL_NREADER]) {
	uint32_t *idx;

	assert(buf != NULL);
	assert(read_marks != NULL);

	idx = (uint32_t *)buf;

	/* The read-mark array starts at the 100th byte of the WAL index
	 * header. See also https://sqlite.org/walformat.html. */
	memcpy(read_marks, &idx[25], (sizeof *idx) * DQLITE__FORMAT_WAL_NREADER);
}
