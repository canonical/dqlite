#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <sqlite3.h>

#include "./lib/assert.h"

#include "format.h"

/* Decode the page size ("Must be a power of two between 512 and 32768
 * inclusive, or the value 1 representing a page size of 65536").
 *
 * Return 0 if the page size is out of bound. */
static unsigned formatDecodePageSize(uint8_t buf[4])
{
	uint64_t page_size = 0;

	page_size += (uint64_t)(buf[0] << 24);
	page_size += (uint64_t)(buf[1] << 16);
	page_size += (uint64_t)(buf[2] << 8);
	page_size += (uint64_t)(buf[3]);

	if (page_size == 1) {
		page_size = FORMAT__PAGE_SIZE_MAX;
	} else if (page_size < FORMAT__PAGE_SIZE_MIN) {
		page_size = 0;
	} else if (page_size > (FORMAT__PAGE_SIZE_MAX / 2)) {
		page_size = 0;
	} else if (((page_size - 1) & page_size) != 0) {
		page_size = 0;
	}

	return (unsigned)page_size;
}

void formatWalGetPageSize(const uint8_t *header, unsigned *page_size)
{
	/* The page size is stored in the 4 bytes starting at 8
	 * (big-endian) */
	uint8_t buf[4] = {header[8], header[9], header[10], header[11]};

	*page_size = formatDecodePageSize(buf);
}

void formatDatabaseGetPageSize(const uint8_t *header, unsigned *page_size)
{
	/* The page size is stored in the 16th and 17th bytes
	 * (big-endian) */
	uint8_t buf[4] = {0, 0, header[16], header[17]};

	*page_size = formatDecodePageSize(buf);
}

void formatWalGetMxFrame(const uint8_t *header, uint32_t *mx_frame)
{
	assert(header != NULL);
	assert(mx_frame != NULL);

	/* The mxFrame number is 16th byte of the WAL index header. See also
	 * https://sqlite.org/walformat.html. */
	*mx_frame = ((uint32_t *)header)[4];
}

void formatWalGetReadMarks(const uint8_t *header,
			    uint32_t read_marks[FORMAT__WAL_NREADER])
{
	uint32_t *idx;

	assert(header != NULL);
	assert(read_marks != NULL);

	idx = (uint32_t *)header;

	/* The read-mark array starts at the 100th byte of the WAL index
	 * header. See also https://sqlite.org/walformat.html. */
	memcpy(read_marks, &idx[25], (sizeof *idx) * FORMAT__WAL_NREADER);
}
