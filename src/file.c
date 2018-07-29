#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <sqlite3.h>

#include "format.h"

/* Guess the file type by looking the filename. */
static int dqlite__file_guess_type(const char *filename) {
	/* TODO: improve the check. */
	if (strstr(filename, "-wal") != NULL) {
		return DQLITE__FORMAT_WAL;
	}

	return DQLITE__FORMAT_DB;
}

int dqlite_file_read(const char *vfs_name,
                     const char *filename,
                     uint8_t **  buf,
                     size_t *    len) {
	sqlite3_vfs * vfs;
	int           type;
	int           flags;
	sqlite3_file *file;
	unsigned      page_size;
	sqlite3_int64 offset;
	int           rc;

	assert(vfs_name != NULL);
	assert(filename != NULL);
	assert(buf != NULL);
	assert(len != NULL);

	/* Lookup the VFS object to use. */
	vfs = sqlite3_vfs_find(vfs_name);
	if (vfs == NULL) {
		rc = SQLITE_ERROR;
		goto err;
	}

	type = dqlite__file_guess_type(filename);

	/* Common flags */
	flags = SQLITE_OPEN_READWRITE;

	if (type == DQLITE__FORMAT_DB) {
		flags |= SQLITE_OPEN_MAIN_DB;
	} else {
		flags |= SQLITE_OPEN_WAL;
	}

	/* Open the file */
	file = sqlite3_malloc(sizeof(*file));
	if (file == NULL) {
		rc = SQLITE_NOMEM;
		goto err;
	}

	rc = vfs->xOpen(vfs, filename, file, flags, &flags);
	if (rc != SQLITE_OK) {
		goto err_after_file_malloc;
	}

	/* Get the file size */
	rc = file->pMethods->xFileSize(file, (sqlite3_int64 *)len);
	if (rc != SQLITE_OK) {
		goto err_after_file_open;
	}

	/* Check if the file is empty. */
	if (*len == 0) {
		*buf = NULL;
		goto out;
	}

	/* Allocate the read buffer */
	*buf = sqlite3_malloc(*len);
	if (*buf == NULL) {
		rc = SQLITE_NOMEM;
		goto err_after_file_open;
	}

	/* Read the header. The buffer size is enough for both database and WAL
	 * files. */
	rc = file->pMethods->xRead(file, *buf, DQLITE__FORMAT_WAL_HDR_SIZE, 0);
	if (rc != SQLITE_OK) {
		goto err_after_buf_malloc;
	}

	/* Figure the page size. */
	rc = dqlite__format_get_page_size(type, *buf, &page_size);
	if (rc != SQLITE_OK) {
		goto err_after_buf_malloc;
	}

	offset = 0;

	/* If this is a WAL file , we have already read the header and we can
	 * move on. */
	if (type == DQLITE__FORMAT_WAL) {
		offset += DQLITE__FORMAT_WAL_HDR_SIZE;
	}

	while ((size_t)offset < *len) {
		uint8_t *pos = (*buf) + offset;

		if (type == DQLITE__FORMAT_WAL) {
			/* Read the frame header */
			rc = file->pMethods->xRead(
			    file, pos, DQLITE__FORMAT_WAL_FRAME_HDR_SIZE, offset);
			if (rc != SQLITE_OK) {
				goto err_after_buf_malloc;
			}
			offset += DQLITE__FORMAT_WAL_FRAME_HDR_SIZE;
			pos += DQLITE__FORMAT_WAL_FRAME_HDR_SIZE;
		}

		/* Read the page */
		rc = file->pMethods->xRead(file, pos, page_size, offset);
		if (rc != SQLITE_OK) {
			goto err_after_buf_malloc;
		}
		offset += page_size;
	};

out:
	file->pMethods->xClose(file);
	sqlite3_free(file);

	return SQLITE_OK;

err_after_buf_malloc:
	sqlite3_free(*buf);

err_after_file_open:
	file->pMethods->xClose(file);

err_after_file_malloc:
	sqlite3_free(file);

err:
	assert(rc != SQLITE_OK);

	*buf = NULL;
	*len = 0;

	return rc;
}

int dqlite_file_write(const char *vfs_name,
                      const char *filename,
                      uint8_t *   buf,
                      size_t      len) {
	sqlite3_vfs * vfs;
	sqlite3_file *file;
	int           type;
	int           flags;
	unsigned int  page_size;
	sqlite3_int64 offset;
	uint8_t *     pos;
	int           rc;

	assert(vfs_name != NULL);
	assert(filename != NULL);
	assert(buf != NULL);
	assert(len > 0);

	/* Lookup the VFS object to use. */
	vfs = sqlite3_vfs_find(vfs_name);
	if (vfs == NULL) {
		rc = SQLITE_ERROR;
		goto err;
	}

	/* Determine if this is a database or a WAL file. */
	type = dqlite__file_guess_type(filename);

	/* Common flags */
	flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;

	if (type == DQLITE__FORMAT_DB) {
		flags |= SQLITE_OPEN_MAIN_DB;
	} else {
		flags |= SQLITE_OPEN_WAL;
	}

	/* Open the file */
	file = (sqlite3_file *)sqlite3_malloc(sizeof(*file));
	if (file == NULL) {
		rc = SQLITE_NOMEM;
		goto err;
	}
	rc = vfs->xOpen(vfs, filename, file, flags, &flags);
	if (rc != SQLITE_OK) {
		goto err_after_file_malloc;
	}

	/* Truncate any existing content. */
	rc = file->pMethods->xTruncate(file, 0);
	if (rc != SQLITE_OK) {
		goto err_after_file_malloc;
	}

	/* Figure out the page size */
	rc = dqlite__format_get_page_size(type, buf, &page_size);
	if (rc != SQLITE_OK) {
		goto err_after_file_open;
	}

	offset = 0;
	pos    = buf;

	/* If this is a WAL file , write the header first. */
	if (type == DQLITE__FORMAT_WAL) {
		rc = file->pMethods->xWrite(
		    file, pos, DQLITE__FORMAT_WAL_HDR_SIZE, offset);
		if (rc != SQLITE_OK) {
			goto err_after_file_open;
		}
		offset += DQLITE__FORMAT_WAL_HDR_SIZE;
		pos += DQLITE__FORMAT_WAL_HDR_SIZE;
	}

	while ((size_t)offset < len) {
		if (type == DQLITE__FORMAT_WAL) {
			/* Write the frame header */
			rc = file->pMethods->xWrite(
			    file, pos, DQLITE__FORMAT_WAL_FRAME_HDR_SIZE, offset);
			if (rc != SQLITE_OK) {
				goto err_after_file_open;
			}
			offset += DQLITE__FORMAT_WAL_FRAME_HDR_SIZE;
			pos += DQLITE__FORMAT_WAL_FRAME_HDR_SIZE;
		}

		/* Write the page */
		rc = file->pMethods->xWrite(file, pos, page_size, offset);
		if (rc != SQLITE_OK) {
			goto err_after_file_open;
		}
		offset += page_size;
		pos += page_size;
	};

	file->pMethods->xClose(file);
	sqlite3_free(file);

	return SQLITE_OK;

err_after_file_open:
	file->pMethods->xClose(file);

err_after_file_malloc:
	sqlite3_free(file);

err:
	assert(rc != SQLITE_OK);

	return rc;
}
