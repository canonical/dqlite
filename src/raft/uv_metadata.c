#include "assert.h"
#include "byte.h"
#include "uv.h"
#include "uv_encoding.h"

/* We have metadata1 and metadata2. */
#define METADATA_FILENAME_PREFIX "metadata"
#define METADATA_FILENAME_SIZE (sizeof(METADATA_FILENAME_PREFIX) + 2)

/* Format, version, term, vote */
#define METADATA_CONTENT_SIZE (8 * 4)

/* Encode the content of a metadata file. */
static void uvMetadataEncode(const struct uvMetadata *metadata, void *buf)
{
	void *cursor = buf;
	bytePut64(&cursor, UV__DISK_FORMAT);
	bytePut64(&cursor, metadata->version);
	bytePut64(&cursor, metadata->term);
	bytePut64(&cursor, metadata->voted_for);
}

/* Decode the content of a metadata file. */
static int uvMetadataDecode(const void *buf,
			    struct uvMetadata *metadata,
			    char *errmsg)
{
	const void *cursor = buf;
	uint64_t format;
	format = byteGet64(&cursor);
	if (format != UV__DISK_FORMAT) {
		ErrMsgPrintf(errmsg, "bad format version %ju", format);
		return RAFT_MALFORMED;
	}
	metadata->version = byteGet64(&cursor);
	metadata->term = byteGet64(&cursor);
	metadata->voted_for = byteGet64(&cursor);

	/* Coherence checks that values make sense */
	if (metadata->version == 0) {
		ErrMsgPrintf(errmsg, "version is set to zero");
		return RAFT_CORRUPT;
	}

	return 0;
}

/* Render the filename of the metadata file with index @n. */
static void uvMetadataFilename(const unsigned short n, char *filename)
{
	sprintf(filename, METADATA_FILENAME_PREFIX "%d", n);
}

/* Read the n'th metadata file (with n equal to 1 or 2) and decode the content
 * of the file, populating the given metadata buffer accordingly. */
static int uvMetadataLoadN(const char *dir,
			   const unsigned short n,
			   struct uvMetadata *metadata,
			   char *errmsg)
{
	char filename[METADATA_FILENAME_SIZE];  /* Filename of the metadata file
						 */
	uint8_t content[METADATA_CONTENT_SIZE]; /* Content of metadata file */
	off_t size;
	struct raft_buffer buf;
	bool exists;
	int rv;

	assert(n == 1 || n == 2);

	/* Render the metadata path */
	uvMetadataFilename(n, filename);

	rv = UvFsFileExists(dir, filename, &exists, errmsg);
	if (rv != 0) {
		ErrMsgWrapf(errmsg, "check if %s exists", filename);
		return rv;
	}

	memset(metadata, 0, sizeof *metadata);

	/* If the file does not exist, just return. */
	if (!exists) {
		return 0;
	}

	/* If the file exists but has less bytes than expected assume that the
	 * server crashed while writing this metadata file, and pretend it has
	 * not been written at all. If it has more file than expected, return an
	 * error. */
	rv = UvFsFileSize(dir, filename, &size, errmsg);
	if (rv != 0) {
		ErrMsgWrapf(errmsg, "check size of %s", filename);
		return rv;
	}

	if (size != sizeof content) {
		if ((size_t)size < sizeof content) {
			rv = UvFsRemoveFile(dir, filename, errmsg);
			if (rv != 0) {
				return rv;
			}
			return 0;
		}
		ErrMsgPrintf(errmsg, "%s has size %jd instead of %zu", filename,
			     (intmax_t)size, sizeof content);
		return RAFT_CORRUPT;
	}

	/* Read the content of the metadata file. */
	buf.base = content;
	buf.len = sizeof content;

	rv = UvFsReadFileInto(dir, filename, &buf, errmsg);
	if (rv != 0) {
		ErrMsgWrapf(errmsg, "read content of %s", filename);
		return rv;
	};

	/* Decode the content of the metadata file. */
	rv = uvMetadataDecode(content, metadata, errmsg);
	if (rv != 0) {
		ErrMsgWrapf(errmsg, "decode content of %s", filename);
		return rv;
	}

	return 0;
}

int uvMetadataLoad(const char *dir, struct uvMetadata *metadata, char *errmsg)
{
	struct uvMetadata metadata1;
	struct uvMetadata metadata2;
	int rv;

	/* Read the two metadata files (if available). */
	rv = uvMetadataLoadN(dir, 1, &metadata1, errmsg);
	if (rv != 0) {
		return rv;
	}
	rv = uvMetadataLoadN(dir, 2, &metadata2, errmsg);
	if (rv != 0) {
		return rv;
	}

	/* Check the versions. */
	if (metadata1.version == 0 && metadata2.version == 0) {
		/* Neither metadata file exists: have a brand new server. */
		metadata->version = 0;
		metadata->term = 0;
		metadata->voted_for = 0;
	} else if (metadata1.version == metadata2.version) {
		/* The two metadata files can't have the same version. */
		ErrMsgPrintf(errmsg,
			     "metadata1 and metadata2 are both at version %llu",
			     metadata1.version);
		return RAFT_CORRUPT;
	} else {
		/* Pick the metadata with the grater version. */
		if (metadata1.version > metadata2.version) {
			*metadata = metadata1;
		} else {
			*metadata = metadata2;
		}
	}

	return 0;
}

/* Return the metadata file index associated with the given version. */
static unsigned short uvMetadataFileIndex(unsigned long long version)
{
	return version % 2 == 1 ? 1 : 2;
}

int uvMetadataStore(struct uv *uv, const struct uvMetadata *metadata)
{
	char filename[METADATA_FILENAME_SIZE];  /* Filename of the metadata file
						 */
	uint8_t content[METADATA_CONTENT_SIZE]; /* Content of metadata file */
	struct raft_buffer buf;
	unsigned short n;
	int rv;

	assert(metadata->version > 0);

	/* Encode the given metadata. */
	uvMetadataEncode(metadata, content);

	/* Render the metadata file name. */
	n = uvMetadataFileIndex(metadata->version);
	uvMetadataFilename(n, filename);

	/* Write the metadata file, creating it if it does not exist. */
	buf.base = content;
	buf.len = sizeof content;
	rv = UvFsMakeOrOverwriteFile(uv->dir, filename, &buf, uv->io->errmsg);
	if (rv != 0) {
		ErrMsgWrapf(uv->io->errmsg, "persist %s", filename);
		return rv;
	}

	return 0;
}
