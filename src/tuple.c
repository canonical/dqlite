#include <sqlite3.h>

#include "tuple.h"
#include "assert.h"

/* True if a tuple decoder or decoder is using parameter format. */
#define HAS_PARAMS_FORMAT(P) (P->format == TUPLE__PARAMS)

/* True if a tuple decoder or decoder is using row format. */
#define HAS_ROW_FORMAT(P) (P->format == TUPLE__ROW)

/* Return the tuple header size in bytes, for a tuple of @n values.
 *
 * If the tuple is a row, then each slot is 4 bits, otherwise if the tuple is a
 * sequence of parameters each slot is 8 bits. */
static size_t calc_header_size(unsigned n, int format)
{
	size_t size;

	if (format == TUPLE__ROW) {
		size = (n / 2) * sizeof(uint8_t);
		if (n % 2 != 0) {
			size += sizeof(uint8_t);
		}
		size = byte__pad64(size);
	} else {
		assert(format == TUPLE__PARAMS);
		 /* Include params count for the purpose of calculating possible
		  * padding, but then exclude it as we have already read it. */
		size = sizeof(uint8_t) + n * sizeof(uint8_t);
		size = byte__pad64(size);
		size -= sizeof(uint8_t);
	}

	return size;
}

int tuple_decoder__init(struct tuple_decoder *d,
			unsigned n,
			struct cursor *cursor)
{
	size_t header_size;
	int rc;

	d->format = n == 0 ? TUPLE__PARAMS : TUPLE__ROW;

	/* When using row format the number of values is the given one,
	 * otherwise we have to read it from the header. */
	if (HAS_ROW_FORMAT(d)) {
		d->n = n;
	} else {
		uint8_t byte;
		rc = uint8__decode(cursor, &byte);
		if (rc != 0) {
			return rc;
		}
		d->n = byte;
	}

	d->i = 0;
	d->header = cursor->p;

	/* Check that there is enough room to hold n type code slots. */
	header_size = calc_header_size(d->n, d->format);

	if (header_size > cursor->cap) {
		return DQLITE_PARSE;
	}

	d->cursor = cursor;
	d->cursor->p += header_size;
	d->cursor->cap -= header_size;

	return 0;
}

/* Return the number of values in the decoder's tuple. */
unsigned tuple_decoder__n(struct tuple_decoder *d)
{
	return d->n;
}

/* Return the type of the i'th value of the tuple. */
static int get_type(struct tuple_decoder *d, unsigned i)
{
	int type;

	/* In row format the type slot size is 4 bits, while in params format
	 * the slot is 8 bits. */
	if (HAS_ROW_FORMAT(d)) {
		type = d->header[i / 2];
		if (i % 2 == 0) {
			type &= 0x0f;
		} else {
			type = type >> 4;
		}
	} else {
		type = d->header[i];
	}

	return type;
}

int tuple_decoder__next(struct tuple_decoder *d, struct value *value)
{
	int rc;
	assert(d->i < d->n);
	value->type = get_type(d, d->i);
	switch (value->type) {
		case SQLITE_INTEGER:
			rc = int64__decode(d->cursor, &value->integer);
			break;
		case SQLITE_FLOAT:
			rc = float__decode(d->cursor, &value->float_);
			break;
		case SQLITE_BLOB:
			rc = blobDecode(d->cursor, &value->blob);
			break;
		case SQLITE_NULL:
			/* TODO: allow null to be encoded with 0 bytes? */
			rc = uint64__decode(d->cursor, &value->null);
			break;
		case SQLITE_TEXT:
			rc = text__decode(d->cursor, &value->text);
			break;
		case DQLITE_ISO8601:
			rc = text__decode(d->cursor, &value->iso8601);
			break;
		case DQLITE_BOOLEAN:
			rc = uint64__decode(d->cursor, &value->boolean);
			break;
		default:
			rc = DQLITE_PARSE;
			break;
	};
	if (rc != 0) {
		return rc;
	}
	d->i++;
	return 0;
}

/* Return a pointer to the tuple header. */
static uint8_t *encoder__header(struct tuple_encoder *e)
{
	return buffer__cursor(e->buffer, e->header);
}

int tuple_encoder__init(struct tuple_encoder *e,
			unsigned n,
			int format,
			struct buffer *buffer)
{
	void *cursor;
	size_t n_header;

	e->n = n;
	e->format = format;
	e->buffer = buffer;
	e->i = 0;

	/* With params format we need to fill the first byte of the header with
	 * the params count. */
	if (HAS_PARAMS_FORMAT(e)) {
		uint8_t *header = buffer_advance(buffer, 1);
		if (header == NULL) {
			return DQLITE_NOMEM;
		}
		header[0] = (uint8_t)n;
	}

	e->header = buffer__offset(buffer);

	/* Reset the header */
	n_header = calc_header_size(n, format);
	memset(encoder__header(e), 0, n_header);

	/* Advance the buffer write pointer past the tuple header. */
	cursor = buffer_advance(buffer, n_header);
	if (cursor == NULL) {
		return DQLITE_NOMEM;
	}

	return 0;
}

/* Set the type of the i'th value of the tuple. */
static void set_type(struct tuple_encoder *e, unsigned i, int type)
{
	uint8_t *header = encoder__header(e);

	/* In row format the type slot size is 4 bits, while in params format
	 * the slot is 8 bits. */
	if (HAS_ROW_FORMAT(e)) {
		uint8_t *slot;
		slot = &header[i / 2];
		if (i % 2 == 0) {
			*slot = (uint8_t)type;
		} else {
			*slot |= (uint8_t)(type << 4);
		}
	} else {
		header[i] = (uint8_t)type;
	}
}

int tuple_encoder__next(struct tuple_encoder *e, struct value *value)
{
	void *cursor;
	size_t size;

	assert(e->i < e->n);

	set_type(e, e->i, value->type);

	switch (value->type) {
		case SQLITE_INTEGER:
			size = int64__sizeof(&value->integer);
			break;
		case SQLITE_FLOAT:
			size = float__sizeof(&value->float_);
			break;
		case SQLITE_BLOB:
			size = blobSizeof(&value->blob);
			break;
		case SQLITE_NULL:
			/* TODO: allow null to be encoded with 0 bytes */
			size = uint64__sizeof(&value->null);
			break;
		case SQLITE_TEXT:
			size = text__sizeof(&value->text);
			break;
		case DQLITE_UNIXTIME:
			size = int64__sizeof(&value->unixtime);
			break;
		case DQLITE_ISO8601:
			size = text__sizeof(&value->iso8601);
			break;
		case DQLITE_BOOLEAN:
			size = uint64__sizeof(&value->boolean);
			break;
		default:
			assert(0);
	};

	/* Advance the buffer write pointer. */
	cursor = buffer_advance(e->buffer, size);
	if (cursor == NULL) {
		return DQLITE_NOMEM;
	}

	switch (value->type) {
		case SQLITE_INTEGER:
			int64__encode(&value->integer, &cursor);
			break;
		case SQLITE_FLOAT:
			float__encode(&value->float_, &cursor);
			break;
		case SQLITE_BLOB:
			blobEncode(&value->blob, &cursor);
			break;
		case SQLITE_NULL:
			/* TODO: allow null to be encoded with 0 bytes */
			uint64__encode(&value->null, &cursor);
			break;
		case SQLITE_TEXT:
			text__encode(&value->text, &cursor);
			break;
		case DQLITE_UNIXTIME:
			int64__encode(&value->unixtime, &cursor);
			break;
		case DQLITE_ISO8601:
			text__encode(&value->iso8601, &cursor);
			break;
		case DQLITE_BOOLEAN:
			uint64__encode(&value->boolean, &cursor);
			break;
	};

	e->i++;

	return 0;
}
