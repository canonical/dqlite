#include <sqlite3.h>

#include "tuple.h"
#include "assert.h"

/* Return the tuple header size in bytes, for a tuple of @n values.
 *
 * If the tuple is a row, then each slot is 4 bits, otherwise if the tuple is a
 * sequence of parameters each slot is 8 bits. */
static size_t calc_header_size(unsigned long n, int format)
{
	size_t size;

	switch (format) {
		case TUPLE__ROW:
			/* Half a byte for each slot, rounded up... */
			size = (n + 1) / 2;
			/* ...and padded to a multiple of 8 bytes. */
			size = BytePad64(size);
			break;
		case TUPLE__PARAMS:
			/* 1-byte params count at the beginning of the first word */
			size = n + 1;
			size = BytePad64(size);
			/* Params count is not included in the header */
			size -= 1;
			break;
		case TUPLE__PARAMS32:
			/* 4-byte params count at the beginning of the first word */
			size = n + 4;
			size = BytePad64(size);
			/* Params count is not included in the header */
			size -= 4;
			break;
		default:
			assert(0);
	}

	return size;
}

int tuple_decoder__init(struct tuple_decoder *d,
			unsigned n,
			int format,
			struct cursor *cursor)
{
	size_t header_size;
	uint8_t byte = 0;
	uint32_t val = 0;
	int rc = 0;

	switch (format) {
		case TUPLE__ROW:
			assert(n > 0);
			d->n = n;
			break;
		case TUPLE__PARAMS:
			assert(n == 0);
			rc = uint8__decode(cursor, &byte);
			d->n = byte;
			break;
		case TUPLE__PARAMS32:
			assert(n == 0);
			rc = uint32__decode(cursor, &val);
			d->n = val;
			break;
		default:
			assert(0);
	}
	if (rc != 0) {
		return rc;
	}

	d->format = format;
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
unsigned long tuple_decoder__n(struct tuple_decoder *d)
{
	return d->n;
}

/* Return the type of the i'th value of the tuple. */
static int get_type(struct tuple_decoder *d, unsigned long i)
{
	int type;

	/* In row format the type slot size is 4 bits, while in params format
	 * the slot is 8 bits. */
	if (d->format == TUPLE__ROW) {
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
			rc = blob__decode(d->cursor, &value->blob);
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
			unsigned long n,
			int format,
			struct buffer *buffer)
{
	void *cursor;
	size_t n_header;

	e->n = n;
	e->format = format;
	e->buffer = buffer;
	e->i = 0;

	/* When encoding a tuple of parameters, we need to write the
	 * number of values at the beginning of the header. */
	if (e->format == TUPLE__PARAMS) {
		assert(n <= UINT8_MAX);
		uint8_t *header = buffer__advance(buffer, 1);
		if (header == NULL) {
			return DQLITE_NOMEM;
		}
		header[0] = (uint8_t)n;
	} else if (e->format == TUPLE__PARAMS32) {
		uint32_t val = (uint32_t)n;
		assert((unsigned long long)val == (unsigned long long)n);
		void *header = buffer__advance(buffer, 4);
		if (header == NULL) {
			return DQLITE_NOMEM;
		}
		uint32__encode(&val, &header);
	}

	e->header = buffer__offset(buffer);

	/* Reset the header */
	n_header = calc_header_size(n, format);
	memset(encoder__header(e), 0, n_header);

	/* Advance the buffer write pointer past the tuple header. */
	cursor = buffer__advance(buffer, n_header);
	if (cursor == NULL) {
		return DQLITE_NOMEM;
	}

	return 0;
}

/* Set the type of the i'th value of the tuple. */
static void set_type(struct tuple_encoder *e, unsigned long i, int type)
{
	uint8_t *header = encoder__header(e);

	/* In row format the type slot size is 4 bits, while in params format
	 * the slot is 8 bits. */
	if (e->format == TUPLE__ROW) {
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
			size = blob__sizeof(&value->blob);
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
	cursor = buffer__advance(e->buffer, size);
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
			blob__encode(&value->blob, &cursor);
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
