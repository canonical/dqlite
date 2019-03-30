#include "tuple.h"
#include "assert.h"

int tuple_decoder__init(struct tuple_decoder *d,
			unsigned n,
			struct cursor *cursor)
{
	size_t header_size;
	int rc;
	d->is_row = n != 0;
	if (d->is_row) {
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

	/* Check that there is enough room to hold n type code slots. If the
	 * tuple is a row, then each slot is 4 bits, otherwise if the tuple is a
	 * sequence of parameters each slot is 8 bits. */
	if (d->is_row) {
		header_size = (n / 2) * sizeof(uint8_t);
		if (n % 2 != 0) {
			header_size += sizeof(uint8_t);
		}
		header_size = byte__pad64(header_size);
	} else {
		header_size = d->n * sizeof(uint8_t);
		header_size = byte__pad64(header_size);
		header_size -= sizeof(uint8_t); /* The first byte holds n */
	}

	if (header_size > cursor->cap) {
		return DQLITE_PARSE;
	}

	d->header = cursor->p;
	d->cursor = cursor;
	d->cursor->p += header_size;
	d->cursor->cap -= header_size;

	return 0;
}

/* Return the type of the i'th value of the tuple. */
static int get_type(struct tuple_decoder *d, unsigned i)
{
	int type;

	/* In row format the type slot size is 4 bits, while in params format
	 * the slot is 8 bits. */
	if (d->is_row) {
		type = d->header[i / 2];
		if (d->i % 2 == 0) {
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
			assert(0); /* TODO */
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
