/**
 * Encode and decode tuples of database values.
 *
 * A tuple is composed by a header and a body.
 *
 * The format of the header changes depending on whether the tuple is a sequence
 * of parameters to bind to a statement, or a sequence of values of a single row
 * yielded by a query.
 *
 * For a tuple of parameters the format of the header is:
 *
 *  8 bits: Number of values in the tuple.
 *  8 bits: Type code of the 1st value of the tuple.
 *  8 bits: Type code of the 2nd value of the tuple, or 0.
 *  8 bits: Type code of the 3rd value of the tuple, or 0.
 *  ...
 *
 * This repeats until reaching a full 64-bit word. If there are more than 7
 * parameters to bind, the header will grow additional 64-bit words as needed,
 * following the same pattern: a sequence of 8-bit slots with type codes of the
 * parameters followed by a sequence of zero bits, until word boundary is
 * reached.
 *
 * For a tuple of row values the format of the header is:
 *
 *  4 bits: Type code of the 1st value of the tuple.
 *  4 bits: Type code of the 2nd value of the tuple, or 0.
 *  4 bits: Type code of the 3rd value of the tuple, or 0.
 *  ...
 *
 * This repeats until reaching a full 64-bit word. If there are more than 16
 * values, the header will grow additional 64-bit words as needed, following the
 * same pattern: a sequence of 4-bit slots with type codes of the values
 * followed by a sequence of zero bits, until word boundary is reached.
 *
 * After the header the body follows immediately, which contains all parameters
 * or values in sequence, encoded using type-specific rules.
 */

#ifndef DQLITE_TUPLE_H_
#define DQLITE_TUPLE_H_

#include <stdbool.h>
#include <stdint.h>
#include <unistd.h>

#include "lib/buffer.h"
#include "lib/serialize.h"

#include "protocol.h"

/* Formats for tuple encoding and decoding. */
enum {
	/* Used for coding a row from the database: length field is implicit,
	 * type codes are 4 bits each. */
	TUPLE__ROW = 1,
	/* Used for coding a short tuple of statement parameters: length field
	 * is 1 byte, type codes are 1 byte each. */
	TUPLE__PARAMS,
	/* Used for coding a longer tuple of statement parameters: length field
	 * is 4 bytes, type codes are 1 byte each. */
	TUPLE__PARAMS32
};

/**
 * Hold a single database value.
 */
struct value
{
	int type;
	union {
		int64_t integer;
		double float_;
		uv_buf_t blob;
		uint64_t null;
		const char *text;
		const char *iso8601; /* INT8601 date string */
		int64_t unixtime;    /* Unix time in seconds since epoch */
		uint64_t boolean;
	};
};

/**
 * Maintain state while decoding a single tuple.
 */
struct tuple_decoder
{
	unsigned long n;       /* Number of values in the tuple */
	struct cursor *cursor; /* Reading cursor */
	int format;            /* Tuple format */
	unsigned long i;       /* Index of next value to decode */
	const uint8_t *header; /* Pointer to tuple header */
};

/**
 * Initialize the state of the decoder, before starting to decode a new
 * tuple.
 *
 * If @n is zero, it means that the tuple is a sequence of statement
 * parameters. In that case the d->n field will be read from the first byte of
 * @cursor.
 */
int tuple_decoder__init(struct tuple_decoder *d,
			unsigned n,
			int format,
			struct cursor *cursor);

/**
 * Return the number of values in the tuple being decoded.
 *
 * In row format this will be the same @n passed to the constructor. In
 * parameters format this is the value contained in the first byte of the tuple
 * header.
 */
unsigned long tuple_decoder__n(struct tuple_decoder *d);

/**
 * Decode the next value of the tuple.
 */
int tuple_decoder__next(struct tuple_decoder *d, struct value *value);

/**
 * Maintain state while encoding a single tuple.
 */
struct tuple_encoder
{
	unsigned long n;       /* Number of values in the tuple */
	int format;            /* Tuple format */
	struct buffer *buffer; /* Write buffer */
	unsigned long i;       /* Index of next value to encode */
	size_t header;         /* Buffer offset of tuple header */
};

/**
 * Initialize the state of the encoder, before starting to encode a new
 * tuple. The @n parameter must always be greater than zero.
 */
int tuple_encoder__init(struct tuple_encoder *e,
			unsigned long n,
			int format,
			struct buffer *buffer);

/**
 * Encode the next value of the tuple.
 */
int tuple_encoder__next(struct tuple_encoder *e, struct value *value);

#endif /* DQLITE_TUPLE_H_ */
