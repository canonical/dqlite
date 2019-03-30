/**
 * Encode and decode tuples of database values.
 *
 * A tuple is composed by a header and a body.
 *
 * The format of the header changes depending on whether the tuple is a sequence
 * of parameters to bind to a statement, or a sequence of values of a single row
 * yielded by a query.
 *
 * For a tutple of parameters the format of the header is:
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

#include "./lib/serialize.h"

/**
 * Hold a single database value.
 */
struct value
{
	int type;
	union {
		int64_t integer;
		double float_;
		uint64_t null;
		const char *text;
		const char *iso8601; /* Date */
		uint64_t boolean;
	};
};

/**
 * Maintain state while decoding a single tuple.
 */
struct tuple_decoder
{
	bool is_row;	   /* Whether the tuple has row format */
	unsigned n;	    /* Number of values in the tuple */
	unsigned i;	    /* Index of next value to decode */
	const uint8_t *header; /* Pointer to tuple header */
	struct cursor *cursor; /* Reading cursor */
};

/**
 * Initialize the state of the decoder, before starting to decode a new
 * tuple. If @n is zero, it means that the tuple is a sequence of statement
 * parameters. In that case the d->n field will be read from the first byte of
 * @cursor.
 */
int tuple_decoder__init(struct tuple_decoder *d,
			unsigned n,
			struct cursor *cursor);

/**
 * Decode the next value of the tuple.
 */
int tuple_decoder__next(struct tuple_decoder *d, struct value *value);

#endif /* DQLITE_TUPLE_H_ */
