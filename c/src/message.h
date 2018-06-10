#ifndef DQLITE_MESSAGE_H
#define DQLITE_MESSAGE_H

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <endian.h>

#include "dqlite.h"
#include "error.h"
#include "lifecycle.h"

/* The size of the message header, always 8 bytes. */
#define DQLITE__MESSAGE_HEADER_LEN 8

/* Length of the statically allocated message body buffer of dqlite__message. If
 * a message body exeeds this size, a dynamically allocated buffer will be
 * used. */
#define DQLITE__MESSAGE_BUF_LEN 4096

/* The size in bytes of a single word in the message body.
 *
 * Since the 'words' field of dqlite__message is 32-bit, the maximum size of a
 * message body is about 34G.
 */
#define DQLITE__MESSAGE_WORD_SIZE 8

/*
 * Common header for requests and responses.
 */
struct dqlite__message {
	/* public */
	uint32_t words; /* Number of 64-bit words in the body (little endian) */
	uint8_t  type;  /* Code identifying the message type */
	uint8_t  flags; /* Type-specific flags */
	uint16_t extra; /* Extra space for type-specific data */

	/* read-only */
	dqlite__error error;

	/* private */
	uint8_t  buf1[DQLITE__MESSAGE_BUF_LEN]; /* Pre-allocated body buffer, enough for most cases */
	uint8_t *buf2;                          /* Dynamically allocated buffer for large bodies */
	size_t   offset;                        /* Number of bytes that have been read or written so far */
	size_t   cap;                           /* Number of bytes available in buf2 */
};

void dqlite__message_init(struct dqlite__message *m);
void dqlite__message_close(struct dqlite__message *m);

/* For requests, called when a starting to receive a new message.
 *
 * It returns the message header buffer and its length. The buffer must be
 * progressivelly filled with the received data as it gets read from the socket,
 * until it's full.
 *
 * For responses, this is called when starting to write the message header,
 * after the response has been rendered. The buffer must be written to the
 * socket before the body one.
 */
void dqlite__message_header_buf(
	struct dqlite__message *m,
	uint8_t **buf,
	size_t *len);

/* Called when the buffer returned by dqlite__message_header_buf has been
 * completely filled by reads and the header is complete.
 *
 * Return an error if the header data is invalid or if there is not enough
 * memory to hold the message body.
 */
int dqlite__message_header_received(struct dqlite__message *m);

/* For requests, this is called when starting to receive a message body, after
 * the message header has been fully received.
 *
 * It returns the message body buffer and its length. The buffer must be
 * progressivelly filled with the received data as it gets read from the socket,
 * until it's full.
 *
 * For responses, this is called when starting to write the message body, after
 * the response has been rendered. The buffer must be written to the socket
 * after the header one.
 */
void dqlite__message_body_buf(
	struct dqlite__message *m,
	uint8_t **buf,
	size_t *len);

/* Called when the last buffer returned by dqlite__message_body_buf has been
 * completely filled by reads and the body is complete.
 *
 * Return an error if the body data is invalid.
 */
int dqlite__message_body_received(struct dqlite__message *m);

/* Return the length of the message body, in bytes */
size_t dqlite__message_body_len(struct dqlite__message *m);

/*
 * APIs for writing and reading the message body.
 */
int dqlite__message_alloc(struct dqlite__message *m, uint32_t words);

int dqlite__message_write_text(struct dqlite__message *m, const char *text);
int dqlite__message_read_text(struct dqlite__message *m, const char **text);

int dqlite__message_write_int64(struct dqlite__message *m, int64_t value);
int dqlite__message_read_int64(struct dqlite__message *m, int64_t *value);

int dqlite__message_write_uint64(struct dqlite__message *m, uint64_t value);
int dqlite__message_read_uint64(struct dqlite__message *m, uint64_t *value);

void dqlite__message_flush(struct dqlite__message *m, uint8_t type, uint8_t flags);
void dqlite__message_processed(struct dqlite__message *m);

/*
 * Utilities for handling byte order.
 */

DQLITE_INLINE uint16_t dqlite__message_flip16(uint16_t v) {
#if defined(__BYTE_ORDER) && (__BYTE_ORDER == __LITTLE_ENDIAN)
	return v;
#elif defined(__BYTE_ORDER) && (__BYTE_ORDER == __BIG_ENDIAN) && \
      defined(__GNUC__) && __GNUC__ >= 4 && __GNUC_MINOR__ >= 8
	return __builtin_bswap16(v);
#else
	union { uint16_t u; uint8_t v[2]; } s;
	s.v[0] = (uint8_t)v;
	s.v[1] = (uint8_t)(v>>8);
	return s.u;
#endif
}

DQLITE_INLINE uint32_t dqlite__message_flip32(uint32_t v) {
#if defined(__BYTE_ORDER) && (__BYTE_ORDER == __LITTLE_ENDIAN)
	return v;
#elif defined(__BYTE_ORDER) && (__BYTE_ORDER == __BIG_ENDIAN) && \
      defined(__GNUC__) && __GNUC__ >= 4 && __GNUC_MINOR__ >= 8
	return __builtin_bswap32(v);
#else
	union { uint32_t u; uint8_t v[4]; } s;
	s.v[0] = (uint8_t)v;
	s.v[1] = (uint8_t)(v>>8);
	s.v[2] = (uint8_t)(v>>16);
	s.v[3] = (uint8_t)(v>>24);
	return s.u;
#endif
}

DQLITE_INLINE uint64_t dqlite__message_flip64(uint64_t v) {
#if defined(__BYTE_ORDER) && (__BYTE_ORDER == __LITTLE_ENDIAN)
	return v;
#elif defined(__BYTE_ORDER) && (__BYTE_ORDER == __BIG_ENDIAN) && \
      defined(__GNUC__) && __GNUC__ >= 4 && __GNUC_MINOR__ >= 8
	return __builtin_bswap64(v);
#else
	union { uint64_t u; uint8_t v[8]; } s;
	s.v[0] = (uint8_t)v;
	s.v[1] = (uint8_t)(v>>8);
	s.v[2] = (uint8_t)(v>>16);
	s.v[3] = (uint8_t)(v>>24);
	s.v[4] = (uint8_t)(v>>32);
	s.v[5] = (uint8_t)(v>>40);
	s.v[6] = (uint8_t)(v>>48);
	s.v[7] = (uint8_t)(v>>56);
	return s.u;
#endif
}

#endif /* DQLITE_MESSAGE_H */
